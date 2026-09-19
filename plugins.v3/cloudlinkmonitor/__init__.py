import asyncio
import datetime
import hashlib
import inspect
import json
import os
import re
import shutil
import socket
import threading
import time
import traceback
import unicodedata
from collections import Counter
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from app import schemas
from app.chain.media import MediaChain
from app.chain.scraping import ScrapingChain
from app.chain.storage import StorageChain
from app.chain.tmdb import TmdbChain
from app.chain.transfer import TransferChain
from app.core.config import settings
from app.core.context import MediaInfo
from app.core.event import eventmanager, Event
from app.core.metainfo import MetaInfo, MetaInfoPath
from app.db.downloadhistory_oper import DownloadHistoryOper
from app.db.transferhistory_oper import TransferHistoryOper
from app.helper.directory import DirectoryHelper
from app.log import logger
from app.modules.filemanager import FileManagerModule
from app.plugins import _PluginBase
from app.schemas import NotificationType, TransferInfo, TransferDirectoryConf
from app.schemas.types import EventType, MediaSource, MediaType, SystemConfigKey
from app.utils.string import StringUtils
from app.utils.system import SystemUtils

lock = threading.Lock()

# 特典/附加子目录名(任意一级子目录命中即整体跳过;不作用于种子根目录)。前后分隔符用零宽断言,finditer 能拿到每一处命中
SP_DIR_RE = re.compile(
    r"(?<![^\s_\-.\[\(（【])(?:bonus(?:es)?|extras?|specials?|sps?|nc(?:op|ed)?|pv|cm|menus?|previews?|trailers?|"
    r"scans?|cds?|bk|booklet|gallery|fonts?|samples?|others?)(?![^\s_\-.\]\)）】])"
    r"|特典|番外|映像|特報|预告|扫图|画集|原声|音乐"
    # 花絮/彩蛋紧跟在"含/附/带/加/+/及/和/与/、"后面(某剧.S01.全40集.含花絮.2160p、第1季(含花絮)、全12期+花絮)
    # 说的是正片目录附带了花絮,不是花絮目录
    r"|(?<![含附带加+＋&及和与、絮蛋])(?<![含附带加+＋&及和与、]\s)(?:花絮|彩蛋)", re.IGNORECASE)
# 上面的目录词里也会出现在作品名里的(Trailer Park Boys、Extras、Bonus Family、The Others、Night Gallery、The Menu、
# 音乐之声、映像研):只有这类词、都在子目录名的片名区里、片名区里还有别的词或后接年份/季号时是"软标记",
# 定下作品后按其标题核对;裸标签目录(Extras、Bonus Disc、Sample、Extras Vol 1)与其余目录词一律是特典目录
SOFT_DIR_WORDS = {"bonus", "bonuses", "extra", "extras", "special", "specials", "other", "others", "menu", "menus",
                  "preview", "previews", "trailer", "trailers", "sample", "samples", "gallery", "音乐", "映像"}
# 容易撞上作品名的附加目录名(Interview with the Vampire、幕后之王、The Interview):只认整个目录名就是它
# (Plex/Emby 附加内容目录约定:Featurettes、Behind The Scenes、Deleted Scenes、Interviews);可用 +/&/、/和 连写(特辑+花絮)
_SP_DIR_WORDS = (r"featurettes?|interviews?|behind[\s_.-]?the[\s_.-]?scenes|deleted[\s_.-]?scenes|"
                 r"幕后(?:花絮|特辑)?|特辑|(?:附加|追加)?(?:花絮|彩蛋)|"
                 r"ng|pv|sps?|mv|cm|nc(?:op|ed)|extras?|bonus(?:es)?|specials?|making(?:[\s_.-]?of)?|"
                 r"supplements?|(?:special|bonus)[\s_.-]?features?|extended[\s_.-]?scenes|alternat(?:e|ive)[\s_.-]?endings?|"
                 r"press[\s_.-]?kits?|epk|片花|(?:主创|导演|演员)?(?:访谈|采访|专访)|"
                 r"发布会|路演|首映礼?|物料|预告片?|番外|特典")
SP_DIR_NAME_RE = re.compile(
    r"[\[(（【]?\s*(?:" + _SP_DIR_WORDS + r")(?:\s*[+＋&、和与及]\s*(?:" + _SP_DIR_WORDS + r"))*"
    r"\s*\d*\s*[\])）】]?", re.IGNORECASE)
# 文件名特典标记(SP/OVA/OAD/番外 这类有内容的特别篇不在此列,由批次清单按 TMDB 第 0 季映射)。
# 前后分隔符用零宽断言(不吞字符),finditer 才能拿到每一处命中(相邻两个标记共用一个分隔符);
# PREVIEW 后只收 1-3 位编号(Preview 01 仍是特典),不吞空格和年份(Preview (1989) 的命中只是 Preview)
SP_FILE_RE = re.compile(
    r"(?<![^\s_\-.\[\(（【])(?:NC(?:OP|ED)\d*|MENU\d*|PV\d*|CM\d*|(?:WEB\s*|THEATER\s*)?PREVIEW(?:\s*\d{1,3}(?!\d)|\d+)?|"
    r"TEASER\d*|TRAILER\d*|COMMENTARY|DOCUMENTARY\d*|SAMPLE|INTERVIEW|MAKING|DIGEST|LOGO|TV\s*SPOT|CREDITLESS|PROMO|"
    r"映像特典|特典映像|予告編?|次回予告|预告片?|預告片?|宣传|特報|特报)(?![^\s_\-.\]\)）】\(（#])", re.IGNORECASE)
# 也是普通英文单词、会出现在片名里的特典词(The Menu、The Interview、Interview with the Vampire、Trailer Park Boys、
# Making Waves、Sample People):只有这类词、且都在文件名开头的片名区里时是"软标记",要按作品在 TMDB 上的标题核对;
# NCOP/PV/CM/Menu01/TV Spot/Creditless/予告 等特典专用词(或带编号)一律是特典
SOFT_MARKER_WORDS = {"menu", "preview", "teaser", "trailer", "commentary", "documentary", "sample", "interview",
                     "making", "digest", "logo", "promo"}
# 特典标签(比 SP_FILE_RE 词更全、分隔更宽),只用来核对软标记文件:片名解释不了的这类词一处都不能有
# (The.Menu.2022.TV.Spot、The.Menu.2022.Featurette、The Menu (2022)-deleted 不能因为 Menu 属于片名就被当成正片)
EXTRA_LABEL_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:trailers?|teasers?|previews?|samples?|promos?|tv[\s_.\-]*spots?|clips?|featurettes?|"
    r"behind[\s_.\-]*the[\s_.\-]*scenes|deleted[\s_.\-]*scenes?|bloopers?|gag[\s_.\-]*reels?|outtakes?|"
    r"making[\s_.\-]*of|interviews?|commentar(?:y|ies)|documentar(?:y|ies)|extras?|bonus(?:es)?|"
    r"(?:special|bonus)[\s_.\-]*features?|supplements?|extended[\s_.\-]*scenes?|alternat(?:e|ive)[\s_.\-]*endings?|"
    r"sneak[\s_.\-]*peeks?|press[\s_.\-]*kits?|epk|"
    r"nc(?:op|ed)|creditless)(?![A-Za-z0-9])"
    # Plex/Emby/Jellyfin 本地附加内容的文件名后缀(Movie (2022)-deleted.mkv)
    r"|(?<=-)(?:deleted|scenes?|shorts?|others?|behindthescenes)$"
    r"|幕后(?:花絮|特辑|制作)?|花絮|彩蛋|特辑|特典|映像|预告片?|預告片?|予告|宣传片?|特[报報]"
    r"|片花|(?:主创|导演|演员)?(?:访谈|采访|专访)|发布会|路演|首映礼|删减(?:片段|镜头|场景)|NG镜头|特别收录", re.IGNORECASE)
# 电影批次目录名里才算特典标签的词(剧集的 Specials 是第 0 季,不是特典)
MOVIE_LABEL_RE = re.compile(r"(?<![A-Za-z0-9])specials(?![A-Za-z0-9])", re.IGNORECASE)
# 片名区到此为止的词元:年份、季集号、集数(纯数字)、分辨率/来源/编码等技术信息
ZONE_END_RE = re.compile(r"\d{1,4}|s\d{1,2}(?:e\d{1,4})?|e\d{1,4}|ep\d{1,4}|\d{3,4}[pi]|[248]k|uhd|hdr(?:10)?|"
                         r"bluray|blu|bdrip|brrip|bdremux|remux|webrip|webdl|web|hdtv|dvdrip|dvd|hdrip|"
                         r"x26[45]|h26[45]|hevc|avc|10bit|8bit|ma10p|hi10p", re.IGNORECASE)
YEAR_TOKEN_RE = re.compile(r"(?:19|20)\d{2}")
# 名字开头的发布标签:第一个括号组([Grp]、[4K]、【高清影视之家发布 www.BBQDDQ.com】),其后的技术/站点括号组([1080P]),
# 以及不带括号的站点前缀(阳光电影www.ygdy8.com.)
_CJK_CHARS = chr(0x4E00) + "-" + chr(0x9FFF)
LEAD_GROUP_RE = re.compile(r"\s*[\[【(（][^\]】)）]*[\]】)）]")
LEAD_TECH_RE = re.compile(r"\d{3,4}[pi]|(?<![0-9a-z])[248]k(?![0-9a-z])|uhd|hdr|web-?dl|blu-?ray|www\.|"
                          r"\.(?:com|net|org|cc|co|tv|me|la|cn)(?![0-9a-z])", re.IGNORECASE)
SITE_PREFIX_RE = re.compile(r"\s*[" + _CJK_CHARS + r"]{0,8}\s*www\.[0-9a-z\-]+(?:\.[0-9a-z\-]+)*"
                            r"\.(?:com|net|org|cc|cn|tv|me|la|xyz|top|co|vip)(?![0-9a-z])[\s._\-]*", re.IGNORECASE)
# 中文名与英文名之间的发布标签(菜单[简繁英字幕].The.Menu、菜单.国英双语.The.Menu、夜访吸血鬼 第一季[全7集][中文字幕].Interview…):
# 片名区可以跨过它们接上下一段标题。只收字幕/音轨/季集数/版本/年份/技术信息,不收任何特典词(花絮、访谈、预告…)。
# 按单个词逐个匹配(简繁英字幕 = 简+繁+英+字幕),每个位置只有一种拆法,不会回溯爆炸
_NUM_CN = r"[0-9一二三四五六七八九十百零〇两]+"
_GAP_WORD = (r"第\s*" + _NUM_CN + r"(?:\s*[-~至]\s*" + _NUM_CN + r")?\s*[季部集话話期]"
             r"|全\s*" + _NUM_CN + r"\s*[季部集话話期]|" + _NUM_CN + r"\s*[集话話]全"
             r"|[简繁中英日韩国粤台港双多]|字幕|双字|中字|英字|双语|音轨|配音|国配|中文|英文|国语|粤语|英语|日语|韩语|简体|繁体"
             r"|特效(?=[简繁中英双]|字幕|中字)|内[封嵌]|外挂"
             r"|加长|未删减|导演剪辑|修复|院线|国际|完整|收藏|典藏|重制|高码率?|杜比视界|杜比|蓝光|原盘|高清|超清|标清|版本?|\d{2,3}帧率?"
             r"|(?:19|20)\d{2}|\d{3,4}[pi]|[248]k|uhd|hdr(?:10\+?)?|dv|sdr|imax|3d|blu-?ray|bd(?:rip)?|web-?dl|webrip"
             r"|hdtv|remux|x26[45]|h\.?26[45]|hevc|avc|10bit|\d{2,3}fps")
TITLE_GAP_RE = re.compile(r"(?:[\s._\-+&/|、,，·\[\]【】()（）]|" + _GAP_WORD + r")*", re.IGNORECASE)
# 电影文件名片名(和紧跟的年份)之后、技术信息之前只该有发布用词(版本/剪辑/语言/配音/音视频规格);出现别的词
# (The Interview (2014) - Line-O-Rama、The.Menu.2022.Recipe.for.Disaster.1080p)是以片名开头命名的特典
RESIDUE_RE = re.compile(
    r"(?:[\s._\-+&/|、,，·()（）'’]|" + _GAP_WORD +
    r"|extended|uncut|unrated|uncensored|censored|remaster(?:ed)?|restored|proper|repack|rerip|real|internal|limited"
    r"|hybrid|dc|se|directors?|cut|final|theatrical|imax|criterion|collection|collectors?|edition|anniversary|special"
    r"|ultimate|definitive|version|open|matte|\d{1,3}(?:st|nd|rd|th)|dual|multi|audio|dubbed|dub|subbed|subs?|org"
    r"|chinese|english|mandarin|cantonese|japanese|korean|french|german|spanish|italian|russian|hindi|tamil|telugu"
    r"|eng|chs|cht|chi|jpn|kor|fre|ger|spa|ita|rus|complete|full|dovi|hlg|atmos|truehd|dts|ma|ddp?\d?|eac3|ac3"
    r"|aac\d?|flac|lpcm|opus|\d+ch|amzn|nf|dsnp|hmax|atvp|itunes|web|bd\d{3,4}[pi]|hd\d{3,4}[pi]?|xvid|divx|8bit"
    r"|dl|hd|sd|fhd|hq|h?sbs|h?ou|ws|fs|ntsc|pal|readnfo|nfofix|dirfix|syncfix|subfix|retail|vostfr|vf[fqi2]?|truefrench"
    r"|subfrench|castellano|latino|esp|nordic|swedish|danish|dutch|polish|portuguese|thai|turkish|arabic|hebrew|czech"
    r"|hungarian|greek|finnish|norwegian|ukrainian|persian|vietnamese|indonesian)*",
    re.IGNORECASE)
# 子目录名里除特典词外只有这些词(光盘/容器/连接词/编号)时仍是特典目录名(Bonus Disc、Extras Vol 1、Extras and Trailers)
DIR_VOCAB = {"disc", "disk", "dvd", "bd", "cd", "content", "contents", "material", "materials", "stuff", "misc", "art",
             "artwork", "images", "pictures", "photos", "clips", "videos", "files", "footage", "official", "theatrical",
             "tv", "spots", "movie", "film", "more", "vol", "volume", "part", "and", "or", "extra", "extras", "bonus",
             "bonuses", "special", "specials", "trailer", "trailers", "teaser", "teasers", "sample", "samples",
             "preview", "previews", "menu", "menus", "other", "others", "gallery", "featurette", "featurettes",
             "sneak", "peek", "peeks", "deleted", "scene", "scenes", "making", "interview", "interviews", "promo",
             "promos", "promotional", "commercial", "commercials", "ads", "posters", "stills", "scans", "booklet",
             "bdmv", "iso", "ncop", "nced", "pv", "cm", "sp", "sps", "creditless", "opening", "ending",
             "和", "与", "及"}
# 片名区后面紧跟的集号/季号(剧集的 "[Grp] Trailer Park Boys - 01"、"Trailer.Park.Boys.S01"、"某剧 第01集")
EPISODE_AFTER_RE = re.compile(r"^[\s._\-\[\]()（）【】]*(?:(?:19|20)\d{2}[\s._\-\[\]()（）【】]*)?"
                              r"(?:s\d{1,2}(?:e\d{1,4})?|e\d{1,4}|ep\.?\s*\d{1,4}|第\s*" + _NUM_CN + r"\s*[集话話季]"
                              r"|\d{1,3}(?:v\d)?)(?![0-9a-z])", re.IGNORECASE)
# 批次目录名里特典标签前后的"附带"语境(+ Extras、& Featurettes、with Bonus、含花絮、附赠花絮、附幕后花絮、Trailer Included):
# 说的是正片之外附带了特典,不是整批只有特典。"附带"语境后面(或 Included 前面)连着列出的标签(含花絮特辑、
# + Extras (Trailers, Featurettes)、[Trailers & Extras Included])整串都算附带
INCLUDED_BEFORE_RE = re.compile(r"(?:[+＋&、]|(?<![A-Za-z])(?:and|with|w/|incl\.?|including|includes|plus)"
                                r"|[含附带加及和与赠送])[\s._\-\[(（【]*$", re.IGNORECASE)
INCLUDED_AFTER_RE = re.compile(r"^[\s._\-\])）】]*(?:included|incl\b|inside)", re.IGNORECASE)
LABEL_RUN_GAP_RE = re.compile(r"[\s._\-,，/、;；+＋&()（）\[\]【】]*(?:(?<![A-Za-z])(?:and|or)(?![A-Za-z])|[及和与或])?"
                              r"[\s._\-,，/、;；+＋&()（）\[\]【】]*", re.IGNORECASE)
# 片名区里不算"具体片名词"的虚词(The Interview、Making Of 这类名字只由特典词和虚词组成,不能自证是片名)
TITLE_STOPWORDS = {"the", "a", "an", "of", "and", "with", "in", "on", "at", "to", "for", "from", "by", "or",
                   "le", "la", "les", "el", "los", "las", "der", "die", "das", "il", "lo", "o", "os",
                   "de", "du", "des", "del", "da", "di"}
# 片名比对用的词元:拉丁字母/数字串(撇号并入词内,比较时去掉)或连续的中日韩文字
TITLE_TOKEN_RE = re.compile(r"[0-9A-Za-z\u00C0-\u024F'\u2019]+|[\u3040-\u30FF\u3400-\u4DBF\u4E00-\u9FFF\uAC00-\uD7AF]+")
# 批次清单的特典判定规则版本:此前的全标记批次清单没识别作品就整批按特典(会把 The Menu 这类电影当特典),遇到即按新规则重建。
# 特典词/特典目录的判定规则每改一次就加 1
MARKER_REV = 4
# 文件名里明确是特典的词:LLM 判为特典/杂项时采信(即使文件名带集号,如 "S01E02 Featurette"、"第5集 花絮")
EXTRA_HINT_RE = re.compile(r"花絮|彩蛋|预告片|預告片|Featurettes?|Deleted[\s_.-]?Scenes?|Behind[\s_.-]?the[\s_.-]?Scenes"
                           r"|(?<![A-Za-z])NC(?:OP|ED)\d*(?![A-Za-z])|Creditless", re.IGNORECASE)
# 自定义识别词里声明的媒体类型(type=tv/movie/movies)、季号(s=N)与作品 id(词里写的 tmdbid=/doubanid=… 或插入的 [tmdbid=…])
WORD_TYPE_RE = re.compile(r"[{\[;]\s*type\s*=\s*(tv|movies?|电视剧|电影)\b", re.IGNORECASE)
WORD_SEASON_RE = re.compile(r"[{\[;]\s*s\s*=\s*\d")
WORD_ID_RE = re.compile(r"(?:tmdb|douban|bangumi|anilist)(?:id)?\s*[=\-]\s*\d|media_id\s*=", re.IGNORECASE)
# 文件/目录名里自带的 Emby/Plex 式 id 标签([tmdbid=…]、{tmdb-…}、[imdbid-tt…]):不是用户声明、不带类型,按标题识别前剥掉
ID_TAG_RE = re.compile(r"[\[{(]\s*(?:tmdb|tvdb|imdb|douban|bangumi|anilist)(?:id)?\s*[=\-][^\]})]*[\]})]",
                       re.IGNORECASE)


def _lead_end(norm: str) -> int:
    """名字开头发布标签([Grp]、[4K]、【…www.BBQDDQ.com】、阳光电影www.ygdy8.com.)结束的位置;没有时 0"""
    m = LEAD_GROUP_RE.match(norm)
    pos = m.end() if m else 0
    while True:
        m = LEAD_GROUP_RE.match(norm, pos)
        if m and LEAD_TECH_RE.search(m.group(0)):
            pos = m.end()
            continue
        m = SITE_PREFIX_RE.match(norm, pos)
        if m and m.end() > pos:
            pos = m.end()
            continue
        return pos


def _name_zone(norm: str) -> Tuple[int, int, bool, List[Tuple[str, int, int]]]:
    """
    名字的片名区:(开头标签结束处, 片名区结束处, 是否遇到了年份/季集号/集数/技术信息, 词元)。片名区从开头标签之后算起,
    到第一个年份/季集号/集数/技术信息词元为止;中文名.年份.英文名(菜单.2022.The.Menu.1080p)年份后面那段英文名也在片名区里
    """
    tokens = _title_tokens(norm)
    lead = _lead_end(norm)
    body = [t for t in tokens if t[1] >= lead]
    for i, (word, start, _) in enumerate(body):
        if not ZONE_END_RE.fullmatch(word):
            continue
        head = body[:i]
        if head and all(not re.match(r"[0-9a-z]", t[0]) for t in head) and YEAR_TOKEN_RE.fullmatch(word):
            nxt = next((t for t in body[i + 1:] if ZONE_END_RE.fullmatch(t[0])), None)
            return lead, (nxt[1] if nxt else len(norm)), True, tokens
        return lead, start, True, tokens
    return lead, len(norm), False, tokens


def _title_tokens(text) -> List[Tuple[str, int, int]]:
    """片名比对用的词元 (词, 起, 止):小写,去掉重音与撇号(Schindler's = Schindlers、menú = menu)"""
    tokens = []
    for m in TITLE_TOKEN_RE.finditer(text or ""):
        word = "".join(ch for ch in unicodedata.normalize("NFKD", m.group(0)) if not unicodedata.combining(ch))
        word = word.replace("'", "").replace("\u2019", "").lower()
        if word:
            tokens.append((word, m.start(), m.end()))
    return tokens


# 8 位十六进制 CRC(括号内)与「」『』集标题:MP 解析器会把 (08377E86) 当成 E86、把「レム」当成剧名
CRC_RE = re.compile(r"[\(\[（【][0-9A-Fa-f]{8}[\)\]）】]")
QUOTE_RE = re.compile(r"「[^」]*」|『[^』]*』")
# "4th - 08" / "[4th]" 这类序数词季号(后面不接 Season 的)归一化为 S4;"4th Season" MP 本身认识,不动
ORDINAL_SEASON_RE = re.compile(r"(?<![A-Za-z0-9])(\d{1,2})(?:st|nd|rd|th)(?=\s*[-\]\)）】])", re.IGNORECASE)
# 文件名里显式写了季号的依据;没有这些依据而解析器仍给出季号(典型:把"- 08"当 S01E08)时季号视为未知
EXPLICIT_SEASON_RE = re.compile(
    r"(?<![A-Za-z0-9])S\d{1,2}(?:E\d{1,4})?(?![A-Za-z0-9])|Season\s*\d{1,2}|\d{1,2}(?:st|nd|rd|th)\s*Season"
    r"|第\s*[0-9一二三四五六七八九十]+\s*[季期部]|(?<![A-Za-z0-9])(?:II|III|IV)(?![A-Za-z0-9])", re.IGNORECASE)
EP_RANGE_RE = re.compile(r"E(\d{1,4})(?:\s*-\s*E?(\d{1,4}))?", re.IGNORECASE)
# 全角括号/方括号归一化(「凡人修仙传（2020）」的年份要能解析出来)
FULLWIDTH_TABLE = str.maketrans({"（": "(", "）": ")", "【": "[", "】": "]", "［": "[", "］": "]", "｛": "{", "｝": "}"})
# 剧名末尾的部数/序号:闯关东Ⅰ、闯关东1、闯关东II、庆余年第二部、Show Part 2(TMDB 标题通常不带)。
# 空格后的英文罗马数字不去:MetaInfo 会把 "Rocky II" 规范成 "Rocky Ii",剩下能匹配的只有 Malcolm X 这类本名
SEQ_SUFFIX_RE = re.compile(
    r"(?:\s*(?P<roman>[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅪⅫ])"
    r"|(?<=[\u4e00-\u9fff])\s*(?:(?P<cjk_digit>[1-9１-９])|(?P<cjk_roman>IX|IV|VI{0,3}|I{1,3}|X))"
    r"|\s+(?P<digit>[1-9])"
    r"|\s*第\s*(?P<part_cn>[一二三四五六七八九十0-9]+)\s*部"
    r"|\s+(?i:part)\.?\s*(?P<part_no>[0-9]{1,2}))\s*$")
ROMAN_VALUES = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10}
CN_NUMERALS = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}
# 来自本批文件自身的有效证据(可以定案);同名搜索/去序号识别是零票旁听,下载记录/订阅只是提示
STRONG_SRCS = {"文件名", "目录名", "文件名(去序号)", "目录名(去序号)", "下载记录直通", "识别词"}
# V3 旧整理兼容命令(chain.transfer)同步执行失败、任务留给持久化整理队列在后台接着做时的返回信息
QUEUE_HANDOFF_MESSAGES = ("整理失败，请稍后重试", "整理结果确认失败，后台将自动重试")
# 交给整理队列后等它结算的最长时间(队列失败重试间隔 30 秒起,多次重试后才判失败)
HANDOFF_WAIT = 1800
# 只说明"最近下过/订阅了"、与本批文件名无关的来源:这类候选不能当主候选,也不能被换选或 LLM 选中
HINT_SRCS = {"下载记录", "订阅"}

MANIFEST_PROMPT = (
    "你是媒体库整理助手。输入:一个发布目录名、其中全部视频文件的编号列表(含文件大小)、该作品在媒体库使用的"
    "『目标编号体系』下的季/集结构,以及可能相关的备选作品。请为每个文件给出分类与季集映射。\n"
    "类别:\n"
    "- main: 正片(对应结构中某季某集;电影则为影片本体)\n"
    "- special: 有剧情内容的特别篇/短篇/OVA/OAD/番外/总集篇等,若能对应结构中第 0 季(特别篇)的某一集则给出季集\n"
    "- extra: 非正片映像:PV/CM/NCOP/NCED(无字幕 OP/ED)/Menu/予告·次回预告·Preview/Teaser/Trailer/Commentary(评论音轨版)/"
    "Documentary/Interview/Making/花絮/Sample/Logo 等\n"
    "- other: 音乐、扫图、字体、非视频等\n"
    "规则:\n"
    "1) 文件名末尾括号内的 8 位十六进制是 CRC 校验码,不是集数;「」内是集标题,不是剧名。\n"
    "2) s/e 必须使用『目标编号体系』中列出的季号/集号。文件名里的季集可能是另一套编号(按播出季编号、或绝对集数),"
    "结构里每集都并列标注了绝对集数(abs)和/或按季编号,请据此换算;换算不了就省略 s/e(系统会跳过该文件,比放错位置好)。\n"
    "3) 同一 (s,e) 不能分配给多个文件。\n"
    "4) 集号按文件名中的编号/标题与结构中集标题的语义对应来确定,不要只靠顺序猜。\n"
    "5) 体积明显小于正片的文件(几十 MB)通常是预告/特典/短篇,不是正片。\n"
    "6) 若该批次实际属于备选作品列表中的另一部作品,在 series_tmdbid 中给出其 TMDB id,并按该作品自身的编号给出 s/e(单季作品季号为 1;不确定则省略 s/e);否则给出当前作品 id。\n"
    "只输出一个紧凑的 JSON 对象(不要换行缩进、不要解释):"
    "{\"series_tmdbid\": 数字, \"items\": [{\"i\": 文件编号, \"c\": \"main|special|extra|other\", \"s\": 季号, \"e\": 集号}, ...]}"
    " s/e 只在 main/special 且确定时给出。每个文件编号都必须且只能出现一次。"
)


class FileMonitorHandler(FileSystemEventHandler):
    """
    目录监控响应类
    """

    def __init__(self, monpath: str, sync: Any, **kwargs):
        super(FileMonitorHandler, self).__init__(**kwargs)
        self._watch_path = monpath
        self.sync = sync

    def on_created(self, event):
        self.sync.event_handler(event=event, text="创建",
                                mon_path=self._watch_path, event_path=event.src_path)

    def on_moved(self, event):
        self.sync.event_handler(event=event, text="移动",
                                mon_path=self._watch_path, event_path=event.dest_path)


class CloudLinkMonitor(_PluginBase):
    # 插件名称
    plugin_name = "目录实时监控"
    # 插件描述
    plugin_desc = "监控目录文件变化，自动转移媒体与字幕文件，支持云端定时对账与挂载缓存强制刷新。"
    # 插件图标
    plugin_icon = "Linkease_A.png"
    # 插件版本
    plugin_version = "3.6.2"
    # 插件作者
    plugin_author = "thsrite,Anniversor"
    # 作者主页
    author_url = "https://github.com/thsrite"
    # 插件配置项ID前缀
    plugin_config_prefix = "cloudlinkmonitor_"
    # 加载顺序
    plugin_order = 4
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _scheduler = None
    transferhis = None
    downloadhis = None
    transferchian = None
    tmdbchain = None
    storagechain = None
    _observer = []
    _enabled = False
    _notify = False
    _onlyonce = False
    _history = False
    _scrape = False
    _type = False
    _category = False
    _refresh = False
    _softlink = False
    _strm = False
    _cron = None
    filetransfer = None
    mediaChain = None
    _size = 0
    # 模式 compatibility/fast
    _mode = "compatibility"
    # 转移方式
    _transfer_type = "softlink"
    _monitor_dirs = ""
    _exclude_keywords = ""
    _interval: int = 10
    # 对账/缓存刷新配置
    _openlist_url = ""
    _openlist_token = ""
    _openlist_base = ""
    _rc_socket = ""
    _rc_fs = ""
    _rc_mount_prefix = ""
    _junk_clean = False
    _junk_exts = ".url,.html,.htm,.txt"
    # 快速探测间隔(秒)，0为关闭
    _fast_interval = 0
    _llm_classify = True
    _leftover_policy = "quarantine"
    _leftover_dir = ""
    _batch_settle = 120
    _emby_reprobe = True
    _emby_sync_group = True
    _group_map = ""
    _batch_list_cache = None
    # 作品编号体系(剧集组)持久化映射与进程内状态
    _series_groups = None
    # 预览 API 的测试开关/只读模式只作用于发起预览的线程,不影响同时在跑的对账/复查
    _override_tls = threading.local()
    _group_notified = None
    _emby_sync_pending = None
    _recheck_pending = None
    _recheck_timers = None
    # 交给 MP 整理队列在后台接手的整理:{源路径: 上下文};结算后补做刮削/通知/strm
    _handoffs = None
    _handoff_timer = None
    _words_sig = None
    # 通知型目录(无目的目录)的云端探测与已通知集合
    _notify_probe_interval = 180
    _notify_fp = None
    _notify_last = 0
    _notified = None
    _notified_dirty = False
    # 批次清单(持久化)与进程内缓存
    _manifests = None
    _batch_seen = None
    _recog_cache = None
    # 逐文件特典判定里认不出作品的文件名备忘(一小时内不重复识别;识别词变更/手动运行时清空)
    _verdict_memo = None
    # LLM 季集映射答案(输入完全相同就沿用,挂起批次复查时不重复调用)
    _llm_cache = None
    _tmdb_struct_cache = None
    _emby_cache = None
    _manifest_lock = None
    # 内置垃圾/广告文件名模式（与排除关键词合并生效，防止配置被误清后失去防线）
    _builtin_junk_patterns = ["【更多", "更多.*(下载|访问)", "TVBOXNOW", r"\.url$"]
    # 失败重试计数(进程内)
    _retry_counts = None
    # 快速探测指纹(进程内)
    _fast_fp = None
    # 对账互斥锁
    _busy = None
    # 存储源目录与目的目录关系
    _dirconf: Dict[str, Optional[Path]] = {}
    # 存储源目录转移方式
    _transferconf: Dict[str, Optional[str]] = {}
    _overwrite_mode: Dict[str, Optional[str]] = {}
    _medias = {}
    # 退出事件
    _event = threading.Event()

    @property
    def _order_override(self) -> Optional[dict]:
        return getattr(self._override_tls, "value", None)

    @_order_override.setter
    def _order_override(self, value: Optional[dict]):
        self._override_tls.value = value

    def init_plugin(self, config: dict = None):
        self.transferhis = TransferHistoryOper()
        self.downloadhis = DownloadHistoryOper()
        self.transferchian = TransferChain()
        self.tmdbchain = TmdbChain()
        self.mediaChain = MediaChain()
        self.storagechain = StorageChain()
        self.scrapingchain = ScrapingChain()
        self.filetransfer = FileManagerModule()
        # 清空配置
        self._dirconf = {}
        self._transferconf = {}
        self._overwrite_mode = {}

        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._notify = config.get("notify")
            self._onlyonce = config.get("onlyonce")
            self._history = config.get("history")
            self._scrape = config.get("scrape")
            self._type = config.get("type")
            self._category = config.get("category")
            self._refresh = config.get("refresh")
            self._mode = config.get("mode")
            self._transfer_type = config.get("transfer_type")
            self._monitor_dirs = config.get("monitor_dirs") or ""
            self._exclude_keywords = config.get("exclude_keywords") or ""
            self._interval = config.get("interval") or 10
            self._cron = config.get("cron")
            self._size = config.get("size") or 0
            self._softlink = config.get("softlink")
            self._strm = config.get("strm")
            self._openlist_url = (config.get("openlist_url") or "").rstrip("/")
            self._openlist_token = config.get("openlist_token") or ""
            self._openlist_base = (config.get("openlist_base") or "").rstrip("/")
            self._rc_socket = config.get("rc_socket") or ""
            self._rc_fs = config.get("rc_fs") or ""
            self._rc_mount_prefix = (config.get("rc_mount_prefix") or "").rstrip("/")
            self._junk_clean = config.get("junk_clean") or False
            self._junk_exts = config.get("junk_exts") or ".url,.html,.htm,.txt"
            try:
                self._fast_interval = int(config.get("fast_interval") or 0)
            except (TypeError, ValueError):
                self._fast_interval = 0
            self._llm_classify = config.get("llm_classify", True)
            self._leftover_policy = config.get("leftover_policy") or "quarantine"
            self._leftover_dir = (config.get("leftover_dir") or "").rstrip("/")
            try:
                self._batch_settle = int(config.get("batch_settle")) \
                    if config.get("batch_settle") not in (None, "") else 120
            except (TypeError, ValueError):
                self._batch_settle = 120
            self._emby_reprobe = config.get("emby_reprobe", True)
            self._emby_sync_group = config.get("emby_sync_group", True)
            try:
                self._notify_probe_interval = int(config.get("notify_probe_interval") or 180)
            except (TypeError, ValueError):
                self._notify_probe_interval = 180
            self._group_map = config.get("group_map") or ""

        # 重置失败重试计数与快速探测指纹
        self._retry_counts = {}
        self._fast_fp = None
        # 锁对象跨重载保持不变:上一轮配置排下、正在运行的复查/对账还持有它们
        self._busy = self._busy or threading.Lock()
        self._batch_list_cache = {}
        self._manifests = self.get_data("batch_manifest") or {}
        self._series_groups = self.get_data("series_groups") or {}
        self._order_override = None
        self._group_notified = set()
        self._emby_sync_pending = set()
        self._recheck_pending = set()
        # 不重置:紧接着的 stop_service 要取消上一轮配置排下的复查定时器;取消前先记下它们对应的批次,配置加载后重新挂上
        self._recheck_timers = self._recheck_timers or {}
        rearm = [ctx for ctx in (getattr(t, "clm_ctx", None) for t in list(self._recheck_timers.values())) if ctx]
        self._notify_fp = {}
        self._notify_last = 0
        self._notified = self.get_data("notified_files") or {}
        self._notified_dirty = False
        self._batch_seen = {}
        self._recog_cache = {}
        self._verdict_memo = {}
        self._tmdb_struct_cache = {}
        self._emby_cache = {}
        self._manifest_lock = self._manifest_lock or threading.RLock()

        # 停止现有任务
        self.stop_service()

        if self._enabled or self._onlyonce:
            # 定时服务管理器
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            if self._notify:
                # 追加入库消息统一发送服务
                self._scheduler.add_job(self.send_msg, trigger='interval', seconds=15)

            # 读取目录配置
            monitor_dirs = self._monitor_dirs.split("\n")
            if not monitor_dirs:
                return
            for mon_path in monitor_dirs:
                # 格式源目录:目的目录
                if not mon_path:
                    continue

                # 自定义覆盖方式
                _overwrite_mode = 'never'
                if mon_path.count("@") == 1:
                    _overwrite_mode = mon_path.split("@")[1]
                    mon_path = mon_path.split("@")[0]

                # 自定义转移方式
                _transfer_type = self._transfer_type
                if mon_path.count("#") == 1:
                    _transfer_type = mon_path.split("#")[1]
                    mon_path = mon_path.split("#")[0]

                # 存储目的目录
                if SystemUtils.is_windows():
                    if mon_path.count(":") > 1:
                        paths = [mon_path.split(":")[0] + ":" + mon_path.split(":")[1],
                                 mon_path.split(":")[2] + ":" + mon_path.split(":")[3]]
                    else:
                        paths = [mon_path]
                else:
                    paths = mon_path.split(":")

                # 目的目录
                target_path = None
                if len(paths) > 1:
                    mon_path = paths[0]
                    target_path = Path(paths[1])
                    self._dirconf[mon_path] = target_path
                else:
                    self._dirconf[mon_path] = None
                    logger.info(f"{mon_path} 的目的目录为空，发生变动时直接通知下游")

                # 转移方式
                self._transferconf[mon_path] = _transfer_type
                self._overwrite_mode[mon_path] = _overwrite_mode

                # 启用目录监控
                if self._enabled:
                    # 检查媒体库目录是不是下载目录的子目录
                    try:
                        if target_path and target_path.is_relative_to(Path(mon_path)):
                            logger.warn(f"{target_path} 是监控目录 {mon_path} 的子目录，无法监控")
                            self.systemmessage.put(f"{target_path} 是下载目录 {mon_path} 的子目录，无法监控")
                            continue
                    except Exception as e:
                        logger.debug(str(e))
                        pass

                    try:
                        if self._mode == "compatibility":
                            # 兼容模式，目录同步性能降低且NAS不能休眠，但可以兼容挂载的远程共享目录如SMB
                            observer = PollingObserver(timeout=10)
                        else:
                            # 内部处理系统操作类型选择最优解
                            observer = Observer(timeout=10)
                        self._observer.append(observer)
                        observer.schedule(FileMonitorHandler(mon_path, self), path=mon_path, recursive=True)
                        observer.daemon = True
                        observer.start()
                        logger.info(f"{mon_path} 的云盘实时监控服务启动")
                    except Exception as e:
                        err_msg = str(e)
                        if "inotify" in err_msg and "reached" in err_msg:
                            logger.warn(
                                f"云盘实时监控服务启动出现异常：{err_msg}，请在宿主机上（不是docker容器内）执行以下命令并重启："
                                + """
                                     echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf
                                     echo fs.inotify.max_user_instances=524288 | sudo tee -a /etc/sysctl.conf
                                     sudo sysctl -p
                                     """)
                        else:
                            logger.error(f"{mon_path} 启动目云盘实时监控失败：{err_msg}")
                        self.systemmessage.put(f"{mon_path} 启动云盘实时监控失败：{err_msg}")

            if self._enabled:
                self.__rearm_rechecks(rearm)

            # 运行一次定时服务
            if self._onlyonce:
                logger.info("云盘实时监控服务启动，立即运行一次")
                self._scheduler.add_job(name="云盘实时监控",
                                        func=self.sync_all, trigger='date',
                                        run_date=datetime.datetime.now(
                                            tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3)
                                        )
                # 关闭一次性开关
                self._onlyonce = False
                # 保存配置
                self.__update_config()

            # 启动定时服务
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __update_config(self):
        """
        更新配置
        """
        self.update_config({
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": self._onlyonce,
            "mode": self._mode,
            "transfer_type": self._transfer_type,
            "monitor_dirs": self._monitor_dirs,
            "exclude_keywords": self._exclude_keywords,
            "interval": self._interval,
            "history": self._history,
            "softlink": self._softlink,
            "strm": self._strm,
            "scrape": self._scrape,
            "type": self._type,
            "category": self._category,
            "size": self._size,
            "refresh": self._refresh,
            "cron": self._cron,
            "openlist_url": self._openlist_url,
            "openlist_token": self._openlist_token,
            "openlist_base": self._openlist_base,
            "rc_socket": self._rc_socket,
            "rc_fs": self._rc_fs,
            "rc_mount_prefix": self._rc_mount_prefix,
            "junk_clean": self._junk_clean,
            "junk_exts": self._junk_exts,
            "fast_interval": self._fast_interval,
            "llm_classify": self._llm_classify,
            "leftover_policy": self._leftover_policy,
            "leftover_dir": self._leftover_dir,
            "batch_settle": self._batch_settle,
            "emby_reprobe": self._emby_reprobe,
            "emby_sync_group": self._emby_sync_group,
            "notify_probe_interval": self._notify_probe_interval,
            "group_map": self._group_map,
        })

    @eventmanager.register(EventType.PluginAction)
    def remote_sync(self, event: Event):
        """
        远程全量同步
        """
        if event:
            event_data = event.event_data
            if not event_data or event_data.get("action") != "cloud_link_sync":
                return
            self.post_message(channel=event.event_data.get("channel"),
                              title="开始同步云盘实时监控目录 ...",
                              userid=event.event_data.get("user"))
        self.sync_all()
        if event:
            self.post_message(channel=event.event_data.get("channel"),
                              title="云盘实时监控目录同步完成！", userid=event.event_data.get("user"))

    def sync_all(self):
        """
        立即运行一次，全量同步目录中所有文件
        """
        logger.info("开始全量同步云盘实时监控目录 ...")
        # 手动运行:挂起中的批次(认不出作品/TMDB 瞬时故障)不等退避到期,立即重试一次;逐文件判定也不沿用"认不出"备忘
        self.__release_retry_batches()
        self._verdict_memo = {}
        try:
            # 遍历所有监控目录
            for mon_path in list(self._dirconf.keys()):
                try:
                    logger.info(f"开始处理监控目录 {mon_path} ...")
                    media_files = SystemUtils.list_files(Path(mon_path), settings.RMT_MEDIAEXT)
                    sub_files = SystemUtils.list_files(Path(mon_path), settings.RMT_SUBEXT)
                    logger.info(f"监控目录 {mon_path} 共发现 {len(media_files)} 个媒体文件、{len(sub_files)} 个字幕文件")
                    # 遍历目录下所有文件（先媒体后字幕，保证字幕改名与视频一致时目标目录已就绪）
                    for file_path in media_files + sub_files:
                        logger.info(f"开始处理文件 {file_path} ...")
                        self.__handle_file(event_path=str(file_path), mon_path=mon_path, from_reconcile=True,
                                           force_notify=True)
                    self.__save_notified()
                except Exception as e:
                    logger.error(f"全量同步监控目录 {mon_path} 出错:{str(e)} - {traceback.format_exc()}")
        finally:
            # 放行的挂起批次没被扫到(扫描中途出错)时,重新挂上复查,不能就此没有定时器
            self.__rearm_rechecks([])
        logger.info("全量同步云盘实时监控目录完成！")

    def sync_reconcile(self):
        """
        定时对账：强制刷新云端列表与挂载缓存后，扫描监控目录中未处理的媒体与字幕文件。
        仅处理配置了目的目录的监控目录；失败记录会有限次重试。
        """
        busy = self._busy
        if busy and not busy.acquire(blocking=False):
            logger.info("上一轮对账仍在进行，跳过本轮")
            return
        try:
            logger.info("开始云盘目录定时对账 ...")
            try:
                self.__refresh_sources()
            except Exception as e:
                logger.error(f"对账刷新缓存失败：{str(e)}")
            for mon_path in list(self._dirconf.keys()):
                if self._dirconf.get(mon_path) is None:
                    # 通知型目录:不强刷云端(零 QPS),只按挂载缓存补发漏掉的文件
                    try:
                        self.__notify_scan(mon_path, source="对账")
                    except Exception as e:
                        logger.error(f"对账扫描通知型目录 {mon_path} 失败:{str(e)}")
                    continue
                try:
                    media_files = SystemUtils.list_files(Path(mon_path), settings.RMT_MEDIAEXT)
                    sub_files = SystemUtils.list_files(Path(mon_path), settings.RMT_SUBEXT)
                except Exception as e:
                    logger.error(f"对账扫描 {mon_path} 失败：{str(e)}")
                    continue
                if media_files or sub_files:
                    logger.info(f"对账：{mon_path} 发现 {len(media_files)} 个媒体文件、{len(sub_files)} 个字幕文件")
                # 按批次分组:先建批次清单(一批一次),再按 正片(季集序)->特别篇->字幕 的顺序处理
                for batch_root, group in self.__group_by_batch(mon_path, media_files + sub_files):
                    if batch_root is not None:
                        mf = self.__get_manifest(batch_root)
                        if mf and mf.get("pending"):
                            log = logger.debug if mf.get("cached") else logger.info
                            log(f"批次 {batch_root.name} 暂缓整理({mf.get('reason') or '文件仍在变化'}),"
                                f"本轮跳过 {len(group)} 个文件,{mf.get('retry_in')} 秒后自动复查")
                            self.__schedule_batch_recheck(batch_root, mon_path, int(mf.get("retry_in") or 60))
                            continue
                        group = self.__order_batch(mf, batch_root, group)
                    for file_path in group:
                        self.__handle_file(event_path=str(file_path), mon_path=mon_path, from_reconcile=True)
            if self._junk_clean:
                try:
                    self.__clean_junk_dirs()
                except Exception as e:
                    logger.error(f"清理垃圾目录失败：{str(e)}")
            if self._leftover_policy and self._leftover_policy != "off":
                try:
                    self.__clean_leftover_dirs()
                except Exception as e:
                    logger.error(f"清理整理残留失败：{str(e)}")
            logger.info("云盘目录定时对账完成")
        finally:
            if busy:
                busy.release()

    def fast_probe(self):
        """
        快速探测：低成本强刷监控目录浅层列表（顶层+一级子目录），
        指纹变化时立即触发对账。监控目录平时为空，稳态开销约为每轮1次API调用。
        """
        if not self._enabled:
            return
        try:
            self.__notify_probe()
        except Exception as e:
            logger.error(f"通知型目录探测失败:{str(e)}")
        if self._busy and self._busy.locked():
            return
        fp = self.__collect_fingerprint()
        if fp is None:
            return
        if self._fast_fp is None:
            self._fast_fp = fp
            logger.debug("快速探测：完成基线采集")
            return
        if fp != self._fast_fp:
            logger.info("快速探测：发现云端变化，立即触发对账 ...")
            self._fast_fp = fp
            self.sync_reconcile()
            # 对账消化文件后重新基线，避免下一轮因文件被搬走再次触发
            new_fp = self.__collect_fingerprint()
            if new_fp is not None:
                self._fast_fp = new_fp

    def __collect_fingerprint(self) -> Optional[dict]:
        """
        采集监控目录云端浅层指纹（顶层+至多10个一级子目录），每目录一次强制刷新列表
        """
        if not self._openlist_url or not self._openlist_token:
            return None
        fp = {}
        for mon_path in self._dirconf.keys():
            if self._dirconf.get(mon_path) is None:
                continue
            ol_path = self.__map_openlist_path(mon_path)
            if not ol_path:
                continue
            top = self.__openlist_list(ol_path)
            if top is None:
                # API异常时放弃本轮，避免误判触发
                return None
            fp[ol_path] = tuple(sorted(
                (i.get("name"), i.get("is_dir"), i.get("size"), i.get("modified")) for i in top))
            for name in [i.get("name") for i in top if i.get("is_dir")][:10]:
                sub_path = f"{ol_path.rstrip('/')}/{name}"
                sub = self.__openlist_list(sub_path)
                if sub is not None:
                    fp[sub_path] = tuple(sorted(
                        (i.get("name"), i.get("is_dir"), i.get("size"), i.get("modified")) for i in sub))
        return fp

    def __openlist_list(self, ol_path: str) -> Optional[list]:
        """
        OpenList 强制刷新列出目录（refresh=true 直连网盘），失败返回 None
        """
        try:
            resp = requests.post(f"{self._openlist_url}/api/fs/list",
                                 headers={"Authorization": self._openlist_token,
                                          "Content-Type": "application/json"},
                                 json={"path": ol_path, "refresh": True, "page": 1, "per_page": 0},
                                 timeout=30)
            data = resp.json() if resp is not None else {}
            if data.get("code") != 200:
                logger.debug(f"OpenList 列目录 {ol_path} 失败：{data.get('message')}")
                return None
            return (data.get("data") or {}).get("content") or []
        except Exception as e:
            logger.debug(f"OpenList 列目录 {ol_path} 异常：{str(e)}")
            return None

    def __refresh_target_dir(self, local_dir: str):
        """
        转移落地后对目标目录做 OpenList->rclone 双层强刷,消除挂载读空窗,
        让 strm 助手的就绪门控首查即过。
        风控考量:按目录60秒去重(一季N集共享一个目录只刷1次);
        rclone 刷新读取的是 OpenList 刚刷新的缓存,不再穿透到115。
        """
        try:
            cache = getattr(self, "_target_refreshed", None)
            if cache is None:
                cache = {}
                self._target_refreshed = cache
            now = datetime.datetime.now().timestamp()
            if now - (cache.get(local_dir) or 0) < 60:
                return
            cache[local_dir] = now
            if len(cache) > 200:
                cutoff = now - 300
                self._target_refreshed = {k: v for k, v in cache.items() if v > cutoff}
            # 保序:先 OpenList 强刷(绕过115列表缓存),后 rclone vfs 刷新(拉取新列表)
            ol_path = self.__map_openlist_path(local_dir)
            if ol_path:
                self.__openlist_list(ol_path)
            self.__vfs_refresh(local_dir)
            logger.info(f"目标目录已双层强刷：{local_dir}")
        except Exception as e:
            logger.warn(f"目标目录强刷失败：{local_dir} - {str(e)}")

    def __refresh_sources(self):
        """
        对账前强制刷新：先让 OpenList 直连网盘拉取真实列表，再刷新 rclone VFS 目录缓存，
        保证随后的挂载扫描看到云端真实状态。均为尽力而为，失败不阻塞扫描。
        """
        for mon_path in self._dirconf.keys():
            if self._dirconf.get(mon_path) is None:
                continue
            if self._openlist_url and self._openlist_token:
                ol_path = self.__map_openlist_path(mon_path)
                if ol_path:
                    logger.info(f"对账：强制刷新 OpenList 列表 {ol_path} ...")
                    self.__openlist_refresh_walk(ol_path)
            if self._rc_socket and self._rc_fs:
                logger.info(f"对账：刷新 rclone 目录缓存 {mon_path} ...")
                self.__vfs_refresh(mon_path)

    def __map_openlist_path(self, local_path: str) -> Optional[str]:
        """
        将本地挂载路径映射为 OpenList 路径
        """
        if not self._rc_mount_prefix or not str(local_path).startswith(self._rc_mount_prefix):
            return None
        rel = str(local_path)[len(self._rc_mount_prefix):]
        return (self._openlist_base + rel) or "/"

    def __openlist_refresh_walk(self, ol_path: str, depth: int = 0, budget: list = None):
        """
        递归强制刷新 OpenList 目录列表（refresh=true 直连网盘），budget 限制总目录数防失控
        """
        if budget is None:
            budget = [200]
        if budget[0] <= 0 or depth > 6:
            return
        budget[0] -= 1
        items = self.__openlist_list(ol_path)
        if items is None:
            logger.warn(f"OpenList 刷新 {ol_path} 失败")
            return
        for item in items:
            if item.get("is_dir"):
                self.__openlist_refresh_walk(f"{ol_path.rstrip('/')}/{item.get('name')}",
                                             depth + 1, budget)

    def __rc_call(self, endpoint: str, payload: dict, timeout: int = 300) -> bool:
        """
        通过 unix socket 调用 rclone rc 接口
        """
        if not self._rc_socket:
            return False
        try:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect(self._rc_socket)
            req = (f"POST /{endpoint} HTTP/1.1\r\nHost: local\r\nContent-Type: application/json\r\n"
                   f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode() + body
            s.sendall(req)
            data = b""
            while True:
                chunk = s.recv(65536)
                if not chunk:
                    break
                data += chunk
            s.close()
            ok = data.startswith(b"HTTP/1.1 200")
            if not ok:
                logger.warn(f"rclone rc {endpoint} 返回异常：{data[:200]}")
            return ok
        except Exception as e:
            logger.warn(f"rclone rc {endpoint} 调用失败：{str(e)}")
            return False

    def __vfs_refresh(self, local_path: str, recursive: bool = True) -> bool:
        """
        强制刷新 rclone VFS 目录缓存(默认递归;通知型目录的定向刷新用非递归,避免 rclone 重新拉整棵树)
        """
        if not self._rc_fs or not self._rc_mount_prefix \
                or not str(local_path).startswith(self._rc_mount_prefix):
            return False
        rel = str(local_path)[len(self._rc_mount_prefix):].strip("/")
        if not rel:
            return False
        return self.__rc_call("vfs/refresh", {"fs": self._rc_fs, "dir": rel,
                                              "recursive": "true" if recursive else "false"})

    def __exclude_patterns(self) -> List[str]:
        """
        内置垃圾模式与用户配置的排除关键词合并
        """
        patterns = list(self._builtin_junk_patterns)
        if self._exclude_keywords:
            patterns.extend(k for k in self._exclude_keywords.split("\n") if k)
        return patterns

    def __source_readable(self, path_str: str) -> bool:
        """
        源文件真实可读校验：stat大小>0且能读到数据。失败时强制刷新OpenList与
        rclone两层缓存后重试一次。0字节或仍不可读返回False。
        """
        try:
            p = Path(path_str)
            for attempt in range(2):
                try:
                    if p.exists() and p.stat().st_size > 0:
                        with open(path_str, "rb") as f:
                            if len(f.read(4096)) > 0:
                                return True
                except Exception as e:
                    logger.debug(f"源可读性检查异常：{path_str} - {str(e)}")
                if attempt == 0:
                    ol_parent = self.__map_openlist_path(str(p.parent))
                    if ol_parent and self._openlist_url and self._openlist_token:
                        self.__openlist_list(ol_parent)
                    self.__vfs_refresh(str(p.parent))
        except Exception as e:
            logger.debug(f"源可读性检查失败：{path_str} - {str(e)}")
        return False

    @staticmethod
    def __match_any(keywords: List[str], text: str) -> bool:
        """
        判断文本是否命中任一正则关键字（无效正则忽略）
        """
        for keyword in keywords:
            if not keyword:
                continue
            try:
                if re.findall(keyword, text):
                    return True
            except Exception:
                continue
        return False

    def __clean_junk_dirs(self):
        """
        清理监控目录下只剩垃圾文件（广告/推广/空目录）的一级子目录。
        含任何未被排除的媒体/字幕/音轨文件或未知类型文件时保守不动。
        """
        junk_exts = [e.strip().lower() for e in (self._junk_exts or "").split(",") if e.strip()]
        keep_exts = [str(e).lower() for e in
                     (settings.RMT_MEDIAEXT + settings.RMT_SUBEXT + getattr(settings, "RMT_AUDIOEXT", []))]
        keywords = self.__exclude_patterns()
        for mon_path in self._dirconf.keys():
            if self._dirconf.get(mon_path) is None:
                continue
            root = Path(mon_path)
            if not root.exists():
                continue
            for child in root.iterdir():
                try:
                    if not child.is_dir():
                        continue
                    all_files = [f for f in child.rglob("*") if f.is_file()]
                    has_valuable = False
                    for f in all_files:
                        if f.suffix.lower() in keep_exts and not self.__match_any(keywords, str(f)):
                            has_valuable = True
                            break
                    if has_valuable:
                        continue
                    if all_files and not all(
                            f.suffix.lower() in junk_exts or self.__match_any(keywords, str(f))
                            for f in all_files):
                        # 含未知类型文件，保守起见不动
                        continue
                    logger.warn(f"对账清理：删除垃圾目录 {child}（含 {len(all_files)} 个垃圾文件）")
                    shutil.rmtree(child, ignore_errors=True)
                except Exception as e:
                    logger.warn(f"对账清理 {child} 失败：{str(e)}")


    # region 批次清单:批次级识别(候选作品投票 + TMDB 季集结构 + 规则/LLM 映射 + 确定性校验)

    @staticmethod
    def __clean_name(name: str) -> str:
        """剥掉 8 位十六进制 CRC 与「」『』内的集标题(MP 解析器会把 CRC 当集数、把集标题当剧名)"""
        cleaned = (name or "").translate(FULLWIDTH_TABLE)
        cleaned = CRC_RE.sub(" ", cleaned)
        cleaned = QUOTE_RE.sub(" ", cleaned)
        cleaned = ORDINAL_SEASON_RE.sub(lambda m: f"S{int(m.group(1))}", cleaned)
        return re.sub(r"\s{2,}", " ", cleaned).strip()

    def __clean_meta(self, file_path: Path):
        """基于清洗后文件名解析元数据;清洗无变化时沿用带路径上下文的 MetaInfoPath"""
        cleaned = self.__clean_name(file_path.stem)
        if cleaned and cleaned != file_path.stem:
            meta = MetaInfo(title=cleaned)
            if meta.name:
                return meta
        return MetaInfoPath(file_path)

    @staticmethod
    def __fmt_size(size) -> str:
        size = float(size or 0)
        for unit in ("B", "KB", "MB", "GB"):
            if size < 1024:
                return f"{size:.0f}{unit}" if unit in ("B", "KB") else f"{size:.1f}{unit}"
            size /= 1024
        return f"{size:.2f}TB"

    def __sync_words_sig(self) -> Optional[str]:
        """自定义识别词的指纹;识别词变了时,按标题缓存的识别结果与逐文件判定备忘作废(否则改了词后还拿旧结果)"""
        try:
            sig = hashlib.md5(json.dumps(self.systemconfig.get(SystemConfigKey.CustomIdentifiers) or [],
                                         ensure_ascii=False).encode("utf-8")).hexdigest()
        except Exception:
            sig = None
        if sig != self._words_sig:
            self._recog_cache = {}
            self._verdict_memo = {}
            self._words_sig = sig
        return sig

    def __lexical_extra(self, file_path: Path, mon_path: str) -> Optional[str]:
        """
        残留清理在没有当前批次清单时用:只认确定是特典的——特典目录(硬标记)或文件名硬标记。
        软标记(特典词可能是片名的一部分:The Menu.mkv、Trailer.Park.Boys.S01/)不算特典,宁可留着,不把正片当残留清走
        """
        file_path = Path(file_path)
        try:
            rel = file_path.relative_to(Path(mon_path))
        except ValueError:
            rel = None
        for part in (list(rel.parts[1:-1]) if rel and len(rel.parts) > 2 else []):
            if self.__dir_marker(part)[0] == "hard":
                return f"特典/附加目录({part})"
        if self.__marker_info(self.__clean_name(file_path.stem))[0] == "hard":
            return "特典/附加文件(文件名标记)"
        return None

    def __extra_verdict(self, file_path: Path, mon_path: str) -> Optional[str]:
        """
        启发式特典判定(无批次清单时逐文件整理的回退、通知型目录):① 子目录名(不含种子根目录)② 清洗后文件名标记
        (带显式 SxxExx 的视为正片,避免剧集标题误伤)。特典词可能是片名一部分的"软标记"(The.Menu.2022、
        Trailer.Park.Boys.S01/)按作品标题核对:先看批次目录名识别出的作品(目录名是独立旁证),再看文件名自己识别出的作品;
        特典词都在名字开头的片名区里、且有旁证(目录名/识别词/单文件或目录名就是该片名时的年份与集号)才按正片处理(返回 None)。
        目录名是另一部作品、或特典词不属于识别出的作品的片名:特典;认不出作品、属于片名但没有旁证、年份不符、
        目录名带特典标签、电影片名后跟着发布用词以外的内容:本轮不整理(返回原因,下一轮再核对;残留清理不按这里的结论清走)
        """
        file_path = Path(file_path)
        try:
            rel = file_path.relative_to(Path(mon_path))
        except ValueError:
            rel = None
        items = []
        for part in (list(rel.parts[1:-1]) if rel and len(rel.parts) > 2 else []):
            dkind, dsoft = self.__dir_marker(part)
            if dkind == "hard":
                return f"特典/附加目录({part})"
            if dkind == "soft":
                items.append(dsoft)
        stem = self.__clean_name(file_path.stem)
        kind, soft = self.__marker_info(stem)
        if kind == "hard":
            return "特典/附加文件(文件名标记)"
        if kind == "soft":
            items.append(soft)
        if not items:
            return None
        extra = "特典/附加文件(文件名标记)"
        unverified = "特典/附加文件(带特典词,无法核对是否属于片名,暂不整理)"
        batch_name = rel.parts[0] if rel and len(rel.parts) > 1 else None
        sig = self.__sync_words_sig()
        if self._verdict_memo is None:
            self._verdict_memo = {}
        mkey = f"{batch_name or ''}|{stem}|{sig}"
        now = time.time()
        memo = self._verdict_memo.get(mkey)
        if memo and now - memo[0] < 3600:
            # 一小时内核对过、当时有识别没查到(通知型目录每轮对账都会再判):不重复识别,沿用上次的结论
            return memo[1]
        try:
            meta = MetaInfo(title=stem)
        except Exception:
            meta = None
        sxe = bool(re.search(r"S\d{1,2}E\d{1,3}", unicodedata.normalize("NFKC", stem), re.IGNORECASE))
        file_year = next((it["year"] for it in items if it.get("year")), None) or getattr(meta, "year", None)
        works = []
        missed = False
        if batch_name:
            fmi = self.__folder_work(batch_name, file_year)
            if fmi and getattr(fmi, "tmdb_id", None):
                works.append((fmi, True, False))
            else:
                missed = True
        declared = bool(meta is not None and getattr(meta, "media_id", None)
                        and any(WORD_ID_RE.search(str(w)) for w in (getattr(meta, "apply_words", None) or [])))
        own = self.__recognize_title(stem, MediaType.TV if getattr(meta, "type", None) == MediaType.TV else None)
        if own and getattr(own, "tmdb_id", None):
            if not any(str(w[0].tmdb_id) == str(own.tmdb_id) for w in works):
                works.append((own, False, declared))
        else:
            missed = True

        def done(verdict: str) -> str:
            if missed:
                self._verdict_memo = {k: m for k, m in self._verdict_memo.items() if now - m[0] < 3600}
                self._verdict_memo[mkey] = (now, verdict)
            return verdict

        if not works:
            return done(unverified)
        bnorm = unicodedata.normalize("NFKC", self.__clean_name(batch_name) or batch_name) if batch_name else ""
        folder_miss = elsewhere = False
        loose = None
        for mi, independent, decl in works:
            variants, singles = self.__title_sets(mi)
            spans = [self.__title_region(it["norm"], variants, singles) for it in items]
            tv = getattr(mi, "type", None) == MediaType.TV
            work_year = getattr(mi, "year", None)
            labels = self.__batch_name_labels(batch_name, variants, singles, movie=not tv) if batch_name else []
            titled = bool(batch_name and self.__title_region(bnorm, variants, singles))
            corroborated = independent or decl or sxe or any(
                self.__soft_corroborated(it, sp, work_year, False, tv and titled, not batch_name or titled)
                for it, sp in zip(items, spans))
            if not all(self.__hits_in_region(it["hits"], sp) for it, sp in zip(items, spans)):
                if decl:
                    # 识别词指定了作品,特典词却不在该作品开头的片名里
                    id_spans = self.__word_spans(meta, unicodedata.normalize("NFKC", stem))
                    return done(unverified if self.__id_hold(items, spans, id_spans, self.__hits_in_region) else extra)
                if independent:
                    folder_miss = True
                if not labels and corroborated and self.__title_elsewhere(items, variants, singles):
                    elsewhere = True
                continue
            if not decl and not tv and any(it.get("year") and not self.__year_close(it["year"], work_year)
                                           for it in items):
                # 名字里的年份与作品对不上(同名作品/目录名写错):分不清,不猜
                return done(unverified)
            if corroborated:
                if labels or folder_miss:
                    # 批次目录名带特典标签,或目录名是另一部作品(文件名自己识别出的作品不能推翻它)
                    return done(unverified)
                if not tv and any(not it.get("dir") and not self.__residue_ok(it, sp) for it, sp in zip(items, spans)):
                    # 电影片名后跟着发布用词以外的内容(The Interview (2014) - Line-O-Rama):可能是以片名开头命名的特典
                    return done(unverified)
                logger.info(f"{file_path.name} 名字里的特典词属于《{getattr(mi, 'title', '')}》的片名,按正片处理")
                return None
            if loose is None:
                loose = labels
        if elsewhere:
            return done(unverified)
        if folder_miss or loose is None:
            # 批次目录名是另一部作品(特典词不属于它的片名),或特典词不属于任何识别出的作品的片名:特典
            return done(extra)
        # 特典词属于文件名自己识别出的作品的片名,但没有旁证:批次目录名带特典标签时是特典,否则本轮不整理
        return done(extra if loose else unverified)

    def __folder_work(self, batch_name: str, file_year=None):
        """批次目录名识别出的作品(目录名套上指定 id 的识别词时按 id 查);认不出时 None"""
        bclean = self.__clean_name(batch_name) or batch_name
        try:
            dident = self.__word_identity(MetaInfo(title=bclean))
        except Exception:
            dident = {"source": None}
        if dident.get("source"):
            if dident.get("tmdbid") and dident.get("type"):
                return self.__recognize_id(dident["tmdbid"], dident["type"])
            return None
        dtitle = re.sub(r"\s{2,}", " ", ID_TAG_RE.sub(" ", bclean)).strip()
        return self.__recognize_dir_title(dtitle, file_year)[0] if dtitle else None

    def __recognize_dir_title(self, dtitle: str, file_year=None, mtype: Optional[MediaType] = None):
        """
        按目录名识别。目录名不带年份而本批文件带年份(非剧集批次)时先按"目录名 年份"识别:裸标题(The Interview、
        Halloween)会落到最新的同名作品上;不带年份识别出的作品与文件年份相差超过 1 年时不作依据。
        返回 (作品, 年份不符的同名作品)
        """
        bare = False
        if file_year and mtype != MediaType.TV:
            try:
                bare = not MetaInfo(title=dtitle).year
            except Exception:
                bare = False
        if bare:
            mi = self.__recognize_title(f"{dtitle} {file_year}", mtype)
            if mi and getattr(mi, "tmdb_id", None) and self.__year_close(getattr(mi, "year", None), file_year):
                return mi, None
        mi = self.__recognize_title(dtitle, mtype)
        if bare and mi and getattr(mi, "tmdb_id", None) and not self.__year_close(getattr(mi, "year", None), file_year):
            return None, mi
        return mi, None

    @staticmethod
    def __year_close(a, b, tol: int = 1) -> bool:
        """两个年份相差不超过 tol 年;任一方缺失或不是数字时不算不符"""
        try:
            return abs(int(str(a)[:4]) - int(str(b)[:4])) <= tol
        except (TypeError, ValueError):
            return True

    @staticmethod
    def __marker_info(cleaned: str) -> Tuple[Optional[str], Optional[dict]]:
        """
        文件名特典标记(只看文件名本身,不依赖解析器取的名字,也不受识别词影响):返回 (类别, 软标记信息)。
        类别 None=无标记(或带显式 SxxExx);"soft"=命中的全是 SOFT_MARKER_WORDS,这些词和其余特典标签(EXTRA_LABEL_RE)
        都在片名区里(开头发布标签之后、第一个年份/季集号/集数/技术信息之前;中文名.年份.英文名 的英文名也算)、
        不是整个括号里只有它([Menu]、(Trailer)),片名区也不只剩特典词本身(或带年份):可能是片名的一部分,
        要按作品标题核对;其余为 "hard"。软标记信息 {"norm": 归一化文件名, "hits": [(起, 止, 词)] 含全部特典标签,
        "year": 文件名里的年份}
        """
        norm = unicodedata.normalize("NFKC", cleaned or "")
        if re.search(r"S\d{1,2}E\d{1,3}", norm, re.IGNORECASE):
            return None, None

        def spans(regex):
            # 去掉命中尾部的空白(Preview (1989) 的命中只是 Preview)
            return [(m.start(), m.start() + len(m.group(0).rstrip()), m.group(0).strip())
                    for m in regex.finditer(norm) if m.group(0).strip()]

        hits = spans(SP_FILE_RE)
        if not hits:
            return None, None
        if any(re.sub(r"\s+", " ", h[2]).lower() not in SOFT_MARKER_WORDS for h in hits):
            return "hard", None
        labels = hits + spans(EXTRA_LABEL_RE)
        lead, zone_end, _, tokens = _name_zone(norm)
        year = next((int(t[0]) for t in tokens if YEAR_TOKEN_RE.fullmatch(t[0])), None)
        for start, end, _ in labels:
            if end > zone_end:
                # 特典词/标签在年份、季集号或技术信息之后(The.Menu.2022.Trailer、Movie (2022)-deleted):是标签
                return "hard", None
            before, after = norm[:start].rstrip(), norm[end:].lstrip()
            if before[-1:] in ("[", "(", "（", "【") and after[:1] in ("]", ")", "）", "】") \
                    and not re.match(r"[\])）】]\s*[\[(（【]?\s*(?:19|20)\d{2}(?!\d)", after):
                # 整个括号里只有它([Menu]、(Trailer)、【Preview】):是标签
                return "hard", None
        zone_words = [t[0] for t in tokens if t[1] >= lead and t[2] <= zone_end] \
            or [t[0] for t in tokens if t[2] <= zone_end]
        if year is None and all(w in SOFT_MARKER_WORDS for w in zone_words):
            # 片名区只剩特典词本身(Trailer.mkv、Sample.mkv、[Grp] Menu、[4K] Trailer):特典;
            # 带年份的(Interview.2007)仍可能是片名
            return "hard", None
        return "soft", {"norm": norm, "hits": labels, "year": year}

    @staticmethod
    def __dir_marker(part: str) -> Tuple[Optional[str], Optional[dict]]:
        """
        子目录名的特典标记:(None/"hard"/"soft", 软标记信息)。"soft":命中的全是 SOFT_DIR_WORDS、都在目录名的片名区里,
        且片名区里还有光盘/容器/连接词/编号以外的词(Trailer Park Boys、Night Gallery、音乐之声)或片名区后接年份/季集号
        (Extras.S01、The Others (2001)):可能是作品名的一部分,要按作品标题核对;整个目录名就是附加目录名(Featurettes、
        Extras、特辑+花絮)、裸标签目录(Bonus Disc、Sample、Extras Vol 1)与其余命中一律 "hard"
        """
        raw = str(part).strip()
        if SP_DIR_NAME_RE.fullmatch(raw):
            return "hard", None
        norm = unicodedata.normalize("NFKC", raw)
        hits = [(m.start(), m.end(), m.group(0)) for m in SP_DIR_RE.finditer(norm) if m.group(0)]
        if not hits:
            return None, None
        if any(h[2].lower() not in SOFT_DIR_WORDS for h in hits):
            return "hard", None
        # 其余特典标签(Trailer.Park.Boys.S01.Featurettes、Extras.S01.Deleted.Scenes)同样要在片名区里、被作品标题解释
        hits += [(m.start(), m.start() + len(m.group(0).rstrip()), m.group(0).strip())
                 for m in EXTRA_LABEL_RE.finditer(norm) if m.group(0).strip()]
        lead, zone_end, ended, tokens = _name_zone(norm)
        if any(h[1] > zone_end for h in hits):
            return "hard", None
        # 片名区里除特典词外只剩光盘/容器/连接词/编号(Bonus Disc、Extras Vol 1、Extras and Trailers、Trailers):特典目录;
        # 片名区后接年份/季集号的(Extras.S01、The Others (2001))与带别的词的(Trailer Park Boys、Night Gallery)按作品标题核对
        rest = [t[0] for t in tokens if t[1] >= lead and t[2] <= zone_end
                and not any(h[0] <= t[1] and t[2] <= h[1] for h in hits)]
        stop = next((t[0] for t in tokens if t[1] == zone_end), "") if ended else ""
        if not (YEAR_TOKEN_RE.fullmatch(stop) or re.fullmatch(r"s\d{1,2}(?:e\d{1,4})?|e\d{1,4}|ep\d{1,4}", stop)) \
                and all(w in DIR_VOCAB or w.isdigit() for w in rest):
            return "hard", None
        year = next((int(t[0]) for t in tokens if YEAR_TOKEN_RE.fullmatch(t[0])), None)
        return "soft", {"norm": norm, "hits": hits, "year": year, "dir": raw}

    @staticmethod
    def __title_variants(names: list) -> set:
        """标题的词元序列;带副标题的另收冒号/破折号前的主标题"""
        variants = set()
        for name in names or []:
            if not name or not isinstance(name, str):
                continue
            name = unicodedata.normalize("NFKC", name)
            for part in (name, re.split(r"\s*[:：]\s*|\s+[-–—]\s+", name, 1)[0]):
                tokens = tuple(t[0] for t in _title_tokens(part))
                if tokens:
                    variants.add(tokens)
        return variants

    def __title_sets(self, mi, title: Optional[str] = None) -> Tuple[set, set]:
        """
        作品全部标题(中文名/原名/英文名/别名/译名)的词元序列,以及可以单独成立的单个词标题:拉丁单词只认正式名
        (中文名/原名/英文名),别名/译名里的单个英文词(The Interview 的别名 Interview)不算;整段中日韩文字的标题都算
        """
        mains = [title, getattr(mi, "title", None), getattr(mi, "original_title", None), getattr(mi, "en_title", None)]
        variants = self.__title_variants(mains + list(getattr(mi, "names", None) or []))
        singles = {v for v in self.__title_variants(mains) if len(v) == 1}
        # 别名本身就是特典词的(The Interview 的别名「采访」)不能当片名,否则会吞掉目录名里的特典标签
        singles |= {v for v in variants if len(v) == 1 and not re.match(r"[0-9a-z]", v[0])
                    and not EXTRA_LABEL_RE.fullmatch(v[0]) and not SP_FILE_RE.fullmatch(v[0])}
        return variants, singles

    @staticmethod
    def __title_region(norm: str, variants: set, singles: set) -> Optional[List[Tuple[int, int]]]:
        """
        名字开头的片名区:从开头(或开头的发布标签之后)起,由作品标题(完整词元序列;单个词只认正式名)、跟在标题后的年份、
        以及两段标题之间的发布标签(字幕/音轨/季集数/版本/技术信息,菜单[简繁英字幕].The.Menu)连成;返回其中每段标题在名字里的
        位置 [(起, 止)](特典词必须落在某段标题里,落在标签里的不算);开头不是作品标题时 None
        """
        tokens = _title_tokens(norm)
        words = [t[0] for t in tokens]
        n = len(words)

        def title_at(i: int) -> int:
            return max((len(v) for v in variants
                        if tuple(words[i:i + len(v)]) == v and (len(v) > 1 or v in singles)), default=0)

        starts = {0}
        first = LEAD_GROUP_RE.match(norm)
        for cut in {0, (first.end() if first else 0), _lead_end(norm)}:
            idx = next((i for i, t in enumerate(tokens) if t[1] >= cut), None)
            if idx:
                starts.add(idx)
            # 标题前只隔着发布标签([1080P][中英字幕]The.Menu、[4K][国语中字]菜单.The.Menu)时从标题起算
            j = next((i for i, t in enumerate(tokens) if t[1] >= cut and title_at(i)), None)
            if j is not None and TITLE_GAP_RE.fullmatch(norm[cut:tokens[j][1]]):
                starts.add(j)
        best = None
        for start in sorted(starts):
            i, spans, end = start, [], 0
            while i < n:
                k = title_at(i)
                if k:
                    spans.append((tokens[i][1], tokens[i + k - 1][2]))
                    end = tokens[i + k - 1][2]
                    i += k
                    continue
                if not spans:
                    break
                if YEAR_TOKEN_RE.fullmatch(words[i]):
                    end = tokens[i][2]
                    i += 1
                    continue
                # 两段标题之间的发布标签:下一段标题前只隔着字幕/音轨/季集数/版本/技术信息时接上
                j = next((j for j in range(i + 1, min(n, i + 12)) if title_at(j)), None)
                if j is None or not TITLE_GAP_RE.fullmatch(norm[end:tokens[j][1]]):
                    break
                i = j
            if spans and (best is None or spans[-1][1] > best[-1][1]):
                best = spans
        return best

    @staticmethod
    def __hits_in_region(hits: list, spans: Optional[List[Tuple[int, int]]]) -> bool:
        """每一处特典词/标签都落在片名区的某段标题里(逐处判断:Trailer.Park.Boys.The.Movie 片名区之外再出现的 Trailer 不算)"""
        return bool(spans) and bool(hits) and all(any(s <= h[0] and h[1] <= e for s, e in spans) for h in hits)

    @staticmethod
    def __title_occurrences(norm: str, variants: set, singles: set) -> List[Tuple[int, int]]:
        """名字里任意位置出现的作品标题 [(起, 止)](不要求在开头)"""
        tokens = _title_tokens(norm)
        words = [t[0] for t in tokens]
        spans = []
        for i in range(len(words)):
            for v in variants:
                if tuple(words[i:i + len(v)]) == v and (len(v) > 1 or v in singles):
                    spans.append((tokens[i][1], tokens[i + len(v) - 1][2]))
        return spans

    @staticmethod
    def __residue_ok(soft: dict, spans: Optional[List[Tuple[int, int]]]) -> bool:
        """
        电影文件名在开头片名(和紧跟的年份)之后、第一个技术信息之前只有发布用词(版本/剪辑/语言/规格;方括号里的标签、
        末尾的 -发布组 不算)。The Interview (2014) - Line-O-Rama、The.Interview.2014.Seth.Rogen.and.James.Franco.1080p 不是
        """
        if not spans:
            return False
        norm = soft["norm"]
        tokens = [t for t in _title_tokens(norm) if t[1] >= spans[-1][1]]
        pos = spans[-1][1]
        while tokens and YEAR_TOKEN_RE.fullmatch(tokens[0][0]):
            pos = tokens[0][2]
            tokens = tokens[1:]
        stop = next((t[1] for t in tokens if ZONE_END_RE.fullmatch(t[0])), len(norm))
        rest = re.sub(r"[\[【][^\]】]*[\]】]", " ", norm[pos:stop]).replace("'", "").replace(chr(0x2019), "")
        if stop == len(norm):
            rest = re.sub(r"-[0-9A-Za-z]+\s*$", "", rest)
        return bool(RESIDUE_RE.fullmatch(rest))

    @staticmethod
    def __soft_corroborated(soft: dict, spans: Optional[List[Tuple[int, int]]], work_year,
                            independent: bool = False, tv: bool = False, allow_self: bool = True) -> bool:
        """
        特典词属于片名之外的旁证,防止"Making Of.mkv / Director's Commentary 01.mkv 识别成片名就叫这个的作品、再拿它的标题
        解释自己"的循环:作品来自本文件以外的证据(识别词/下载记录直通/批次目录名/其他正片文件);或(allow_self:单文件批次,
        或批次目录名开头就是该作品的片名)名字里的年份与作品相差不超过 1 年,或剧集片名区里有特典词和虚词以外的词
        (Trailer Park Boys 的 Park/Boys、中文片名)且紧跟集号/季号。名字自己的年份/集号是拿来识别它的依据,单独不能自证
        """
        if independent:
            return True
        if not allow_self:
            return False
        try:
            if soft.get("year") and work_year and abs(int(soft["year"]) - int(str(work_year)[:4])) <= 1:
                return True
        except (TypeError, ValueError):
            pass
        if not tv or not spans or not EPISODE_AFTER_RE.match(soft["norm"][spans[-1][1]:]):
            return False
        for word, start, end in _title_tokens(soft["norm"]):
            if not any(s <= start and end <= e for s, e in spans):
                continue
            if word in SOFT_MARKER_WORDS or word in TITLE_STOPWORDS or word.isdigit():
                continue
            if any(h[0] <= start and end <= h[1] for h in soft["hits"]):
                continue
            return True
        return False

    def __batch_name_labels(self, batch_name: str, variants: set, singles: set, movie: bool = False) -> List[str]:
        """
        批次目录名里作品标题解释不了、也不在"附带"语境(+ Extras、with Bonus、含花絮特辑、附赠花絮、
        [Trailers & Extras Included])里的特典词/标签(Movie.2022.Trailer/、Movie.2022.EXTRAS/、Movie Special Features/、
        电影的 Specials/):目录说明整批可能只是特典,其中特典词属于片名的文件分不清是正片还是特典。
        带 SxxExx 的目录名不看(剧集的集标题可能正好是这些词)
        """
        bnorm = unicodedata.normalize("NFKC", self.__clean_name(batch_name) or batch_name or "")
        if re.search(r"S\d{1,2}E\d{1,3}", bnorm, re.IGNORECASE):
            return []
        titled = self.__title_region(bnorm, variants, singles) or []
        found = []
        for regex in (SP_FILE_RE, EXTRA_LABEL_RE) + ((MOVIE_LABEL_RE,) if movie else ()):
            for m in regex.finditer(bnorm):
                text = m.group(0)
                if not text.strip():
                    continue
                start = m.start() + len(text) - len(text.lstrip())
                found.append([start, m.start() + len(text.rstrip())])
        merged: List[List[int]] = []
        for s, e in sorted(found):
            if merged and s < merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], e)
            else:
                merged.append([s, e])
        # 片名里的词(菜单[中文字幕].The.Menu 的 Menu)不是标签
        labels = [(s, e) for s, e in merged if not any(a <= s and e <= b for a, b in titled)]
        n = len(labels)
        fwd, bwd = [False] * n, [False] * n
        for i, (s, e) in enumerate(labels):
            # 附带语境之后连着列出的标签(含花絮特辑、+ Extras (Trailers, Featurettes))
            fwd[i] = bool(INCLUDED_BEFORE_RE.search(bnorm[:s])) or (
                i > 0 and fwd[i - 1] and bool(LABEL_RUN_GAP_RE.fullmatch(bnorm[labels[i - 1][1]:s])))
        for i in range(n - 1, -1, -1):
            s, e = labels[i]
            # "… Included" 前面连着列出的标签([Trailers & Extras Included]);普通连词(Trailers & Featurettes)不往前传
            bwd[i] = bool(INCLUDED_AFTER_RE.match(bnorm[e:])) or (
                i < n - 1 and bwd[i + 1] and bool(LABEL_RUN_GAP_RE.fullmatch(bnorm[e:labels[i + 1][0]])))
        return sorted({bnorm[s:e] for i, (s, e) in enumerate(labels) if not fwd[i] and not bwd[i]})

    def __work_titles(self, series: dict) -> Optional[Tuple[set, set]]:
        """主候选作品在 TMDB 上的全部标题(按 id 与类型查,进程内缓存);取不到时 None"""
        try:
            mtype = MediaType(series.get("type"))
        except ValueError:
            return None
        if mtype not in (MediaType.MOVIE, MediaType.TV) or not series.get("tmdbid"):
            return None
        mi = self.__recognize_id(series["tmdbid"], mtype)
        if not mi or str(getattr(mi, "tmdb_id", "")) != str(series["tmdbid"]):
            return None
        return self.__title_sets(mi, series.get("title"))

    def __title_elsewhere(self, items: list, variants: set, singles: set) -> bool:
        """
        开头片名区解释不了,但每处特典词/标签都落在名字里某段作品标题里,且那段标题前只有中日韩文字、括号标签和技术信息
        (菜單.The.Menu.2022、拖车公园男孩.Trailer.Park.Boys.The.Movie.2006:片名前有认不出的译名)
        """
        for it in items:
            occ = self.__title_occurrences(it["norm"], variants, singles)
            if not self.__hits_in_region(it["hits"], occ):
                return False
            first = min(s for s, e in occ if any(s <= h[0] and h[1] <= e for h in it["hits"]))
            head = re.sub(r"[\[【(（][^\]】)）]*[\]】)）]", " ", it["norm"][:first])
            if any(re.match(r"[0-9a-z]", t[0]) and not ZONE_END_RE.fullmatch(t[0]) for t in _title_tokens(head)):
                return False
        return True

    @staticmethod
    def __word_spans(meta, norm: str) -> Optional[List[Tuple[int, int]]]:
        """指定作品 id 的识别词在名字里匹配到的位置(用户声明这段就是片名);定位不到或词写不成正则时 None"""
        spans = []
        for w in getattr(meta, "apply_words", None) or []:
            w = str(w)
            if "=>" not in w or not WORD_ID_RE.search(w):
                continue
            left = w.split("=>")[0].strip()
            if not left:
                continue
            try:
                spans += [(m.start(), m.end()) for m in re.finditer(left, norm) if m.end() > m.start()]
            except re.error:
                return None
        return spans or None

    @staticmethod
    def __id_hold(items: list, spans: list, id_spans: Optional[List[Tuple[int, int]]], explained_by) -> bool:
        """
        识别词指定了作品、该作品开头的片名却解释不了的特典词:落在识别词匹配的那段(用户声明它就是片名)里、
        在子目录名里、或定位不到识别词时分不清(True);都在那段之外时是标签,按特典(False)
        """
        if id_spans is None:
            return True
        for it, sp in zip(items, spans):
            for h in it["hits"]:
                if explained_by([h], sp):
                    continue
                if it.get("dir") or any(s < h[1] and h[0] < e for s, e in id_spans):
                    return True
        return False

    def __resolve_soft_markers(self, batch_root: Path, batch_name: str, parsed: Dict[str, dict],
                               soft_rels: List[str], primary: dict, independent: bool) -> Optional[dict]:
        """
        作品已定:软标记文件(文件名或子目录名里的特典词可能是片名一部分)按其全部标题核对,结论:
        True=正片候选(特典词都在名字开头片名区的标题里,且有旁证 __soft_corroborated;识别词指定了作品的文件要指定的
        就是本批作品);False=特典(特典词不属于片名;或属于片名但没有旁证而批次目录名带特典标签、目录名又不是该作品);
        "hold"=待人工,不整理也不当特典清走(识别词对不上、电影年份与作品不符、属于片名但没有任何旁证、批次目录名带特典标签
        而文件名像正片、片名前有认不出的内容)。
        返回 {"state": {文件: 结论}, "notes": {文件: 待人工原因}, "conflict": [批次目录名里的特典标签],
        "loose": [属于片名但没有旁证的文件], "folder_titled": 批次目录名开头就是该作品的片名,
        "residue": [片名后跟着发布用词以外内容的电影文件]};取不到作品标题时 None
        """
        titles = self.__work_titles(primary)
        if titles is None:
            return None
        variants, singles = titles
        tv = primary.get("type") == MediaType.TV.value
        work_year = primary.get("year")
        is_dir = not batch_root.is_file()
        conflict = self.__batch_name_labels(batch_name, variants, singles, movie=not tv) if is_dir else []
        bnorm = unicodedata.normalize("NFKC", self.__clean_name(batch_name) or batch_name or "")
        folder_titled = bool(is_dir and self.__title_region(bnorm, variants, singles))
        # 名字自己的年份只在单文件批次或批次目录名就是该作品片名时算旁证,剧集"片名后接集号"只在目录名就是该作品片名时算:
        # 目录批次里的通用名特典(Special Interview 01、Trailer (2014))识别成同名作品后会拿自己的集号/年份循环自证
        allow_self = not is_dir or folder_titled
        tv_rule = tv and folder_titled
        remedy = "(改好目录名/文件名或识别词后,新文件出现或清单过期时会重新核对)"
        both = f"批次目录名带特典标签 {'/'.join(conflict)},名字里的特典词又属于片名,分不清是正片还是特典,待人工{remedy}"
        state: Dict[str, Any] = {}
        notes: Dict[str, str] = {}
        loose: List[str] = []
        residue: List[str] = []
        for rel in soft_rels:
            v = parsed[rel]
            items = v["soft"]
            spans = [self.__title_region(it["norm"], variants, singles) for it in items]
            explained = all(self.__hits_in_region(it["hits"], sp) for it, sp in zip(items, spans))
            corroborated = independent or v.get("sxe") or any(
                self.__soft_corroborated(it, sp, work_year, False, tv_rule, allow_self) for it, sp in zip(items, spans))
            ok: Any = False
            if v.get("id_source"):
                if v.get("asserted_tmdb") != primary["tmdbid"]:
                    ok, notes[rel] = "hold", f"识别词指定的不是本批作品,待人工{remedy}"
                elif explained:
                    ok = True
                elif self.__id_hold(items, spans, v.get("id_spans"), self.__hits_in_region):
                    ok, notes[rel] = "hold", f"识别词指定的作品与名字里的特典词对不上,待人工{remedy}"
            elif not explained:
                if not conflict and corroborated and self.__title_elsewhere(items, variants, singles):
                    # 特典词都落在名字里某段作品标题里,只是片名前有认不出的内容:不当特典清走
                    ok, notes[rel] = "hold", f"名字里的特典词属于片名,但片名前有认不出的内容,待人工{remedy}"
            else:
                off = next((it["year"] for it in items
                            if not tv and it.get("year") and not self.__year_close(it["year"], work_year)), None)
                if off:
                    ok, notes[rel] = "hold", f"名字里的年份 {off} 与作品年份 {work_year} 不符(可能是同名作品),待人工{remedy}"
                elif corroborated:
                    ok = True
                elif not conflict:
                    ok, notes[rel] = "hold", f"名字里的特典词属于片名,但没有年份等旁证,分不清是正片还是特典,待人工{remedy}"
                    loose.append(rel)
                elif folder_titled:
                    # 目录名开头就是该作品的片名(目录名识别瞬断时本该是旁证):不能因此当特典清走
                    ok, notes[rel] = "hold", both
            if ok is True and conflict:
                ok, notes[rel] = "hold", both
            if ok is True and not tv and any(not it.get("dir") and not self.__residue_ok(it, sp)
                                             for it, sp in zip(items, spans)):
                residue.append(rel)
            v["marker"] = None if ok is True else v.get("soft_note") or "文件名标记"
            state[rel] = ok
        kept = [Path(r).name for r, ok in state.items() if ok is True]
        if kept:
            logger.info(f"批次 {batch_name}:{len(kept)} 个名字里的特典词属于《{primary['title']}》({primary['year']})"
                        f"的片名,按正片候选处理:" + "; ".join(kept[:10]) + (" ..." if len(kept) > 10 else ""))
        held = [f"{Path(r).name}({notes.get(r)})" for r, ok in state.items() if ok == "hold"]
        if held:
            logger.info(f"批次 {batch_name}:{len(held)} 个带特典词的文件分不清是正片还是特典:" + "; ".join(held[:10]))
        rest = sum(1 for ok in state.values() if ok is False)
        if rest:
            logger.info(f"批次 {batch_name}:{rest} 个带特典词的文件仍按特典(特典词不属于《{primary['title']}》"
                        f"开头的片名" + (f",且批次目录名带特典标签 {'/'.join(conflict)}" if conflict else "") + ")")
        return {"state": state, "notes": notes, "conflict": conflict, "loose": loose, "folder_titled": folder_titled,
                "residue": residue}

    def __batch_root(self, file_path: Path, mon_path: str) -> Optional[Path]:
        """文件所属批次根目录(监控目录下的一级子目录);直接位于监控目录的单文件无批次"""
        try:
            rel = Path(file_path).relative_to(Path(mon_path))
        except ValueError:
            return None
        if len(rel.parts) < 2:
            return None
        return Path(mon_path) / rel.parts[0]

    def __group_by_batch(self, mon_path: str, files: list) -> List[Tuple[Optional[Path], List[Path]]]:
        groups: Dict[str, Tuple[Optional[Path], List[Path]]] = {}
        for f in files:
            root = self.__batch_root(Path(f), mon_path)
            groups.setdefault(str(root) if root else "", (root, []))[1].append(Path(f))
        return list(groups.values())

    def __order_batch(self, mf: Optional[dict], batch_root: Path, files: List[Path]) -> List[Path]:
        """按清单排序:正片(季集序) -> 特别篇 -> 对应字幕 -> 其余;无清单按路径排序"""
        if not mf or mf.get("status") != "ok":
            return sorted(files)
        rank = {"main": 0, "special": 1}
        sub_exts = [str(e).lower() for e in settings.RMT_SUBEXT]

        def key(f: Path):
            entry = self.__manifest_entry(mf, batch_root, f) or {}
            cls = entry.get("c")
            order = rank.get(cls, 8)
            if f.suffix.lower() in sub_exts:
                order = 4 if cls in rank else 9
            season = entry.get("s") if entry.get("s") is not None else 999
            episode = entry.get("e") if entry.get("e") is not None else 999
            return order, season, episode, str(f)

        return sorted(files, key=key)

    def __batch_listing(self, batch_root: Path) -> Dict[str, list]:
        """批次内媒体文件 (相对路径, 大小, mtime) 与字幕相对路径(60 秒缓存,避免反复遍历挂载目录)"""
        key = str(batch_root)
        now = time.time()
        if self._batch_list_cache is None:
            self._batch_list_cache = {}
        cached = self._batch_list_cache.get(key)
        if cached and now - cached[0] < 60:
            return cached[1]
        media, subs = [], []
        if batch_root.is_file():
            # 直接位于监控目录根下的单个媒体文件:视为只有一个文件的批次
            try:
                st = batch_root.stat()
                if batch_root.suffix.lower() in [str(e).lower() for e in settings.RMT_MEDIAEXT]:
                    media.append((batch_root.name, st.st_size, st.st_mtime))
            except Exception as e:
                logger.warn(f"读取文件信息失败 {batch_root}:{str(e)}")
            listing = {"media": media, "subs": subs}
            self._batch_list_cache[key] = (now, listing)
            return listing
        try:
            for f in SystemUtils.list_files(batch_root, settings.RMT_MEDIAEXT):
                try:
                    st = Path(f).stat()
                except Exception:
                    continue
                media.append((str(Path(f).relative_to(batch_root)).replace("\\", "/"), st.st_size, st.st_mtime))
            for f in SystemUtils.list_files(batch_root, settings.RMT_SUBEXT):
                subs.append(str(Path(f).relative_to(batch_root)).replace("\\", "/"))
        except Exception as e:
            logger.warn(f"列举批次文件失败 {batch_root}:{str(e)}")
        media.sort()
        subs.sort()
        listing = {"media": media, "subs": subs}
        self._batch_list_cache[key] = (now, listing)
        return listing

    def __listed(self, batch_root: Path, file_path: Path) -> bool:
        """媒体文件在批次列举(可能是 60 秒内的缓存)里"""
        try:
            rel = str(Path(file_path).relative_to(batch_root)).replace("\\", "/")
        except ValueError:
            return True
        return any(m[0] == rel for m in self.__batch_listing(batch_root)["media"])

    def __get_manifest(self, batch_root: Path) -> Optional[dict]:
        """
        取批次清单:None=无法建立(回退逐文件识别);{"pending": True, "reason", "retry_in"}=暂缓整理,本轮跳过并复查
        (文件仍在变化 / 认不出作品 / TMDB 瞬时故障)。清单按批次目录持久化;文件被搬走不重算,出现新文件才重建;
        建立失败按次数退避重试(10 分钟起,最长 6 小时);挂起状态也落盘,退避未到期时只读状态不重算。
        """
        key = str(batch_root)
        listing = self.__batch_listing(batch_root)
        media = listing["media"]
        if not media:
            return None
        now = time.time()
        rels = [m[0] for m in media]
        digest = hashlib.md5("\n".join(rels).encode("utf-8")).hexdigest()
        if self._manifest_lock is None:
            self._manifest_lock = threading.RLock()
        with self._manifest_lock:
            if self._manifests is None:
                self._manifests = {}
            # 带测试开关/只读的预览:不复用、不改动已有清单(既不读也不写)
            old = None if self._order_override else self._manifests.get(key)
            # 预览 API 强制重建:不沿用已有清单与退避;已有清单仍是"重建失败时继续暂缓"的依据
            force = bool(getattr(self._override_tls, "rebuild", False))
            if old and not force:
                kept = self.__batch_rels(batch_root, rels)
                old_files = old.get("files") or {}
                # 只要本批会被整理的文件都在清单里(没有新文件)就沿用:不看原始 digest,
                # 否则过滤词放开后,建清单时被过滤掉的文件会没有条目而落到逐文件识别。
                # 旧版的全标记清单(没识别作品就整批按文件名特典词跳过,The Menu 这类电影会被当特典)按新规则重建
                stale_marker = self.__stale_marker_manifest(old)
                if stale_marker:
                    logger.info(f"批次 {batch_root.stem if batch_root.is_file() else batch_root.name} 的清单按旧版"
                                f"文件名特典词规则建立,按新规则重建")
                if old.get("status") == "ok" and set(kept) <= set(old_files.keys()) and not stale_marker:
                    if old.get("digest") != digest and kept and all(
                            (old_files.get(r) or {}).get("c") in ("extra", "other")
                            or (old_files.get(r) or {}).get("llm_c") for r in kept):
                        # 正片已搬走、只剩特典/杂项/LLM 判为特典但带集号的文件:保持清单常新,不让 7 天清理后
                        # 只凭残留文件重建(残留的特典会被当成正片映射)
                        if now - float(old.get("time") or 0) > 86400:
                            old["time"] = now
                            self.__save_manifests()
                    return old
                if old.get("status") in ("failed", "no_media") and old.get("digest") == digest:
                    attempts = int(old.get("attempts") or 1)
                    if now - (old.get("time") or 0) < (600 if attempts <= 1 else 1200 if attempts == 2 else 6 * 3600):
                        return None
            retry_old = old if (old and old.get("status") == "retry" and old.get("digest") == digest) else None
            if retry_old and not self._order_override and not force \
                    and now < float(retry_old.get("next_retry") or 0):
                # 挂起中的批次退避未到期:只读状态,不重算(不重复识别/调 TMDB/刷日志)
                return {"pending": True, "cached": True, "reason": retry_old.get("reason"),
                        "retry_in": int(float(retry_old["next_retry"]) - now) + 1}
            if self._batch_seen is None:
                self._batch_seen = {}
            if retry_old and key not in self._batch_seen:
                # 挂起中的批次文件集合没变(同一 digest):稳定窗口从上次建立时算起,重启/重载后手动运行不必再等一轮
                self._batch_seen[key] = (digest, float(retry_old.get("time") or now))
            seen = self._batch_seen.get(key)
            if not seen or seen[0] != digest:
                seen = (digest, now)
                if not self._order_override:
                    # 预览不改动实时批次的稳定窗口起点
                    self._batch_seen[key] = seen
            # 预览 API 不等稳定窗口(只作用于预览线程,不改共享配置)
            settle = 0 if getattr(self._override_tls, "no_settle", False) else max(0, int(self._batch_settle or 0))
            newest = max(m[2] for m in media)
            if settle and (now - seen[1] < settle or now - newest < settle):
                # 返回剩余稳定时间,调用方据此安排复查(未来时间戳的文件按"刚出现"处理)
                remaining = settle - min(now - seen[1], max(0.0, now - newest))
                return {"pending": True, "reason": "文件仍在变化", "retry_in": int(max(5, min(settle, remaining)) + 3)}
            mf = self.__build_manifest(batch_root, listing, digest)
            if old and old.get("status") in ("ok", "retry") and not mf.get("pending") and mf.get("status") == "failed":
                # 已有清单(定案或挂起中)的批次重建失败(LLM 无结果/出错、建立清单异常):继续暂缓,不回退逐文件识别
                # (挂起的、待人工的文件恰恰是不能逐文件猜的);沿用挂起原因与类型,连续轮数、退避与通知照常累计
                prev = old if old.get("status") == "retry" else {}
                logger.warn(f"批次 {batch_root.stem if batch_root.is_file() else batch_root.name} 重建清单失败"
                            f"(LLM 无结果/出错),保持暂缓,不回退逐文件识别")
                mf = {"pending": True, "retry_in": 1800, "tv": prev.get("tv", True),
                      "kind": prev.get("kind") or "transient",
                      "reason": prev.get("reason") or "重建批次清单失败(LLM 无结果或出错)"}
            if mf.get("pending"):
                # 暂缓整理(认不出作品/TMDB 瞬时故障):跳过并复查,不回退逐文件识别;
                # 状态落盘(重启不丢、对账不重算),复查间隔按同类原因的连续次数翻倍,上限 6 小时
                kind = mf.get("kind") or "transient"
                same_kind = bool(retry_old) and (retry_old.get("kind") or "transient") == kind
                n = (int(retry_old.get("rounds") or 0) if same_kind else 0) + 1
                retry_in = min(int(mf.get("retry_in") or 900) * (2 ** min(n - 1, 4)), 6 * 3600)
                mf["retry_in"] = retry_in
                name = batch_root.stem if batch_root.is_file() else batch_root.name
                logger.warn(f"批次 {name} 暂缓整理:{mf.get('reason')}(第 {n} 次),{retry_in // 60} 分钟后自动复查")
                if self._order_override:
                    return mf
                notified = bool(same_kind and retry_old.get("notified"))
                # 需要人处理的原因(认不出作品/分不清同名作品/识别词问题)连续 3 次、其他原因(TMDB 结构拉不到/下载记录的季集
                # 还没收录)连续 5 次才通知,每种情况只通知一次;通知关着时不记"已通知",打开后下一轮还会补发
                threshold = 3 if kind in ("no_evidence", "ambiguous", "asserted_unresolved") else 5
                if not notified and self._notify and n >= threshold:
                    word_type = "tv" if mf.get("tv", True) else "movie"
                    if kind == "no_evidence":
                        title = f"「{name}」无法确认所属作品,已暂停整理"
                        text = (f"原因:{mf.get('reason')}\n"
                                f"处理:在 MoviePilot「设定 → 词表 → 自定义识别词」添加映射"
                                f"(如「文件里的剧名 => {{[tmdbid=TMDB 上的 id;type={word_type}]}}」,"
                                f"剧集写 type=tv、电影写 type=movie),再在插件里点「立即运行一次」。\n"
                                f"若只是 TMDB 网络故障,恢复后会自动继续(插件最长每 6 小时重试一次)")
                    elif kind == "asserted_unresolved":
                        title = f"「{name}」的自定义识别词没有生效,已暂停整理"
                        text = (f"原因:{mf.get('reason')}\n"
                                f"请检查识别词写法(「文件里的剧名 => {{[tmdbid=TMDB 上的 id;type={word_type}]}}」),"
                                f"改好后在插件里点「立即运行一次」;若只是 TMDB 暂时不可用,恢复后会自动继续")
                    else:
                        title = f"「{name}」连续 {n} 次暂缓整理"
                        text = (f"原因:{mf.get('reason')}\n"
                                f"TMDB 网络故障或数据更新后会自动继续(插件最长每 6 小时重试一次);"
                                f"长期如此请检查 TMDB 上该作品的季集信息")
                    try:
                        if kind == "ambiguous":
                            text += (f"\n处理:在「自定义识别词」添加「文件里的剧名 => {{[tmdbid=正确的 id;type={word_type}]}}」,"
                                     f"再点「立即运行一次」")
                        self.post_message(mtype=NotificationType.Manual, title=title, text=text)
                        notified = True
                    except Exception as e:
                        # 通知失败不能拖垮对账(本轮状态照常落盘,下一轮再补发)
                        logger.warn(f"批次 {name} 暂缓通知发送失败:{str(e)}")
                self._manifests[key] = {"digest": digest, "time": now, "status": "retry", "files": {},
                                        "media": None, "how": "", "unknown": [], "rounds": n,
                                        "next_retry": now + retry_in, "reason": mf.get("reason"),
                                        "kind": kind, "notified": notified, "tv": mf.get("tv", True)}
                self.__save_manifests()
                return mf
            mf["attempts"] = (int(old.get("attempts") or 0) if old and old.get("digest") == digest else 0) + 1
            if self._order_override:
                # 预览 API 的测试开关/只读预览:不落盘、不复用、不动已有清单
                pass
            else:
                self._manifests[key] = mf
                self.__save_manifests()
            return mf if mf.get("status") == "ok" else None

    @staticmethod
    def __stale_marker_manifest(mf: Optional[dict]) -> bool:
        """
        按旧版特典判定规则建立、整批没有正片的清单:没识别作品就整批按文件名特典词/特典目录跳过(The Menu 这类电影、
        Trailer.Park.Boys.S01/ 这类分季目录会被当特典)。取清单时按新规则重建;残留清理不能信它
        (重建前同一轮对账里就会把目录当残留隔离)。已有正片的清单不重建(正片搬走后只剩特典,重建会把残留当正片)
        """
        return bool(mf) and mf.get("status") == "ok" and not mf.get("media") \
            and mf.get("marker_rev") != MARKER_REV \
            and any(str((e or {}).get("note") or "").startswith(("文件名标记", "目录 "))
                    for e in (mf.get("files") or {}).values())

    def __release_retry_batches(self):
        """手动全量运行:挂起中的批次不等退避到期,下一次取清单时立即重算"""
        if self._manifest_lock is None:
            self._manifest_lock = threading.RLock()
        with self._manifest_lock:
            released = 0
            for key, entry in (self._manifests or {}).items():
                if isinstance(entry, dict) and entry.get("status") == "retry":
                    entry["next_retry"] = 0
                    # 手动运行后仍挂起的批次再通知一次(可能刚加了识别词但没生效)
                    entry["notified"] = False
                    released += 1
                    # 已排队的长间隔复查作废,之后按这次重算的结果重新排
                    timer = (self._recheck_timers or {}).pop(key, None)
                    if timer:
                        timer.cancel()
                    if self._recheck_pending:
                        self._recheck_pending.discard(key)
            if released:
                logger.info(f"手动运行:{released} 个暂缓中的批次立即重试")

    def __schedule_batch_recheck(self, batch_root: Path, mon_path: str, delay: int):
        """批次未稳定时,在稳定窗口过后自动复查该批次(同一批次只挂一个复查),不必等下一次定时对账"""
        key = str(batch_root)
        if self._recheck_pending is None:
            self._recheck_pending = set()
        due = time.time() + max(5, int(delay))
        if key in self._recheck_pending:
            prev = (self._recheck_timers or {}).get(key)
            # 已排的复查比这次要求的晚得多(挂起中的长退避,批次里又来了新文件):换成更早的,否则去重
            if prev is None or prev is threading.current_thread() or getattr(prev, "clm_due", 0) <= due + 5:
                return
        self._recheck_pending.add(key)
        busy = self._busy

        def _run():
            acquired = False
            rescheduled = False
            try:
                if busy:
                    acquired = busy.acquire(timeout=600)
                    if not acquired:
                        if (self._recheck_timers or {}).get(key) is threading.current_thread():
                            # 对账/别的复查一直占着锁:过会儿再查,不能就此没有定时器
                            self._recheck_pending.discard(key)
                            rescheduled = True
                            logger.warn(f"批次 {batch_root.name} 复查等待对账锁超时,5 分钟后再查")
                            self.__schedule_batch_recheck(batch_root, mon_path, 300)
                        return
                self._recheck_pending.discard(key)
                if not batch_root.exists() or self._dirconf.get(mon_path) is None:
                    return
                if batch_root.is_file():
                    files = [batch_root]
                else:
                    files = SystemUtils.list_files(batch_root, settings.RMT_MEDIAEXT) + \
                        SystemUtils.list_files(batch_root, settings.RMT_SUBEXT)
                if not files:
                    return
                mf = self.__get_manifest(batch_root)
                if mf and mf.get("pending"):
                    logger.info(f"批次 {batch_root.name} 复查时仍暂缓({mf.get('reason') or '文件仍在变化'}),"
                                f"{mf.get('retry_in')} 秒后再查")
                    rescheduled = True
                    self.__schedule_batch_recheck(batch_root, mon_path, int(mf.get("retry_in") or 60))
                    return
                logger.info(f"批次 {batch_root.name} 已稳定,开始整理 {len(files)} 个文件")
                for file_path in self.__order_batch(mf, batch_root, files) if not batch_root.is_file() else files:
                    self.__handle_file(event_path=str(file_path), mon_path=mon_path, from_reconcile=True)
            except Exception as e:
                logger.error(f"批次 {batch_root.name} 复查失败:{str(e)} - {traceback.format_exc()}")
            finally:
                # 运行期间有新文件又排了新的复查时,去重键归新 timer 所有,不能摘(否则会被再次覆盖成无人引用的孤儿)
                owner = (self._recheck_timers or {}).get(key)
                if not rescheduled and (owner is None or owner is threading.current_thread()):
                    self._recheck_pending.discard(key)
                    if owner is not None:
                        self._recheck_timers.pop(key, None)
                if acquired:
                    busy.release()

        timer = threading.Timer(max(5, int(delay)), _run)
        timer.daemon = True
        timer.clm_ctx = (batch_root, mon_path)
        timer.clm_due = due
        if self._recheck_timers is None:
            self._recheck_timers = {}
        prev = self._recheck_timers.get(key)
        if prev is not None and prev is not threading.current_thread():
            prev.cancel()
        self._recheck_timers[key] = timer
        timer.start()

    def __rearm_rechecks(self, contexts: list):
        """
        重新挂上复查:持久化的挂起批次(重启/重载后内存里的定时器都没了,没配定时对账时会一直搁着),
        以及同一实例重载前已排队的复查(stop_service 刚取消的)。到点后由清单状态决定整理还是继续等。
        """
        todo = {}
        for batch_root, mon_path in contexts or []:
            todo[str(batch_root)] = (mon_path, 60)
        now = time.time()
        due_count = 0
        for key, entry in list((self._manifests or {}).items()):
            if key in todo or not isinstance(entry, dict) or entry.get("status") != "retry":
                continue
            mon_path = self.__monitor_of_batch(key)
            if not mon_path:
                continue
            wait = int(float(entry.get("next_retry") or 0) - now)
            if wait < 60:
                # 已到期的错开排,避免重启后一齐抢对账锁
                wait = 60 + 15 * due_count
                due_count += 1
            todo[key] = (mon_path, wait)
        rearmed = 0
        for key, (mon_path, wait) in todo.items():
            if self._dirconf.get(mon_path) is None:
                continue
            self.__schedule_batch_recheck(Path(key), mon_path, wait)
            rearmed += 1
        if rearmed:
            logger.info(f"已重新挂上 {rearmed} 个批次的复查(挂起中/等待稳定)")

    def __monitor_of_batch(self, key: str) -> Optional[str]:
        """批次(监控目录下的第一级条目)所属的整理型监控目录;不是第一级条目或不在任何整理型监控目录下时为 None"""
        try:
            parent = Path(os.path.normpath(key)).parent
        except Exception:
            return None
        for mon_path, target in (self._dirconf or {}).items():
            if target is not None and parent == Path(os.path.normpath(mon_path)):
                return mon_path
        return None

    def __cancel_rechecks(self):
        """停用/重载插件时取消排队中的批次复查(最长 6 小时后才触发,不能让旧配置的定时器事后再整理)"""
        for timer in list((self._recheck_timers or {}).values()):
            try:
                timer.cancel()
            except Exception:
                pass
        self._recheck_timers = {}
        if self._recheck_pending:
            self._recheck_pending.clear()
        if self._handoff_timer:
            try:
                self._handoff_timer.cancel()
            except Exception:
                pass
            self._handoff_timer = None
        if self._handoffs:
            logger.warn(f"插件停止时还有 {len(self._handoffs)} 个交给 MP 整理队列的文件未结算,"
                        f"其 strm 由 Strm 助手的全量对账补上")
            self._handoffs = {}

    # region 通知型监控目录:最小 QPS 的变动探测与对账

    @staticmethod
    def __notify_key(file_path: Path) -> Optional[str]:
        try:
            st = Path(file_path).stat()
            return f"{st.st_size}:{int(st.st_mtime)}"
        except Exception:
            return None

    def __save_notified(self):
        if not self._notified_dirty:
            return
        try:
            self.save_data("notified_files", self._notified or {})
            self._notified_dirty = False
        except Exception as e:
            logger.warn(f"保存已通知集合失败:{str(e)}")

    def __notify_scan(self, mon_path: str, subpaths: Optional[List[str]] = None, refresh: bool = False,
                      source: str = ""):
        """
        扫描通知型目录(或其中若干子路径),对未通知过/已变化的文件发 strm 事件。
        refresh=True 时只对给定子路径做定向刷新:OpenList 该文件夹 1 次直连刷新 + rclone 非递归 vfs/refresh。
        全目录扫描时顺带清掉已消失文件的记录。
        """
        if self._dirconf.get(mon_path) is not None or not self._strm:
            return
        roots = [Path(p) for p in (subpaths or [mon_path])]
        if refresh:
            for r in roots:
                if r.is_dir() or not r.exists():
                    ol_path = self.__map_openlist_path(str(r))
                    if ol_path and self._openlist_url and self._openlist_token:
                        self.__openlist_refresh_walk(ol_path, budget=[6])
                if self._rc_socket and self._rc_fs:
                    self.__vfs_refresh(str(r) if r.is_dir() else str(r.parent), recursive=False)
        files: List[Path] = []
        for r in roots:
            try:
                if r.is_file():
                    files.append(r)
                elif r.is_dir():
                    files += SystemUtils.list_files(r, settings.RMT_MEDIAEXT) + \
                        SystemUtils.list_files(r, settings.RMT_SUBEXT)
            except Exception as e:
                logger.debug(f"扫描 {r} 失败:{str(e)}")
        if self._notified is None:
            self._notified = {}
        notified = self._notified.setdefault(mon_path, {})
        sent = 0
        for f in files:
            key = self.__notify_key(f)
            if key and notified.get(str(f)) == key:
                continue
            self.__handle_file(event_path=str(f), mon_path=mon_path, from_reconcile=True)
            if key and notified.get(str(f)) == key:
                sent += 1
        if not subpaths:
            for p in [p for p in notified if not Path(p).exists()]:
                notified.pop(p, None)
                self._notified_dirty = True
        self.__save_notified()
        if sent:
            logger.info(f"{source}:通知型目录 {mon_path} 新通知 {sent} 个文件给 strm 助手")

    def __notify_probe(self):
        """
        通知型目录的云端变动探测:每 notify_probe_interval 秒对顶层做 1 次 OpenList 直连列表,
        比较 名称/大小/modified(115 文件夹的 modified 随内容变化更新),只对变化的条目定向刷新并扫描。
        稳态开销:每个通知型目录每周期 1 次 115 调用;有变化时 +1 次/变化文件夹。
        """
        dirs = [m for m in (self._dirconf or {}) if self._dirconf.get(m) is None]
        if not dirs or not self._strm or not self._notify_probe_interval or self._notify_probe_interval <= 0:
            return
        now = time.time()
        if now - (self._notify_last or 0) < self._notify_probe_interval:
            return
        self._notify_last = now
        if not self._openlist_url or not self._openlist_token:
            return
        if self._notify_fp is None:
            self._notify_fp = {}
        for mon_path in dirs:
            ol_path = self.__map_openlist_path(mon_path)
            if not ol_path:
                continue
            top = self.__openlist_list(ol_path)
            if top is None:
                continue
            fp = {str(i.get("name")): (bool(i.get("is_dir")), i.get("size"), i.get("modified")) for i in top}
            old = self._notify_fp.get(mon_path)
            self._notify_fp[mon_path] = fp
            if old is None:
                logger.info(f"通知型目录 {mon_path} 探测基线:{len(fp)} 个顶层条目")
                continue
            changed = [n for n, v in fp.items() if old.get(n) != v]
            removed = [n for n in old if n not in fp]
            if removed and self._notified:
                notified = self._notified.get(mon_path) or {}
                for n in removed:
                    prefix = str(Path(mon_path) / n)
                    for p in [p for p in notified if p == prefix or p.startswith(prefix + "/")]:
                        notified.pop(p, None)
                        self._notified_dirty = True
                self.__save_notified()
            if not changed:
                continue
            logger.info(f"通知型目录 {mon_path} 云端变化:{', '.join(changed[:6])}{' ...' if len(changed) > 6 else ''},定向刷新")
            top_files = [str(Path(mon_path) / n) for n in changed if not fp[n][0]]
            sub_dirs = [str(Path(mon_path) / n) for n in changed if fp[n][0]]
            if top_files:
                if self._rc_socket and self._rc_fs:
                    self.__vfs_refresh(mon_path, recursive=False)
                self.__notify_scan(mon_path, top_files, refresh=False, source="快速探测")
            if sub_dirs:
                if self._rc_socket and self._rc_fs:
                    # 新文件夹要先让挂载的顶层列表看到它
                    self.__vfs_refresh(mon_path, recursive=False)
                self.__notify_scan(mon_path, sub_dirs, refresh=True, source="快速探测")

    # endregion

    def __save_manifests(self):
        now = time.time()
        self._manifests = {k: v for k, v in (self._manifests or {}).items()
                           if now - (v.get("time") or 0) < 7 * 86400}
        try:
            self.save_data("batch_manifest", self._manifests)
        except Exception as e:
            logger.warn(f"保存批次清单失败:{str(e)}")

    def __manifest_entry(self, mf: dict, batch_root: Path, file_path: Path) -> Optional[dict]:
        """文件在清单中的条目;字幕按同目录媒体文件名前缀匹配"""
        if not mf:
            return None
        if Path(batch_root).is_file() or str(file_path) == str(batch_root):
            rel = Path(batch_root).name
        else:
            try:
                rel = str(Path(file_path).relative_to(batch_root)).replace("\\", "/")
            except ValueError:
                return None
        files = mf.get("files") or {}
        if rel in files:
            return files[rel]
        if Path(rel).suffix.lower() in [str(e).lower() for e in settings.RMT_SUBEXT]:
            # 字幕按媒体文件名前缀匹配:先同目录最长匹配,再全批次唯一匹配(DBD 等发布把字幕放子目录)
            parent = str(Path(rel).parent).replace("\\", "/")
            stem = Path(rel).stem
            same_dir, any_dir = None, []
            for mrel, entry in files.items():
                mp = Path(mrel)
                if not stem.startswith(mp.stem):
                    continue
                if str(mp.parent).replace("\\", "/") == parent:
                    if same_dir is None or len(mp.stem) > len(same_dir[0]):
                        same_dir = (mp.stem, entry)
                else:
                    any_dir.append((mp.stem, entry))
            if same_dir:
                return same_dir[1]
            if any_dir:
                any_dir.sort(key=lambda x: -len(x[0]))
                if len(any_dir) == 1 or len(any_dir[0][0]) > len(any_dir[1][0]):
                    return any_dir[0][1]
        return None

    def __build_manifest(self, batch_root: Path, listing: dict, digest: str) -> dict:
        """候选作品投票 -> 编号体系(剧集组)解析 -> 季集结构 -> 规则映射(干净批次免 LLM)/LLM 映射 -> 确定性校验"""
        # __handle_file 在读清单之前就会丢弃的文件(回收站/隐藏、过滤关键字、整理屏蔽词)不属于批次内容:
        # 不投票、不参与映射与规整度判断,也不挡"纯特典批次"判定
        keep = set(self.__batch_rels(batch_root, [m[0] for m in listing["media"]]))
        media = [m for m in listing["media"] if m[0] in keep]
        rels = [m[0] for m in media]
        dropped = len(listing["media"]) - len(media)
        mf = {"digest": digest, "time": time.time(), "status": "failed", "files": {}, "media": None,
              "how": "", "unknown": [], "marker_rev": MARKER_REV}
        # 自定义识别词变了:按标题缓存的识别结果作废(否则改了词后还拿旧结果)
        self.__sync_words_sig()
        try:
            parsed: Dict[str, dict] = {}
            for rel, size, _ in media:
                p = Path(rel)
                cleaned = self.__clean_name(p.stem) or p.stem
                meta = MetaInfo(title=cleaned)
                # 自定义识别词直接指定了作品 id(「错误剧名 => {[tmdbid=…;type=tv]}」):剩下的"剧名"只是替换后的残渣
                # (如只剩年份 2012),不能拿去识别;改按 id 计票。清洗后的名字套不上词时,再用原始文件名试一次(MP 自己
                # 逐文件识别用的是原始文件名,按原文写的词也要认)
                ident, id_meta = self.__word_identity(meta), meta
                if not ident["source"] and p.stem != cleaned:
                    raw_meta = MetaInfo(title=p.stem)
                    raw = self.__word_identity(raw_meta)
                    if raw["source"] or (raw["season"] and not ident["season"]):
                        ident, id_meta = raw, raw_meta
                asserted_id = ident["tmdbid"]
                asserted_season = ident["season"]
                marker = None
                soft: List[dict] = []
                for part in p.parts[:-1]:
                    dkind, dsoft = self.__dir_marker(part)
                    if dkind == "hard":
                        marker, soft = f"目录 {part}", []
                        break
                    if dkind == "soft":
                        soft.append(dsoft)
                if not marker:
                    kind, fsoft = self.__marker_info(cleaned)
                    if kind == "hard":
                        marker, soft = "文件名标记", []
                    elif kind == "soft":
                        soft.append(fsoft)
                    if soft:
                        # 软标记(文件名/子目录名里的特典词可能是片名的一部分:The Menu、Trailer.Park.Boys.S01/)
                        # 定下作品后按其标题核对(__resolve_soft_markers);核对不过时仍按这个标记记为特典
                        marker = "文件名标记" if kind == "soft" else f"目录 {soft[0]['dir']}"
                season = meta.begin_season
                if season is not None and not EXPLICIT_SEASON_RE.search(cleaned) and not asserted_season:
                    # 解析器凭空给出的季号(如 "[4th - 08]" -> S01E08)不可信,按未知处理,交给下载记录/目录名/LLM;
                    # 识别词里写明的 s= 是用户声明,保留
                    season = None
                if asserted_season and ident.get("season_value") is not None:
                    season = ident["season_value"]
                parsed[rel] = {"name": "" if ident["source"] else (meta.name or ""), "year": meta.year, "season": season,
                               "episode": meta.begin_episode, "tv": meta.type == MediaType.TV,
                               "mt": ident["type"].value if ident["type"] else None,
                               "tmdbid": asserted_id, "id_source": ident["source"],
                               "season_declared": bool(asserted_season and ident.get("season_value") is not None),
                               "marker": marker, "soft": soft or None, "soft_note": marker if soft else None,
                               "id_spans": self.__word_spans(id_meta, unicodedata.normalize("NFKC", cleaned))
                               if ident["source"] and soft else None,
                               "sxe": bool(re.search(r"S\d{1,2}E\d{1,3}", cleaned, re.IGNORECASE)), "size": size,
                               "pattern": re.sub(r"\d+", "#", cleaned.lower())}
            batch_name = batch_root.stem if batch_root.is_file() else batch_root.name

            def all_extra(why: str = "") -> dict:
                # 全是特典/附加(NCOP/PV/CM、扫图/特典目录等)或会被过滤的文件:规则映射对它们一律跳过
                mf["files"] = {rel: {"c": "extra", "how": "rule", "note": v["marker"]} for rel, v in parsed.items()}
                mf["how"] = "规则"
                mf["status"] = "ok"
                logger.info(f"批次清单[规则] {batch_name}:没有可整理的正片({why}{len(parsed)} 个特典/附加"
                            + (f",{dropped} 个会被过滤" if dropped else "") + "),跳过")
                return mf

            soft_rels = [rel for rel, v in parsed.items() if v.get("soft")]
            # trial:全都带标记,但有的特典词可能是片名的一部分(The.Menu.2022、Interview.with.the.Vampire.1994):
            # 这些文件先照常参与作品识别投票,定下作品后再按其标题核对(认不出作品同样挂起,不猜)
            trial = False
            if all(v["marker"] for v in parsed.values()):
                if not soft_rels:
                    # 与属于哪部作品无关,不必识别作品(认不出也不该挂起、通知)
                    return all_extra()
                for rel in soft_rels:
                    parsed[rel]["marker"] = None
                trial = True

            plain = [v for v in parsed.values() if not v["marker"]]
            batch_tv = bool(plain) and 2 * sum(1 for v in plain if v.get("tv")) > len(plain)

            def transient_retry(reason: str, retry_in: int = 900, kind: str = "transient") -> dict:
                # 日志、退避与通知由 __get_manifest 按实际复查间隔统一处理;tv 用于通知里给出 type=tv/movie 的写法
                return {"pending": True, "retry_in": retry_in, "reason": reason, "kind": kind, "tv": batch_tv}

            def word_gate(items: List[dict]) -> Optional[dict]:
                # 识别词问题:没写 type=(TMDB 电影和剧集的 id 各自编号,不能猜)、用了非 TMDB 的 id、或 id 查不到
                untyped = sorted({v["tmdbid"] for v in items if v.get("tmdbid") and not v.get("mt")})
                other_src = sorted({str(v.get("id_source")) for v in items
                                    if v.get("id_source") and v.get("id_source") != MediaSource.TMDB.value})
                unresolved = sorted({v["tmdbid"] for v in items
                                     if v.get("tmdbid") and v.get("mt") and not v.get("asserted_tmdb")})
                if untyped:
                    return transient_retry(f"自定义识别词指定了 TMDB {untyped} 但没写 type=movie/tv"
                                           f"(TMDB 电影与剧集的 id 各自编号,不能猜),请补上", 1800, kind="asserted_unresolved")
                if other_src:
                    return transient_retry(f"自定义识别词用的是 {'/'.join(other_src)} 的 id,批次清单只认 TMDB,请改用 tmdbid",
                                           1800, kind="asserted_unresolved")
                if unresolved:
                    return transient_retry(f"自定义识别词指定的 TMDB {unresolved} 查询失败(id/type 可能写错,或 TMDB 暂时不可用)",
                                           1800, kind="asserted_unresolved")
                return None

            def independent_of(series: dict) -> bool:
                # 作品不只来自软标记文件自己的名字:识别词/直通记录、批次目录名(单文件批次的"目录名"就是它自己)、其他正片文件
                srcs = set(series.get("src") or [])
                return bool(series.get("direct") or series.get("asserted")
                            or (not batch_root.is_file() and srcs & {"目录名", "目录名(去序号)"})
                            or (not trial and srcs & {"文件名", "文件名(去序号)"}))

            candidates = self.__series_candidates(batch_name, parsed)
            gate = word_gate(plain)
            if gate:
                return gate
            first_strong = next((c for c in candidates if set(c.get("src") or []) & STRONG_SRCS), None)
            if first_strong is None:
                # 本批文件名/目录名/直通记录都认不出作品(剩下的只有零票旁听或近期下载/订阅的其他作品):
                # 不猜、不回退逐文件识别(逐文件同样认不出,只会刷失败并耗尽重试),挂起复查;
                # 识别服务瞬断会自愈,真认不出会通知人工加识别词
                why = "文件名和目录名在 TMDB 上都匹配不到作品"
                if candidates:
                    why += ",候选只有近期下载/订阅的其他作品或零票搜索结果,不作依据"
                return transient_retry(why, 1800, kind="no_evidence")
            if candidates[0] is not first_strong:
                # 提示类候选(下载记录/订阅)不能靠票数当主候选:主候选必须有本批自身证据
                candidates.remove(first_strong)
                candidates.insert(0, first_strong)
            primary = candidates[0]
            primary = self.__refit_primary(batch_name, parsed, candidates) or primary
            twin = primary.pop("ambiguous", None)
            if twin:
                return transient_retry(
                    f"同名同年作品 TMDB {twin['tmdbid']}《{twin['title']}》({twin['year']}) 比 TMDB {primary['tmdbid']}"
                    f"《{primary['title']}》({primary['year']}) 更装得下本批集号(TMDB 集数未更新或另有同名作品),分不清,不猜",
                    1800, kind="ambiguous")
            soft_state = None
            soft_hold: List[str] = []
            soft_notes: Dict[str, str] = {}

            def report_unknown(files: Dict[str, dict], title: str):
                # 待人工的文件(季集无法确定/分不清是正片还是特典):清单里记为 unknown,不整理、残留清理也不清走
                unknown = [r for r, v in files.items() if v.get("c") == "unknown"]
                mf["unknown"] = unknown
                if not unknown:
                    return
                detail = "; ".join(f"{Path(r).name}({files[r].get('note') or '?'})" for r in unknown[:20])
                logger.warn(f"批次 {batch_name} 有 {len(unknown)} 个文件无法确定,已跳过待人工处理:{detail}")
                if self._notify and not self._order_override:
                    self.post_message(mtype=NotificationType.Manual,
                                      title=f"{title} 批次有 {len(unknown)} 个文件无法确定,已跳过待人工处理",
                                      text="\n".join(f"{Path(r).name}({files[r].get('note') or '?'})"
                                                     for r in unknown[:15]))

            def resolve_soft(series: dict):
                # 返回 (核对结果, 挂起结果);核对结果见 __resolve_soft_markers
                resolved = self.__resolve_soft_markers(batch_root, batch_name, parsed, soft_rels, series,
                                                       independent_of(series))
                if resolved is None:
                    return None, transient_retry(f"取不到 TMDB {series['tmdbid']}《{series['title']}》的标题,"
                                                 f"无法核对名字里的特典词是否属于片名")
                if resolved["loose"] and resolved["folder_titled"] \
                        and not set(series.get("src") or []) & {"目录名", "目录名(去序号)"}:
                    # 批次目录名开头就是该作品的片名,目录名识别却没有落到它(识别服务瞬断/目录名年份写错):
                    # 文件名里的特典词只能靠目录名作旁证,暂缓复查,不当特典也不猜
                    return None, transient_retry(
                        f"批次目录名是《{series['title']}》的片名,但目录名没有识别出该作品,无法核对 "
                        f"{'; '.join(Path(r).name for r in resolved['loose'][:3])} 是正片还是特典", 1800, kind="no_evidence")
                return resolved, None

            if soft_rels:
                # 作品已定:软标记文件按其全部标题核对,特典词都在开头片名区里且有旁证的才按正片候选,
                # 其余是特典或待人工(__resolve_soft_markers)
                resolved, held = resolve_soft(primary)
                if held:
                    return held
                soft_state = resolved["state"]
                soft_notes = resolved["notes"]
                sole = [rel for rel, v in parsed.items() if not v["marker"]]
                if primary["type"] == MediaType.MOVIE.value and len(sole) == 1 and sole[0] in resolved["residue"]:
                    # 电影批次唯一的正片候选是片名后跟着发布用词以外内容的软标记文件(The Interview (2014) - Line-O-Rama,
                    # 纯特典盘):可能是以片名开头命名的特典,不当正片整理,也不当特典清走
                    parsed[sole[0]]["marker"] = parsed[sole[0]].get("soft_note") or "文件名标记"
                    soft_state[sole[0]] = "hold"
                    soft_notes[sole[0]] = ("片名后面跟着发布用词以外的内容,本批又只有这一个正片候选,"
                                           "可能是以片名开头命名的特典,待人工")
                    logger.info(f"批次 {batch_name}:{Path(sole[0]).name} {soft_notes[sole[0]]}")
                soft_hold = [rel for rel, ok in soft_state.items() if ok == "hold"]
                plain = [v for v in parsed.values() if not v["marker"]]
                batch_tv = bool(plain) and 2 * sum(1 for v in plain if v.get("tv")) > len(plain)
                # 刚成为正片候选(或识别词待人工)的文件也要过识别词检查(漏写 type=/非 TMDB id/id 查不到)
                gate = word_gate(plain + [parsed[rel] for rel in soft_hold])
                if gate:
                    return gate
                if not plain:
                    all_extra(f"特典词不属于《{primary['title']}》({primary['year']})的片名或待人工,")
                    for rel in soft_hold:
                        mf["files"][rel] = {"c": "unknown", "how": "rule", "note": soft_notes.get(rel)}
                    report_unknown(mf["files"], primary["title"])
                    return mf
            year_counts = Counter(v.get("year") for v in plain if v.get("year"))
            file_year = year_counts.most_common(1)[0][0] if year_counts else None
            if primary["type"] == MediaType.MOVIE.value and file_year and not primary.get("direct") \
                    and not primary.get("asserted") and not self.__year_close(primary.get("year"), file_year) \
                    and not set(primary.get("src") or []) & (STRONG_SRCS - {"目录名", "目录名(去序号)"}):
                # 电影只凭目录名定下(目录名票数盖过了文件名),年份却与文件对不上,文件名又认出了年份相符的作品:
                # 多半是同名不同年的作品(Halloween (2018)/Halloween.1978…),分不清,不猜
                alt = next((c for c in candidates if c is not primary and self.__year_close(c.get("year"), file_year)
                            and set(c.get("src") or []) & {"文件名", "文件名(去序号)"}), None)
                if alt:
                    return transient_retry(
                        f"目录名识别为《{primary['title']}》({primary['year']}),文件名年份却是 {file_year},"
                        f"文件名识别为《{alt['title']}》({alt['year']}) TMDB {alt['tmdbid']},分不清,不猜",
                        1800, kind="ambiguous")
            # 本批自带年份且与主候选一致:零票旁听作品不能推翻(LLM 换选同样适用)
            year_locked = bool(file_year) and str(primary.get("year")) == str(file_year)
            if primary.get("asserted") and plain:
                # 识别词指定的作品类型与文件对不上(剧集 id 配无集号的文件,或电影 id 配带集号的文件):多半写错了 type。
                # 剧集词里写明了季号(如特别篇 s=0)的无集号文件是用户有意为之,不算矛盾(映射时 LLM 不得改季)
                has_eps = sum(1 for v in plain if v.get("episode") is not None)
                undeclared = [v for v in plain if v.get("episode") is None and not v.get("season_declared")]
                if primary["type"] == MediaType.TV.value and has_eps == 0 and undeclared:
                    return transient_retry(
                        f"自定义识别词指定的 TMDB {primary['tmdbid']}《{primary['title']}》是剧集,但本批文件都不带集号:"
                        f"若这是电影,请把识别词改成 type=movie;若是该剧的特别篇/OVA,请给这些文件单独写识别词并写明季集"
                        f"(如「文件里的标题 => {{[tmdbid={primary['tmdbid']};type=tv;s=0;e=集号]}}」)",
                        1800, kind="asserted_unresolved")
                if primary["type"] == MediaType.MOVIE.value and 2 * has_eps > len(plain):
                    return transient_retry(
                        f"自定义识别词指定的 TMDB {primary['tmdbid']}《{primary['title']}》是电影,但本批文件大多带集号:"
                        f"若这是剧集,请把识别词改成 type=tv", 1800, kind="asserted_unresolved")

            def build_structure(series: dict):
                if series["type"] != MediaType.TV.value:
                    return {"group_id": None, "source": "电影", "auto": False}, {}
                _order = self.__resolve_order(series, parsed, batch_name)
                return _order, self.__tmdb_structure(series, _order)

            def attach_binding(series: dict):
                # 绑定必须在解析编号体系之前挂上:__resolve_order 会读 binding 里的剧集组
                if series.get("direct"):
                    # 出处已确定:只用直通记录本身,不再按标签匹配其他记录(防绑到同作品另一季的记录)
                    b = self.__binding_from_direct(series)
                else:
                    b = self.__history_binding(batch_name, parsed, series)
                if b:
                    series["binding"] = b
                    logger.info(f"批次 {batch_name}:绑定下载记录 S{b['season']:02d} "
                                f"{b.get('episodes_text') or ''} <= {b['torrent_name'][:80]}")

            attach_binding(primary)
            order, structure = build_structure(primary)
            if primary["type"] == MediaType.TV.value and not structure.get("seasons"):
                return transient_retry(f"TMDB {primary['tmdbid']}《{primary['title']}》季集结构为空(获取失败或未收录)")
            files = self.__rule_mapping(batch_name, parsed, primary, structure)
            if files is None and primary.get("direct") and primary.pop("bind_unresolved", False):
                # 出处确定但记录判定的季集在结构里还找不到(TMDB 未更新):等它更新,不交 LLM 也不逐文件
                return transient_retry(f"下载记录判定的季集在 TMDB {primary['tmdbid']} 结构中尚无落点(可能未更新)", 1800,
                                       kind="bind_unresolved")
            how = "规则"
            if files is None:
                if not self._llm_classify or not (getattr(settings, "LLM_API_KEY", None)
                                                  and getattr(settings, "LLM_MODEL", None)):
                    logger.warn(f"批次 {batch_name}:命名不规整且未启用 LLM 批次清单(或系统未配置 LLM),回退逐文件识别")
                    mf["status"] = "no_media"
                    return mf
                result = self.__llm_mapping(batch_name, rels, parsed, primary, candidates, structure)
                if result is None:
                    return mf
                try:
                    alt_id = int(result.get("series_tmdbid")) if result.get("series_tmdbid") is not None else None
                except (TypeError, ValueError):
                    alt_id = None
                retry = None
                switch_ignored = False
                if alt_id and alt_id != primary["tmdbid"]:
                    alt = next((c for c in candidates if c["tmdbid"] == alt_id), None)
                    veto = None
                    if primary.get("direct") or primary.get("asserted"):
                        veto = f"批次出处已由{'下载记录直通' if primary.get('direct') else '自定义识别词'}确定"
                    elif alt is None:
                        veto = "该作品不在候选内"
                    elif self.__hint_only(alt):
                        veto = "该作品只来自近期下载/订阅提示"
                    elif not self.__has_strong(alt) and file_year and str(alt.get("year")) != str(file_year):
                        veto = f"本批年份是 {file_year},零票旁听的 {alt.get('year')} 年作品不采信"
                    elif year_locked and not self.__has_strong(alt):
                        veto = f"本批年份 {file_year} 与当前作品一致,零票旁听的作品不能推翻"
                    elif batch_tv and alt.get("type") == MediaType.MOVIE.value:
                        veto = "本批文件带集号,不换成电影"
                    if veto:
                        # 不换选;LLM 的季集是按那部作品编的,一并不采信。整批挂起复查(TMDB 更新/加识别词后能自愈),
                        # 不存成"全部待人工"的定案清单(那样加了识别词、TMDB 更新都不会再重算)
                        alt_name = f"《{alt['title']}》({alt['year']})" if alt else ""
                        return transient_retry(f"LLM 判定属于 TMDB {alt_id}{alt_name},但{veto};"
                                               f"当前作品《{primary['title']}》({primary['year']})的规则映射也落不下,不猜",
                                               1800, kind="ambiguous")
                    else:
                        logger.info(f"批次 {batch_name}:LLM 判定属于备选作品 {alt['title']} ({alt['year']}) TMDB {alt_id}")
                        primary = alt
                        if soft_state is not None:
                            # 软标记是按原作品的标题核对的:换了作品要按新作品的标题重核,结论变了就不猜
                            again, held = resolve_soft(primary)
                            if held:
                                return held
                            if again["state"] != soft_state:
                                return transient_retry(
                                    f"LLM 判定属于 TMDB {alt_id}《{alt['title']}》({alt['year']}),但按它的标题核对文件名里的"
                                    f"特典词与原作品结论不同(哪些是正片会变),不猜", 1800, kind="ambiguous")
                        attach_binding(primary)
                        order, structure = build_structure(primary)
                        if primary["type"] == MediaType.TV.value and not structure.get("seasons"):
                            return transient_retry(f"备选作品 TMDB {primary['tmdbid']} 季集结构为空(获取失败或未收录)")
                        # 换选后先在新作品的结构下重跑确定性规则映射,能唯一落点就不依赖 LLM 给的季集
                        retry = self.__rule_mapping(batch_name, parsed, primary, structure)
                if retry is not None:
                    files, how = retry, "LLM换选+规则"
                else:
                    files = self.__validate_mapping(result.get("items") or [], rels, parsed, primary, structure)
                    how = "LLM"
                    if switch_ignored:
                        # LLM 是按"另一部作品"的编号在给 s/e,对本作品全部不可信(多落点/特典场景换算校验拦不住)
                        for _rel, _entry in list(files.items()):
                            if _entry.get("c") in ("main", "special"):
                                files[_rel] = {"c": "unknown", "how": "llm",
                                               "note": "LLM 判定属其他作品,其季集不采信"}
            for _rel, _v in parsed.items():
                # 文件自己的识别词指定了别的作品:不能按本批作品整理(一批里混了两部作品/直通记录与识别词冲突)
                if _v.get("asserted_tmdb") and _v["asserted_tmdb"] != primary["tmdbid"] and not _v["marker"]:
                    files[_rel] = {"c": "unknown", "how": "rule",
                                   "note": f"识别词指定 TMDB {_v['asserted_tmdb']},与本批作品 TMDB {primary['tmdbid']} 不符"}
            for _rel in soft_hold:
                # 分不清是正片还是特典的软标记文件(识别词对不上/年份不符/没有旁证/批次目录名带特典标签):不整理,
                # 也不当特典清走,待人工
                files[_rel] = {"c": "unknown", "how": "rule", "note": soft_notes.get(_rel) or "待人工"}
            mf["files"] = files
            mf["media"] = {k: primary.get(k) for k in ("tmdbid", "title", "year", "type")}
            mf["media"]["episode_group"] = order.get("group_id")
            mf["media"]["order_source"] = order.get("source")
            mf["sync_emby"] = bool(order.get("group_id") and self._emby_sync_group
                                   and not str(order.get("source") or "").startswith("Emby"))
            mf["how"] = how
            mf["status"] = "ok"
            counts = Counter(v.get("c") for v in files.values())
            logger.info(f"批次清单[{how}] {batch_name} -> {primary['title']} ({primary['year']}) "
                        f"TMDB {primary['tmdbid']},编号体系:{self.__order_label(order)}:"
                        + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
            mapped = [f"S{v['s']:02d}E{v['e']:02d}<={Path(r).name}" for r, v in files.items()
                      if v.get("c") in ("main", "special") and v.get("s") is not None]
            if mapped:
                logger.info("季集映射:" + " | ".join(mapped[:60]) + (" ..." if len(mapped) > 60 else ""))
            skipped = [f"{Path(r).name}({v.get('note') or v.get('c')})" for r, v in files.items()
                       if v.get("c") in ("extra", "other")]
            if skipped:
                logger.info("跳过(特典/附加/杂项):" + "; ".join(skipped[:40]) + (" ..." if len(skipped) > 40 else ""))
            report_unknown(files, primary["title"])
            return mf
        except Exception as e:
            logger.error(f"建立批次清单失败 {batch_root}:{str(e)} - {traceback.format_exc()}")
            return mf

    @staticmethod
    def __strip_sequence(name: str) -> Tuple[str, Optional[int]]:
        """
        去掉剧名末尾的部数/序号(Ⅰ/Ⅱ、中文后的 1/2/II、空格后的 1-9、第X部、Part N),返回 (剧名, 序号);
        没有可去的返回 ("", None);序号认不出时为 None
        """
        raw = (name or "").strip()
        m = SEQ_SUFFIX_RE.search(raw)
        if not m:
            return "", None
        base = raw[:m.start()].strip(" -_.·:：")
        if len(base) < 2:
            return "", None
        g = m.groupdict()
        num = None
        if g.get("roman"):
            num = "ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅪⅫ".index(g["roman"]) + 1
        elif g.get("cjk_digit") or g.get("digit") or g.get("part_no"):
            num = int(g.get("cjk_digit") or g.get("digit") or g.get("part_no"))
        elif g.get("cjk_roman"):
            num = ROMAN_VALUES.get(g["cjk_roman"])
        elif g.get("part_cn"):
            num = int(g["part_cn"]) if g["part_cn"].isdigit() else CN_NUMERALS.get(g["part_cn"])
        return base, num

    @staticmethod
    def __word_identity(meta) -> dict:
        """
        自定义识别词给文件指定的作品:{"source": 数据源(没有则 None), "tmdbid": TMDB id, "type": 词里声明的类型,
        "season": 词里是否写了 s=, "season_value": 该季号}。类型只认词里明写的 type=,不用解析器猜的(TMDB 电影与剧集 id 各自编号)
        """
        ident = {"source": None, "tmdbid": None, "type": None, "season": False, "season_value": None}
        words = [str(w) for w in (getattr(meta, "apply_words", None) or [])]
        # 只写季号的词(「剧名2 => 剧名 {[s=2]}」)同样是用户对季的声明
        if any(WORD_SEASON_RE.search(w) for w in words):
            ident["season"] = True
            ident["season_value"] = getattr(meta, "begin_season", None)
        src = getattr(meta, "media_source", None)
        src = str(getattr(src, "value", src) or "")
        mid = getattr(meta, "media_id", None)
        if not src or not mid:
            return ident
        if not any(WORD_ID_RE.search(w) for w in words):
            # id 来自文件/目录名自带的 Emby/Plex 式标签,不是识别词:没有声明类型,按标题识别(MP 已从剧名里剥掉标签)
            return ident
        ident["source"] = src
        if src == MediaSource.TMDB.value:
            try:
                ident["tmdbid"] = int(mid)
            except (TypeError, ValueError):
                ident["tmdbid"] = None
        for w in words:
            m = WORD_TYPE_RE.search(w)
            if m:
                ident["type"] = MediaType.MOVIE if m.group(1).lower() in ("movie", "movies", "电影") else MediaType.TV
                break
        return ident

    @staticmethod
    def __has_strong(c: dict) -> bool:
        """候选有来自本批自身的证据(文件名/目录名/直通记录/识别词)"""
        return bool(set(c.get("src") or []) & STRONG_SRCS)

    def __batch_rels(self, batch_root: Path, rels: List[str]) -> List[str]:
        """批次内会被整理的文件(去掉 __handle_file 读清单前就丢弃的:回收站/隐藏、过滤关键字、整理屏蔽词)"""
        patterns = self.__exclude_patterns()
        words = [w for w in (self.systemconfig.get(SystemConfigKey.TransferExcludeWords) or []) if w]
        kept = []
        for rel in rels:
            full = str(batch_root) if batch_root.is_file() else f"{str(batch_root).rstrip('/')}/{rel}"
            if any(x in full for x in ("/@Recycle/", "/#recycle/", "/.", "/@eaDir")) \
                    or self.__match_any(patterns, full):
                continue
            hit = False
            for w in words:
                try:
                    if re.search(r"%s" % w, full, re.IGNORECASE):
                        hit = True
                        break
                except re.error:
                    continue
            if not hit:
                kept.append(rel)
        return kept

    def __recognize_id(self, tmdbid: int, mtype: Optional[MediaType] = None):
        """按 TMDB id 识别(自定义识别词指定的 id),进程内缓存;失败不缓存"""
        if self._recog_cache is None:
            self._recog_cache = {}
        ckey = f"tmdb:{tmdbid}\x00{mtype.value if mtype else ''}"
        if ckey in self._recog_cache:
            return self._recog_cache[ckey]
        mediainfo = None
        try:
            try:
                mediainfo = self.mediaChain.recognize_media(mtype=mtype, media_source=MediaSource.TMDB,
                                                            media_id=str(tmdbid))
            except TypeError:
                mediainfo = self.mediaChain.recognize_media(mtype=mtype, tmdbid=int(tmdbid))
        except Exception as e:
            logger.debug(f"按 TMDB id {tmdbid} 识别失败:{str(e)}")
        if mediainfo:
            self._recog_cache[ckey] = mediainfo
        return mediainfo

    @staticmethod
    def __hint_only(c: dict) -> bool:
        """候选只来自近期下载记录/订阅提示(与本批文件名无关,也不是出处确定的直通记录)"""
        return not c.get("direct") and not (set(c.get("src") or []) - HINT_SRCS)

    def __recognize_title(self, title: str, mtype: Optional[MediaType] = None):
        """按标题原生识别(不触发辅助识别),进程内缓存;mtype 限定类型(带集号的文件按剧集识别,不认成同名电影)"""
        if self._recog_cache is None:
            self._recog_cache = {}
        ckey = f"{title}\x00{mtype.value if mtype else ''}"
        if ckey in self._recog_cache:
            return self._recog_cache[ckey] or None
        mediainfo = None
        try:
            meta = MetaInfo(title=title)
            if meta.name or getattr(meta, "media_id", None):
                mediainfo = self.mediaChain.recognize_media(meta=meta, mtype=mtype)
        except Exception as e:
            logger.debug(f"识别 {title} 失败:{str(e)}")
        if mediainfo:
            self._recog_cache[ckey] = mediainfo
        # 识别失败不缓存:失败可能是识别服务瞬断,缓存会把中断固化到插件重载为止
        return mediainfo

    @staticmethod
    def __binding_from_direct(series: dict) -> Optional[dict]:
        """把候选上的直通下载记录转成绑定(需要有明确单一季号;电影/无季记录只用于作品选择)"""
        d = series.get("direct")
        if not d or d.get("season") is None:
            return None
        eps = sorted(d.get("episodes") or [])
        return {"season": int(d["season"]), "episodes": set(eps), "group": d.get("group"),
                "torrent_name": d.get("torrent_name") or "",
                "episodes_text": (f"E{eps[0]:02d}" if len(eps) == 1 else f"E{eps[0]:02d}-E{eps[-1]:02d}") if eps else "整季"}

    def __direct_binding(self, batch_name: str) -> Optional[dict]:
        """
        批次名(单文件为去扩展名的文件名)与近 14 天下载记录的种子名归一化后完全一致时,
        文件的出处就是该次下载:作品/季/集/剧集组直接采用 MP 下载时已判定并通知过用户的结果。
        115 离线下载的单文件名就是种子名;文件夹种子的内部目录名可能与站点标题不同,不匹配则自然回落到投票。
        同名种子对应多个作品时不直通。
        """
        def norm(s):
            s = str(s or "").translate(FULLWIDTH_TABLE).lower()
            return re.sub(r"\s+", " ", s).strip()

        key = norm(batch_name)
        if len(key) < 10:
            return None
        try:
            cutoff = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime("%Y-%m-%d")
            hits = [his for his in (self.downloadhis.list_by_page(1, 200) or [])
                    if str(getattr(his, "media_source", "") or "") == "themoviedb"
                    and getattr(his, "media_id", None)
                    and str(getattr(his, "date", "") or "") >= cutoff
                    and norm(getattr(his, "torrent_name", "")) == key]
            if not hits:
                return None
            if len({str(h.media_id) for h in hits}) != 1:
                logger.warn(f"批次 {batch_name}:多条同名下载记录属于不同作品,不直通绑定")
                return None
            his = hits[0]
            try:
                tmdbid = int(his.media_id)
            except (TypeError, ValueError):
                return None
            if not tmdbid:
                return None
            seasons = re.findall(r"S(\d{1,2})", str(his.seasons or ""), re.IGNORECASE)
            eps = set()
            for m in EP_RANGE_RE.finditer(str(his.episodes or "")):
                a = int(m.group(1))
                b = int(m.group(2)) if m.group(2) else a
                eps.update(range(min(a, b), max(a, b) + 1))
            return {"tmdbid": tmdbid, "title": his.title, "year": his.year, "type": str(his.type or ""),
                    "season": int(seasons[0]) if len(set(seasons)) == 1 else None,
                    "episodes": eps, "group": getattr(his, "episode_group", None) or None,
                    "torrent_name": str(his.torrent_name or "")}
        except Exception as e:
            logger.debug(f"下载记录直通匹配失败:{str(e)}")
            return None

    def __history_binding(self, batch_name: str, parsed: Dict[str, dict], primary: dict) -> Optional[dict]:
        """
        把批次(或单文件)对应到 MP 的下载记录:同一作品、14 天内、种子名含有批次的发布组/发布标记,
        且记录的集号范围覆盖批次文件的集号(无集号的整季记录只认单季)。命中且季号一致时返回
        {"season", "episodes", "episodes_text", "group", "torrent_name"};否则 None。
        """
        try:
            tmdbid = str(primary.get("tmdbid") or "")
            if not tmdbid:
                return None
            tags = [x.strip() for x in re.findall(r"[\[【]([^\]】]{2,40})[\]】]", batch_name)]
            tags = [x for x in tags if not re.fullmatch(r"[\d\s\-~_.xXpPkK×]+|(?:WEB|BD|TV)[\w\s\-]*", x, re.IGNORECASE)]
            # 剧名本身(方括号里的 Re Zero kara ... / 中文名)不是发布标记,会撞到其他发布组的记录,剔除
            def _norm(s):
                return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", str(s or "").lower())
            names = {_norm(primary.get("title"))} | {_norm(v.get("name")) for v in parsed.values()}
            names.discard("")
            tags = [x for x in tags if _norm(x) and not any(_norm(x) == n or _norm(x) in n or n in _norm(x) for n in names)]
            # 联合发布 "[hyakuhuyu&LoliHouse]" 与种子名 "[百冬练习组&LoliHouse]" 只有一半相同:按 & / 、 , 拆成子标记一起匹配
            tags = list(dict.fromkeys(tags + [p.strip() for x in tags for p in re.split(r"[&/、,，]", x) if len(p.strip()) >= 3]))
            if not tags:
                head = re.split(r"[\s\-_\[]", batch_name.strip(), 1)[0]
                tags = [head] if len(head) >= 3 else []
            if not tags:
                return None
            file_eps = {int(v["episode"]) for v in parsed.values() if not v["marker"] and v.get("episode") is not None}
            cutoff = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime("%Y-%m-%d")
            hits = []
            for his in (self.downloadhis.list_by_page(1, 100) or []):
                if str(getattr(his, "media_source", "") or "") != "themoviedb" or str(his.media_id or "") != tmdbid:
                    continue
                if str(getattr(his, "date", "") or "") < cutoff:
                    continue
                tname = str(his.torrent_name or "")
                tnorm = tname.lower().replace("【", "[").replace("】", "]")
                if not any(tag.lower() in tnorm for tag in tags):
                    continue
                seasons = re.findall(r"S(\d{1,2})", str(his.seasons or ""), re.IGNORECASE)
                if len(set(seasons)) != 1:
                    continue
                season = int(seasons[0])
                eps = set()
                for m in EP_RANGE_RE.finditer(str(his.episodes or "")):
                    a = int(m.group(1))
                    b = int(m.group(2)) if m.group(2) else a
                    eps.update(range(min(a, b), max(a, b) + 1))
                if eps:
                    if not file_eps or not file_eps <= eps:
                        continue
                elif len(parsed) > 1 and len(file_eps) > 1:
                    pass  # 整季记录允许覆盖整批文件
                hits.append({"season": season, "episodes": eps, "group": getattr(his, "episode_group", None),
                             "torrent_name": tname})
            if not hits:
                return None
            if len({h["season"] for h in hits}) != 1:
                logger.warn(f"批次 {batch_name}:多条下载记录季号不一致,不绑定")
                return None
            best = max(hits, key=lambda h: len(h["episodes"]))
            eps = sorted(best["episodes"])
            best["episodes_text"] = (f"E{eps[0]:02d}" if len(eps) == 1 else f"E{eps[0]:02d}-E{eps[-1]:02d}") if eps else "整季"
            return best
        except Exception as e:
            logger.debug(f"下载记录绑定失败:{str(e)}")
            return None

    def __series_candidates(self, batch_name: str, parsed: Dict[str, dict]) -> List[dict]:
        """候选作品:清洗后文件名识别投票(权重=文件数) + 发布目录名识别(权重 3) + 近 7 天下载记录(权重 1)"""
        votes: Dict[str, dict] = {}

        def add(tmdbid, title, year, mtype, weight, src, hint=None, groups=None, episode_group=None):
            try:
                tmdbid = int(tmdbid)
            except (TypeError, ValueError):
                return
            if not tmdbid:
                return
            c = votes.setdefault(str(tmdbid), {"tmdbid": tmdbid, "title": title, "year": year,
                                               "type": mtype, "votes": 0, "src": []})
            c["votes"] += weight
            if src not in c["src"]:
                c["src"].append(src)
            if hint and not c.get("hint"):
                c["hint"] = hint
            if groups and not c.get("groups"):
                c["groups"] = [dict(g) for g in groups if isinstance(g, dict)]
            if episode_group and not c.get("episode_group"):
                c["episode_group"] = str(episode_group)

        def mi_groups(mi):
            return getattr(mi, "episode_groups", None) or (getattr(mi, "tmdb_info", None) or {}).get(
                "episode_groups", {}).get("results")

        direct = self.__direct_binding(batch_name)
        if direct:
            # 用记录的标题+年份识别一次,给直通候选补上 TMDB 剧集组列表(合并季选组需要)
            try:
                d_type = MediaType(direct.get("type")) if direct.get("type") else None
            except ValueError:
                d_type = None
            mi = self.__recognize_title(f"{direct['title']} {direct['year']}"
                                        if direct.get("year") else str(direct.get("title") or ""),
                                        d_type if d_type in (MediaType.TV, MediaType.MOVIE) else None)
            d_groups = mi_groups(mi) if mi and getattr(mi, "tmdb_id", None) == direct["tmdbid"] else None
            add(direct["tmdbid"], direct["title"], direct["year"], direct["type"], 100, "下载记录直通",
                hint=f"下载记录直通 {direct['torrent_name']}", groups=d_groups, episode_group=direct.get("group"))
            dc = votes.get(str(direct["tmdbid"]))
            if dc:
                dc["direct"] = direct
        # 自定义识别词直接指定了 TMDB id(且写明了 type):用户的明确声明,按文件数计票,和直通记录一样不参与换选。
        # 每个文件记下它被指定的作品(asserted_tmdb),映射后不属于本批作品的文件一律不整理
        id_votes = Counter((v["tmdbid"], v["mt"]) for v in parsed.values()
                           if v.get("tmdbid") and v.get("mt") and not v["marker"])
        # 还带着软标记的文件(不投票)的识别词也要解析:之后按正片候选时,指定的必须是本批作品
        soft_pairs = sorted({(v["tmdbid"], v["mt"]) for v in parsed.values()
                             if v.get("tmdbid") and v.get("mt") and v.get("soft")} - set(id_votes))
        for tid, mt in [p for p, _ in id_votes.most_common()] + soft_pairs:
            imi = self.__recognize_id(tid, MediaType(mt))
            if not imi or not getattr(imi, "tmdb_id", None):
                continue
            for v in parsed.values():
                if v.get("tmdbid") == tid and v.get("mt") == mt:
                    v["asserted_tmdb"] = imi.tmdb_id
            cnt = id_votes.get((tid, mt), 0)
            if not cnt:
                continue
            add(imi.tmdb_id, imi.title, imi.year, imi.type.value, cnt, "识别词", groups=mi_groups(imi))
            if votes.get(str(imi.tmdb_id)):
                votes[str(imi.tmdb_id)]["asserted"] = True
        names = Counter((v["name"], v.get("year")) for v in parsed.values() if v["name"] and not v["marker"])
        years = Counter(v.get("year") for v in parsed.values() if v.get("year") and not v["marker"])
        file_year = years.most_common(1)[0][0] if years else None
        # 文件多数带集号(MetaInfo 判为剧集)时按剧集识别:单拿"剧名 年份"去识别会丢掉剧集上下文,可能认成同名电影
        tv_names = Counter((v["name"], v.get("year")) for v in parsed.values()
                           if v["name"] and not v["marker"] and v.get("tv"))
        plain = [v for v in parsed.values() if not v["marker"]]
        batch_type = MediaType.TV if plain and 2 * sum(1 for v in plain if v.get("tv")) > len(plain) else None

        def recognize_stripped(name: str, year, weight: int, src: str, mtype: Optional[MediaType]):
            """
            发布方给剧名加了部数/序号(闯关东Ⅰ、闯关东1、第一部)而 TMDB 标题没有:去掉末尾序号再识别。
            只有"第一部"且年份一致才计票:第二部及以后去掉序号会落到前作(或同年的另一部分),无年份时同名
            作品无法区分,都只作零票旁听,不能单独定案。
            """
            base, num = self.__strip_sequence(name)
            if not base or num != 1:
                # 第二部及以后(或序号认不出)去掉序号会落到前作/另一部分:连零票旁听都不能加,否则可能被换选进前作
                return
            smi = self.__recognize_title(f"{base} {year}" if year else base, mtype)
            if not smi or not getattr(smi, "tmdb_id", None):
                return
            if num == 1 and year and str(smi.year) == str(year):
                add(smi.tmdb_id, smi.title, smi.year, smi.type.value, weight, f"{src}(去序号)", groups=mi_groups(smi))
            else:
                add(smi.tmdb_id, smi.title, smi.year, smi.type.value, 0, "去序号识别", groups=mi_groups(smi))

        for (name, year), cnt in names.most_common(6):
            n_type = MediaType.TV if 2 * tv_names.get((name, year), 0) > cnt else None
            mi = self.__recognize_title(f"{name} {year}" if year else name, n_type)
            if mi and getattr(mi, "tmdb_id", None):
                add(mi.tmdb_id, mi.title, mi.year, mi.type.value, cnt, "文件名", groups=mi_groups(mi))
            else:
                recognize_stripped(name, year, cnt, "文件名", n_type)
        batch_clean = self.__clean_name(batch_name) or batch_name
        try:
            dident = self.__word_identity(MetaInfo(title=batch_clean))
        except Exception:
            dident = {"source": None}
        mi = None
        dtitle = ""
        if dident.get("source"):
            # 目录名套上了指定 id 的识别词:只认 TMDB 且写明 type 的,按声明的类型查(不用本批文件猜的类型)
            if dident.get("tmdbid") and dident.get("type"):
                mi = self.__recognize_id(dident["tmdbid"], dident["type"])
        else:
            # 目录名自带的 id 标签不是用户声明(也没有类型):剥掉后按标题识别,不让本批文件猜的类型决定去哪个库查 id。
            # 不带年份的目录名(The Interview/)按本批文件的年份识别:裸标题会落到最新的同名作品上,
            # 年份对不上的同名作品只零票旁听(不能靠目录名的 3 票盖过文件名)
            dtitle = re.sub(r"\s{2,}", " ", ID_TAG_RE.sub(" ", batch_clean)).strip()
            if dtitle:
                mi, off_year = self.__recognize_dir_title(dtitle, file_year, batch_type)
                if off_year:
                    add(off_year.tmdb_id, off_year.title, off_year.year, off_year.type.value, 0, "目录名(年份不符)",
                        groups=mi_groups(off_year))
        if mi and getattr(mi, "tmdb_id", None):
            add(mi.tmdb_id, mi.title, mi.year, mi.type.value, 3, "目录名", groups=mi_groups(mi))
        elif dtitle:
            # 目录名套上了识别词时,剩下的名字只是残渣,不拿去去序号识别
            try:
                dmeta = MetaInfo(title=dtitle)
                recognize_stripped(dmeta.name or "", dmeta.year or file_year, 3, "目录名", batch_type)
            except Exception as e:
                logger.debug(f"目录名去序号识别失败:{str(e)}")
        # TMDB 同名搜索的其余结果零票旁听:供结构拟合度换选与 LLM 换选(同名不同年/动画与真人版并存的场景)
        try:
            if names:
                (top_name, top_year), _ = names.most_common(1)[0]
                s_meta = MetaInfo(title=f"{top_name} {top_year}" if top_year else top_name)
                for smi in (self.mediaChain.search_medias(meta=s_meta) or [])[:6]:
                    if getattr(smi, "tmdb_id", None):
                        s_type = smi.type.value if hasattr(smi.type, "value") else str(smi.type)
                        add(smi.tmdb_id, smi.title, smi.year, s_type, 0, "同名搜索", groups=mi_groups(smi))
        except Exception as e:
            logger.debug(f"同名搜索失败:{str(e)}")
        try:
            from app.db.subscribe_oper import SubscribeOper
            for sub in (SubscribeOper().list() or []):
                if str(getattr(sub, "media_source", "") or "") == "themoviedb" and getattr(sub, "media_id", None) \
                        and getattr(sub, "episode_group", None):
                    add(sub.media_id, sub.name, sub.year, sub.type, 0, "订阅", episode_group=sub.episode_group)
        except Exception as e:
            logger.debug(f"读取订阅失败:{str(e)}")
        try:
            # 下载记录只作低权重提示:同一作品多条记录(逐集下载)只计 1 票,不能盖过文件名/目录名识别
            cutoff = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
            seen_his = set()
            for his in (self.downloadhis.list_by_page(1, 50) or []):
                if str(getattr(his, "media_source", "") or "") != "themoviedb" or not getattr(his, "media_id", None):
                    continue
                if str(getattr(his, "date", "") or "") < cutoff or str(his.media_id) in seen_his:
                    continue
                seen_his.add(str(his.media_id))
                hint = f"下载记录 {his.torrent_name or ''} {his.seasons or ''}".strip()
                add(his.media_id, his.title, his.year, his.type, 1, "下载记录", hint,
                    episode_group=getattr(his, "episode_group", None))
        except Exception as e:
            logger.debug(f"读取下载记录失败:{str(e)}")
        result = sorted(votes.values(), key=lambda c: -c["votes"])
        acand = next((c for c in result if c.get("asserted")), None)
        if acand is not None and result[0] is not acand:
            # 用户识别词指定的作品不参与票数竞争(直通记录仍排在它前面)
            result.remove(acand)
            result.insert(0, acand)
        dcand = next((c for c in result if c.get("direct")), None)
        if dcand is not None and result[0] is not dcand:
            # 出处确定的候选不参与票数竞争:再多的同名误识别票也不能盖过下载记录本身
            result.remove(dcand)
            result.insert(0, dcand)
        if result:
            logger.info("批次候选作品:" + "; ".join(
                f"{c['title']} ({c['year']}) TMDB {c['tmdbid']} 票数 {c['votes']} [{'/'.join(c['src'])}]"
                for c in result[:4]))
        return result

    # ---- 结构拟合度:同名候选消歧 ----

    def __structure_fit(self, series: dict, parsed: Dict[str, dict]) -> float:
        """
        文件名解析出的集号在该作品默认季集结构下有落点的比例(0~1)。电影/无集号文件不计入;
        显式季号文件也不计入:合并季默认结构下按季落点依赖候选的 ref(groups),不同候选来源的
        groups 可得性不同,拟合度会失真(DBD 2nd Season 案例),此类消歧交给年份投票与 LLM。
        """
        if series.get("type") != MediaType.TV.value:
            return 0.0
        cands = [v for v in parsed.values()
                 if not v["marker"] and v.get("episode") is not None and v.get("season") is None]
        if not cands:
            return 0.0
        structure = self.__tmdb_structure(series, {"group_id": None})
        if not structure.get("seasons"):
            return 0.0
        hit = 0
        for v in cands:
            targets = self.__plausible_targets(structure, v.get("season"), int(v["episode"]))
            if targets:
                hit += 1
        return hit / len(cands)

    def __refit_primary(self, batch_name: str, parsed: Dict[str, dict],
                        candidates: List[dict]) -> Optional[dict]:
        """
        主候选的结构装不下大部分文件时,按拟合度在所有候选(含零票旁听)里重选:
        ≥3 个带集号文件时拟合度需比主候选高出 0.2 以上才换;1-2 个文件时证据少,仅当主候选完全装不下、
        备选完全装得下且有佐证(年份一致或下载记录/订阅背书)才换。有直通绑定的批次出处明确,不换选。
        """
        primary = candidates[0]
        if primary.get("direct") or primary.get("asserted"):
            return None
        if primary.get("type") != MediaType.TV.value or len(candidates) < 2:
            return None
        eps = [v for v in parsed.values()
               if not v["marker"] and v.get("episode") is not None and v.get("season") is None]
        if not eps:
            # 全部文件带显式季号(或无集号):拟合度信号不适用,不做换选
            return None
        small = len(eps) < 3
        if not self.__tmdb_structure(primary, {"group_id": None}).get("seasons"):
            # 主候选结构为空(获取失败/未收录):无法区分"装不下"与"拉不到",不做换选
            return None
        base_fit = self.__structure_fit(primary, parsed)
        if base_fit >= 0.8:
            return None
        # 年份取本批全部正片文件(不止无显式季号的):年份只写在 SxxExx 文件上时也要锁定
        years = Counter(v.get("year") for v in parsed.values() if v.get("year") and not v["marker"])
        file_year = years.most_common(1)[0][0] if years else None
        # 本批年份与主候选一致:主候选有本批自己的年份佐证,零票旁听不能只凭结构拟合度把它换掉——
        # 年份不同的不考虑;同名同年的(同年两部同名作品,或主候选的集数 TMDB 还没更新全)分不清,整批挂起
        year_locked = bool(file_year) and str(primary.get("year")) == str(file_year)
        best, best_key = None, None
        # 只在与本批名字相关的候选里换选(同名搜索旁听与其他强证据);只有下载记录/订阅提示的候选
        # 与本批无关,结构再合身也不选(长篇连载几乎装得下任何集号)
        alts = [c for c in candidates[1:] if not self.__hint_only(c)][:4]
        for cand in alts:
            if cand.get("type") != MediaType.TV.value:
                continue
            if year_locked and not self.__has_strong(cand) and str(cand.get("year")) != str(file_year):
                continue
            fit = self.__structure_fit(cand, parsed)
            key = (fit, 1 if file_year and str(cand.get("year")) == str(file_year) else 0, cand.get("votes") or 0)
            if best_key is None or key > best_key:
                best, best_key = cand, key
        if best and small:
            # 佐证必须与本批文件相关:年份一致,或备选的下载记录经"标签+集号覆盖"匹配上本批;泛泛的"最近下过"不算
            corroborated = (file_year and str(best.get("year")) == str(file_year)) or \
                bool(self.__history_binding(batch_name, parsed, best))
            if base_fit <= 0 and best_key[0] >= 1.0 and corroborated:
                if year_locked and not self.__has_strong(best):
                    primary["ambiguous"] = best
                    return None
                logger.warn(f"批次 {batch_name}:主候选 {primary['title']} ({primary['year']}) 完全装不下该批文件,"
                            f"换选有佐证的 {best['title']} ({best['year']}) TMDB {best['tmdbid']}(拟合度 100%)")
                return best
        elif best and best_key[0] >= base_fit + 0.2:
            if year_locked and not self.__has_strong(best):
                primary["ambiguous"] = best
                return None
            logger.warn(f"批次 {batch_name}:候选 {primary['title']} ({primary['year']}) 结构拟合度仅 "
                        f"{base_fit:.0%},换选 {best['title']} ({best['year']}) TMDB {best['tmdbid']}"
                        f"(拟合度 {best_key[0]:.0%})")
            return best
        if base_fit < 0.5:
            logger.warn(f"批次 {batch_name}:主候选 {primary['title']} ({primary['year']}) 结构拟合度仅 "
                        f"{base_fit:.0%},且无更优候选,映射可能大量跳过")
        return None

    # ---- 编号体系(剧集组)解析 ----

    def __group_map(self) -> Dict[str, str]:
        """插件配置的手动映射:每行 tmdbid=剧集组id"""
        result: Dict[str, str] = {}
        for line in (self._group_map or "").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = re.match(r"^(\d+)\s*[=:：]\s*([0-9a-fA-F]{24})$", line)
            if m:
                result[m.group(1)] = m.group(2)
        return result

    @staticmethod
    def __pick_season_group(groups: list, min_episodes: int = 0) -> Optional[dict]:
        """
        在 TMDB 剧集组里挑"按季"类型(原播出/制作/电视 顺序,且 ≥2 季)的最完整一个;DVD/绝对/故事线/流媒体顺序不选。
        min_episodes:组的总集数必须达到该值(默认正规季集数)——社区维护的残缺篇章分组(如凡人修仙传各种 124–176 集的分组)不选。
        """
        best, best_key = None, None
        for g in groups or []:
            if not isinstance(g, dict):
                continue
            try:
                gtype = int(g.get("type") or 0)
                gcount = int(g.get("group_count") or 0)
                ecount = int(g.get("episode_count") or 0)
            except (TypeError, ValueError):
                continue
            if gtype not in (1, 6, 7) or gcount < 2:
                continue
            if min_episodes and ecount < min_episodes:
                continue
            name = str(g.get("name") or "").strip()
            # 名字就叫 Seasons/Season 的组最常见也最规范,其次名字含 season/季,再按集数多少
            key = (2 if re.match(r"^(seasons?|按季|分季)$", name, re.IGNORECASE)
                   else 1 if re.search(r"season|季", name, re.IGNORECASE) else 0, ecount)
            if best_key is None or key > best_key:
                best, best_key = g, key
        return best

    def __save_series_groups(self):
        try:
            self.save_data("series_groups", self._series_groups or {})
        except Exception as e:
            logger.warn(f"保存剧集组映射失败:{str(e)}")

    @staticmethod
    def __order_label(order: dict) -> str:
        if order.get("group_id"):
            return f"剧集组『{order.get('name') or ''}』({order.get('group_id')},来源 {order.get('source')})"
        return f"TMDB 默认顺序({order.get('source') or '默认'})"

    def __resolve_order(self, series: dict, parsed: Dict[str, dict], batch_name: str) -> dict:
        """
        决定该作品入库使用的编号体系,优先级:Emby 已配置的剧集组(tmdbeg) > MP 下载记录/订阅里的剧集组 >
        插件手动映射 > 已记录的自动选组 > 合并季自动识别并选组(随后同步到 Emby) > TMDB 默认顺序。
        原则:CLM 的命名必须与 Emby 取元数据用的顺序一致。
        """
        tmdbid = series.get("tmdbid")
        key = str(tmdbid)
        title = series.get("title") or key
        groups = series.get("groups") or []
        override = self._order_override or {}

        def gname(gid):
            return next((str(g.get("name") or "") for g in groups if str(g.get("id")) == str(gid)), "")

        def valid(gid):
            return bool(gid) and (not groups or any(str(g.get("id")) == str(gid) for g in groups))

        if override.get("force_default"):
            return {"group_id": None, "name": "", "source": "测试强制默认", "auto": False}
        emby = None if override.get("ignore_emby") else self.__emby_series_info(tmdbid)
        if emby:
            if emby.get("tmdbeg"):
                gid = str(emby["tmdbeg"])
                if not valid(gid):
                    logger.warn(f"Emby 为《{title}》配置的剧集组 {gid} 不在 TMDB 当前剧集组列表中,仍以 Emby 为准")
                return {"group_id": gid, "name": gname(gid), "source": "Emby", "auto": False}
            display_order = str(emby.get("display_order") or "")
            if display_order and display_order.lower() not in ("aired", "default"):
                logger.warn(f"Emby 对《{title}》使用显示顺序 {display_order},插件不支持该顺序,按 TMDB 默认顺序整理")
            return {"group_id": None, "name": "", "source": "Emby默认顺序", "auto": False, "emby_exists": True}
        for src, gid in (("下载记录/订阅", (series.get("binding") or {}).get("group") or series.get("episode_group")),
                         ("配置映射", self.__group_map().get(key)),
                         ("已记录", (self._series_groups or {}).get(key, {}).get("group_id"))):
            if valid(gid):
                return {"group_id": str(gid), "name": gname(gid), "source": src, "auto": False}
        # 合并季识别:TMDB 默认只有一个正规季,且(集数很多 或 发布明确写着 ≥2 季),并存在按季类型剧集组
        default_struct = self.__tmdb_structure(series, {"group_id": None})
        regular = sorted(sn for sn in (default_struct.get("seasons") or {}) if sn > 0)
        release_season = max([int(v["season"]) for v in parsed.values() if v.get("season")] or [0])
        try:
            release_season = max(release_season, int(MetaInfo(title=batch_name).begin_season or 0))
        except Exception:
            pass
        try:
            release_season = max(release_season, int((series.get("binding") or {}).get("season") or 0))
        except (TypeError, ValueError):
            pass
        if len(regular) == 1 and release_season >= 2:
            pick = self.__pick_season_group(groups, min_episodes=self.__season_size(default_struct, regular[0]))
            if pick and self._order_override:
                # 预览(测试开关/只读):只返回选择结果,不记录、不通知
                return {"group_id": str(pick.get("id")), "name": pick.get("name"), "source": "自动(合并季)", "auto": True}
            if pick:
                gid = str(pick.get("id"))
                if self._series_groups is None:
                    self._series_groups = {}
                self._series_groups[key] = {"group_id": gid, "name": pick.get("name"), "title": title,
                                            "source": "auto", "time": time.time()}
                self.__save_series_groups()
                logger.warn(f"《{title}》在 TMDB 为合并单季({self.__season_size(default_struct, regular[0])} 集),"
                            f"自动选用按季剧集组『{pick.get('name')}』({gid}),入库后将同步设置到 Emby")
                if self._notify and key not in self._group_notified:
                    self._group_notified.add(key)
                    self.post_message(mtype=NotificationType.Manual,
                                      title=f"{title}:已自动启用剧集组『{pick.get('name')}』",
                                      text=f"TMDB 把该剧合并为单季,按季整理需要剧集组。已选用 {gid},"
                                           f"入库后会自动同步到 Emby;如需更换,在插件配置里写 {key}={gid} 形式的映射")
                return {"group_id": gid, "name": pick.get("name"), "source": "自动(合并季)", "auto": True}
            logger.warn(f"《{title}》在 TMDB 为合并单季但没有可用的按季剧集组,按默认顺序(绝对集数)整理")
        return {"group_id": None, "name": "", "source": "TMDB默认", "auto": False}

    # ---- 季集结构(目标编号体系)与换算 ----

    def __tmdb_structure(self, series: dict, order: dict) -> dict:
        """
        目标编号体系下的季集结构:
        {"seasons": {季号: {"count","name","episodes": {集号: (集名, 首播, 时长)}}}, "offsets": {季号: 绝对集数偏移},
         "order": 编号体系, "merged": 默认顺序是否为合并单季, "ref": [{"season","start","count","name"}](合并季时的按季参考边界)}
        """
        tmdbid = series.get("tmdbid")
        gid = (order or {}).get("group_id")
        cache_key = f"{tmdbid}:{gid or ''}"
        if self._tmdb_struct_cache is None:
            self._tmdb_struct_cache = {}
        cached = self._tmdb_struct_cache.get(cache_key)
        if cached and time.time() - cached[0] < 3600:
            struct = dict(cached[1])
            struct["order"] = order
            return struct
        seasons: Dict[int, dict] = {}
        try:
            if gid:
                for s in (self.tmdbchain.tmdb_group_seasons(group_id=gid) or []):
                    sn = getattr(s, "season_number", None)
                    if sn is None:
                        continue
                    eps = self.tmdbchain.tmdb_episodes(tmdbid=tmdbid, season=int(sn), episode_group=gid) or []
                    entry = {"count": int(getattr(s, "episode_count", 0) or 0),
                             "name": getattr(s, "name", "") or "", "episodes": {}}
                    for e in eps:
                        num = getattr(e, "episode_number", None)
                        if num is None:
                            continue
                        entry["episodes"][int(num)] = (getattr(e, "name", "") or "", getattr(e, "air_date", "") or "",
                                                       getattr(e, "runtime", None))
                    entry["count"] = max(entry["count"], len(entry["episodes"]))
                    seasons[int(sn)] = entry
            else:
                all_seasons = self.tmdbchain.tmdb_seasons(tmdbid=tmdbid) or []
                total = sum(int(getattr(s, "episode_count", 0) or 0) for s in all_seasons)
                for s in all_seasons:
                    sn = getattr(s, "season_number", None)
                    if sn is None:
                        continue
                    entry = {"count": int(getattr(s, "episode_count", 0) or 0),
                             "name": getattr(s, "name", "") or "", "episodes": {}}
                    if total <= 600 or int(sn) == 0:
                        for e in (self.tmdbchain.tmdb_episodes(tmdbid=tmdbid, season=int(sn)) or []):
                            num = getattr(e, "episode_number", None)
                            if num is None:
                                continue
                            entry["episodes"][int(num)] = (getattr(e, "name", "") or "",
                                                           getattr(e, "air_date", "") or "",
                                                           getattr(e, "runtime", None))
                        entry["count"] = max(entry["count"], len(entry["episodes"]))
                    seasons[int(sn)] = entry
        except Exception as e:
            logger.warn(f"获取 TMDB {tmdbid} 季集结构失败({'剧集组 ' + gid if gid else '默认顺序'}):{str(e)}")
        regular = sorted(sn for sn in seasons if sn > 0)
        offsets: Dict[int, int] = {}
        cum = 0
        for sn in regular:
            offsets[sn] = cum
            cum += max(int(seasons[sn].get("count") or 0), len(seasons[sn].get("episodes") or {}))
        merged = (not gid) and len(regular) == 1 and cum >= 40
        ref, ref_name = [], ""
        if merged:
            pick = self.__pick_season_group(series.get("groups") or [])
            if pick:
                try:
                    cum_ref = 0
                    for s in (self.tmdbchain.tmdb_group_seasons(group_id=str(pick.get("id"))) or []):
                        sn = getattr(s, "season_number", None)
                        cnt = int(getattr(s, "episode_count", 0) or 0)
                        if sn is None or int(sn) <= 0 or cnt <= 0:
                            continue
                        ref.append({"season": int(sn), "start": cum_ref + 1, "count": cnt,
                                    "name": getattr(s, "name", "") or ""})
                        cum_ref += cnt
                    ref_name = str(pick.get("name") or "")
                except Exception as e:
                    logger.debug(f"获取按季参考剧集组失败:{str(e)}")
        struct = {"seasons": seasons, "offsets": offsets, "order": order, "merged": merged,
                  "ref": ref, "ref_name": ref_name}
        if seasons:
            # 空结构不缓存:获取失败时缓存会把"拉不到"固化成"装不下"(结构中毒 1 小时)
            self._tmdb_struct_cache[cache_key] = (time.time(), dict(struct))
        return struct

    @staticmethod
    def __season_size(structure: dict, season: int) -> int:
        entry = (structure.get("seasons") or {}).get(season) or {}
        return max(int(entry.get("count") or 0), len(entry.get("episodes") or {}))

    def __abs_to_target(self, structure: dict, abs_no: int) -> Optional[Tuple[int, int]]:
        """绝对集数 -> 目标编号体系的 (季, 集)"""
        for sn in sorted(structure.get("offsets") or {}):
            off = structure["offsets"][sn]
            cnt = self.__season_size(structure, sn)
            if off < abs_no <= off + cnt:
                return sn, abs_no - off
        return None

    def __season_ep_to_target(self, structure: dict, season: int, episode: int) -> Optional[Tuple[int, int]]:
        """发布的(季, 集) -> 目标编号体系的 (季, 集):目标有该季直接用;合并季目标按参考边界换算成绝对集数"""
        if season in (structure.get("seasons") or {}) and 1 <= episode <= self.__season_size(structure, season):
            return season, episode
        for r in structure.get("ref") or []:
            if r["season"] == season and 1 <= episode <= r["count"]:
                return self.__abs_to_target(structure, r["start"] + episode - 1)
        # 发布按 TMDB 默认的"单季绝对集数"命名(如 7³ACG 的 S01E26–E50),而目标体系按剧集组分季:
        # 第一季放不下的集号只可能是绝对集数,按绝对集数落到对应季
        regular = sorted(structure.get("offsets") or {})
        if regular and season == regular[0] and episode > self.__season_size(structure, season):
            return self.__abs_to_target(structure, episode)
        return None

    def __plausible_targets(self, structure: dict, season: Optional[int], episode: Optional[int]) -> Optional[set]:
        """文件名季集在目标编号体系下的所有合理落点;None 表示文件名无集号、不设约束"""
        if episode is None:
            return None
        result = set()
        if season is not None:
            target = self.__season_ep_to_target(structure, season, episode)
            if target:
                result.add(target)
            return result
        for sn in sorted(structure.get("offsets") or {}):
            if 1 <= episode <= self.__season_size(structure, sn):
                result.add((sn, episode))
        target = self.__abs_to_target(structure, episode)
        if target:
            result.add(target)
        return result

    def __rule_mapping(self, batch_name: str, parsed: Dict[str, dict], primary: dict,
                       structure: dict) -> Optional[Dict[str, dict]]:
        """
        命名整齐、每个文件都能唯一换算到目标编号体系且无歧义的批次直接映射(免 LLM);否则返回 None。
        支持:目标按季 + 发布绝对集数;目标合并季(绝对) + 发布按季;显式 SxxExx。
        """
        files: Dict[str, dict] = {}
        cands: Dict[str, dict] = {}
        for rel, v in parsed.items():
            if v["marker"]:
                files[rel] = {"c": "extra", "how": "rule", "note": v["marker"]}
            else:
                cands[rel] = v
        if primary["type"] != MediaType.TV.value:
            if len(cands) == 1:
                files[next(iter(cands))] = {"c": "main", "how": "rule"}
                return files
            return None
        if not cands:
            return files
        if len({v["pattern"] for v in cands.values()}) > 1 or len({v["name"] for v in cands.values()}) > 1:
            return None
        try:
            bmeta = MetaInfo(title=self.__clean_name(batch_name) or batch_name)
            batch_season = bmeta.begin_season
            if batch_season is not None and not EXPLICIT_SEASON_RE.search(self.__clean_name(batch_name) or batch_name) \
                    and not self.__word_identity(bmeta)["season"]:
                batch_season = None
        except Exception:
            batch_season = None
        binding = primary.get("binding") or {}
        # 记录集号在它自己的编号空间里(默认序/订阅剧集组):与目标编号体系的组不同时集号不可直用,季号仍可参考
        bind_eps_ok = str(binding.get("group") or "") == str((structure.get("order") or {}).get("group_id") or "")
        if binding.get("season") is not None:
            # 下载记录(订阅匹配时判定的季)优先于目录名;显式季号不同时先看换算是否收敛(绝对集数写成 S01 的包)
            for v in cands.values():
                if v["season"] is not None and int(v["season"]) != int(binding["season"]):
                    t = self.__season_ep_to_target(structure, int(v["season"]), int(v["episode"])) \
                        if v.get("episode") is not None else None
                    if not t or t[0] != int(binding["season"]) or \
                            (bind_eps_ok and binding.get("episodes") and t[1] not in binding["episodes"]):
                        logger.warn(f"批次 {batch_name}:文件名季号 S{v['season']} 与下载记录 "
                                    f"S{binding['season']} 冲突,交给 LLM 判断")
                        return None
            batch_season = int(binding["season"])
        regular = sorted(structure.get("offsets") or {})
        b_eps = sorted(binding.get("episodes") or []) if bind_eps_ok else []
        bind_target = None
        if len(cands) == 1 and len(b_eps) == 1 and batch_season is not None:
            bind_target = self.__season_ep_to_target(structure, int(batch_season), b_eps[0])
            if bind_target is None:
                # 记录判定的集号在目标结构无落点(TMDB 未更新/编号差异):不猜,不静默退化为文件名换算
                logger.warn(f"批次 {batch_name}:下载记录判定 S{int(batch_season):02d}E{b_eps[0]:02d} "
                            f"在目标结构无落点,规则映射放弃")
                primary["bind_unresolved"] = True
                return None
        via_binding = set()
        mapping: Dict[str, Tuple[int, int]] = {}
        for rel, v in cands.items():
            from_binding = False
            if v["episode"] is not None:
                ep = int(v["episode"])
            elif bind_target:
                # 单文件批次文件名无集号:用下载记录的集号(出处数据,非猜测)
                ep = b_eps[0]
                from_binding = True
            else:
                return None
            if v["season"] is not None:
                target = self.__season_ep_to_target(structure, int(v["season"]), ep)
            else:
                options = self.__plausible_targets(structure, None, ep) or set()
                target = None
                if batch_season is not None:
                    # 季号证据(下载记录/目录名):优先该季直接对应,其次按季换算(合并季目标)
                    if (batch_season, ep) in options:
                        target = (batch_season, ep)
                    else:
                        target = self.__season_ep_to_target(structure, int(batch_season), ep)
                if target is None:
                    # 唯一落点兜底不得违背已知的季号证据(如绑定 S02 却唯一落到 S01 绝对位):宁可交给 LLM
                    if len(options) == 1 and (batch_season is None or next(iter(options))[0] == int(batch_season)):
                        target = next(iter(options))
                    elif batch_season is None and len(regular) == 1 and (regular[0], ep) in options:
                        target = (regular[0], ep)
            if bind_target and target and target != bind_target:
                # 文件名换算与下载记录判定不一致(订阅集数偏移识别词/TMDB 编号差异):以出处记录为准
                logger.warn(f"批次 {batch_name}:{Path(rel).name} 文件名换算落点 S{target[0]:02d}E{target[1]:02d} "
                            f"与下载记录判定 S{bind_target[0]:02d}E{bind_target[1]:02d} 不一致,以下载记录为准")
                target = bind_target
                from_binding = True
            if target is None and bind_target:
                # 文件名集号(发布组原始编号)在目标结构无落点:以下载记录判定的季集为准
                target = bind_target
                from_binding = True
            if not target:
                return None
            if from_binding:
                via_binding.add(rel)
            mapping[rel] = target
        if len(set(mapping.values())) != len(mapping):
            return None
        sizes = sorted(v["size"] for v in cands.values())
        median = sizes[len(sizes) // 2]
        if median and sizes[0] < 0.35 * median:
            return None
        for rel, (s, e) in mapping.items():
            note = None
            if rel in via_binding:
                note = f"由下载记录 S{s:02d}E{e:02d} 确定"
            elif parsed[rel]["episode"] != e or (parsed[rel]["season"] is not None and parsed[rel]["season"] != s):
                note = f"由文件名 S{parsed[rel]['season'] or '?'}E{parsed[rel]['episode']} 换算"
            files[rel] = {"c": "main", "s": int(s), "e": int(e), "how": "rule"}
            if note:
                files[rel]["note"] = note
        return files

    def __structure_text(self, structure: dict) -> str:
        order = structure.get("order") or {}
        lines = [f"目标编号体系: {self.__order_label(order)}"]
        if structure.get("merged"):
            if structure.get("ref"):
                bounds = "、".join(f"{r['name'] or 'S' + str(r['season'])}=第{r['start']}–{r['start'] + r['count'] - 1}集"
                                  for r in structure["ref"])
                lines.append(f"(默认顺序为合并单季绝对集数;按季参考『{structure.get('ref_name')}』:{bounds})")
            else:
                lines.append("(默认顺序为合并单季绝对集数)")
        offsets = structure.get("offsets") or {}
        multi = len(offsets) > 1
        for sn in sorted((structure.get("seasons") or {}).keys()):
            entry = structure["seasons"][sn]
            lines.append(f"Season {sn} ({entry.get('name') or ''}), {self.__season_size(structure, sn)} 集:")
            for num in sorted(entry.get("episodes") or {}):
                name, air, runtime = entry["episodes"][num]
                tag = ""
                if sn > 0 and multi and sn in offsets:
                    tag = f" (abs {offsets[sn] + num})"
                elif sn > 0 and structure.get("merged") and structure.get("ref"):
                    for r in structure["ref"]:
                        if r["start"] <= num < r["start"] + r["count"]:
                            tag = f" (按季 S{r['season']}E{num - r['start'] + 1:02d})"
                            break
                lines.append(f"  S{sn}E{num}{tag}: {name} ({(air or '')[:7]}{', ' + str(runtime) + 'min' if runtime else ''})")
        return "\n".join(lines) or "(无季集结构/电影)"

    def __llm_mapping(self, batch_name: str, rels: List[str], parsed: Dict[str, dict], primary: dict,
                      candidates: List[dict], structure: dict) -> Optional[dict]:
        """一批一次:编号文件列表 + 目标编号体系季集结构 -> LLM 按编号输出分类与季集(输出极小,不会截断)"""
        listing = "\n".join(f"{i + 1}. {self.__fmt_size(parsed[r]['size'])} {r}" for i, r in enumerate(rels))
        alts = [c for c in candidates if c["tmdbid"] != primary["tmdbid"] and not self.__hint_only(c)][:5]
        def alt_desc(c: dict) -> str:
            # 同名作品(动画/真人版、不同年份)只看标题分不清:附上各季集数
            desc = f"TMDB {c['tmdbid']} {c['title']} ({c['year']}) {c['type']}"
            if c.get("type") != MediaType.TV.value:
                return desc
            try:
                st = self.__tmdb_structure(c, {"group_id": None})
                sns = [sn for sn in sorted(st.get("seasons") or {}) if sn > 0]
                if not sns:
                    return desc
                sizes = ", ".join(f"S{sn}={self.__season_size(st, sn)}集" for sn in sns[:8]) + (" …" if len(sns) > 8 else "")
                return f"{desc} [{sizes}]"
            except Exception as e:
                logger.debug(f"备选作品 {c.get('tmdbid')} 结构获取失败:{str(e)}")
                return desc

        alt_text = "; ".join(alt_desc(c) for c in alts) or "无"
        hint = f"\n提示: {primary['hint']}" if primary.get("hint") else ""
        if primary.get("binding"):
            b = primary["binding"]
            hint += (f"\n下载记录(MP 订阅匹配时已判定,可信): 该批次属于 S{b['season']:02d} {b.get('episodes_text') or ''}"
                     f",种子名 {b['torrent_name'][:100]}")
        user_msg = (f"发布目录名: {batch_name}\n当前作品: TMDB {primary['tmdbid']} {primary['title']} "
                    f"({primary['year']}) 类型:{primary['type']}{hint}\n文件列表(编号. 大小 文件名):\n{listing}\n\n"
                    f"季集结构:\n{self.__structure_text(structure)}\n\n备选作品(可能相关): {alt_text}")
        # 输入(文件、候选、TMDB 结构、模型)完全相同就沿用上次的答案:挂起批次复查时不重复调用、结果也不随机翻转;
        # 任何输入变化(TMDB 更新、换了候选/识别词、新文件)都会重新调用。预览 API 强制重建时重新调用
        ckey = hashlib.md5("\x00".join([str(getattr(settings, "LLM_PROVIDER", "") or ""),
                                        str(getattr(settings, "LLM_MODEL", "") or ""),
                                        MANIFEST_PROMPT, user_msg]).encode("utf-8")).hexdigest()
        if self._llm_cache is None:
            self._llm_cache = {}
        hit = self._llm_cache.get(ckey)
        if hit and time.time() - hit[0] < 7 * 86400 and not getattr(self._override_tls, "rebuild", False):
            logger.info(f"批次 {batch_name}:输入与上次相同,沿用上次的 LLM 季集映射(不重复调用)")
            return json.loads(hit[1])
        logger.info(f"批次 {batch_name}:调用 LLM 建立季集映射({len(rels)} 个文件)...")
        t0 = time.time()
        text = self.__llm_invoke(MANIFEST_PROMPT, user_msg, timeout=300)
        if text is None:
            return None
        data = self.__extract_json(text)
        if not isinstance(data, dict) or not isinstance(data.get("items"), list):
            logger.warn(f"LLM 批次清单返回非预期 JSON:{text[:200]}")
            return None
        logger.info(f"LLM 季集映射完成,耗时 {time.time() - t0:.1f}s,{len(data.get('items'))} 条")
        try:
            self._llm_cache[ckey] = (time.time(), json.dumps(data, ensure_ascii=False))
            if len(self._llm_cache) > 200:
                for k in sorted(self._llm_cache, key=lambda k: self._llm_cache[k][0])[:len(self._llm_cache) - 200]:
                    self._llm_cache.pop(k, None)
        except Exception as e:
            logger.debug(f"缓存 LLM 答案失败:{str(e)}")
        return data

    def __llm_invoke(self, system_prompt: str, user_msg: str, timeout: int = 180) -> Optional[str]:
        """调用 MoviePilot 系统 LLM(协程在线程内 asyncio.run);thinking=off 在部分端点会触发超长推理,映射为 low"""
        try:
            from app.agent.llm import LLMHelper
            from langchain_core.messages import HumanMessage, SystemMessage
        except Exception as e:
            logger.warn(f"LLM 组件不可用:{str(e)}")
            return None
        api_key = getattr(settings, "LLM_API_KEY", None)
        model = getattr(settings, "LLM_MODEL", None)
        if not api_key or not model:
            logger.warn("系统未配置 LLM(API Key/模型)")
            return None
        thinking = getattr(settings, "LLM_THINKING_LEVEL", None)
        if not thinking or str(thinking).lower() in ("off", "auto"):
            thinking = "low"
        holder: Dict[str, Any] = {}
        started = time.time()

        def _worker():
            try:
                llm = LLMHelper.get_llm(
                    streaming=False,
                    provider=getattr(settings, "LLM_PROVIDER", None),
                    model=model,
                    thinking_level=thinking,
                    api_key=api_key,
                    base_url=getattr(settings, "LLM_BASE_URL", None),
                    base_url_preset=getattr(settings, "LLM_BASE_URL_PRESET", None),
                    user_agent=getattr(settings, "LLM_USER_AGENT", None),
                    use_proxy=getattr(settings, "LLM_USE_PROXY", False),
                )
                if inspect.isawaitable(llm):
                    llm = asyncio.run(llm)
                completion = llm.invoke([SystemMessage(content=system_prompt), HumanMessage(content=user_msg)])
                holder["text"] = self.__llm_text(completion)
            except Exception as e:
                holder["error"] = str(e)
            if holder.get("abandoned"):
                logger.warn(f"LLM 调用在超时后才返回,实际耗时 {time.time() - started:.0f}s"
                            f"({'失败:' + holder['error'][:120] if holder.get('error') else '结果已丢弃'})")

        worker = threading.Thread(target=_worker, daemon=True)
        worker.start()
        worker.join(timeout=timeout)
        if worker.is_alive():
            holder["abandoned"] = True
            logger.warn(f"LLM 调用超时({timeout}s,thinking={thinking})")
            return None
        if "error" in holder:
            logger.warn(f"LLM 调用失败:{holder['error']}")
            return None
        return holder.get("text") or ""

    @staticmethod
    def __llm_text(completion: Any) -> str:
        content = getattr(completion, "content", completion)
        try:
            from app.agent.llm import LLMHelper
            extractor = getattr(LLMHelper, "extract_text_content", None) \
                or getattr(LLMHelper, "_extract_text_content", None)
            if callable(extractor):
                return str(extractor(content) or "").strip()
        except Exception:
            pass
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            parts = []
            for block in content:
                if isinstance(block, str):
                    parts.append(block)
                elif isinstance(block, dict) and block.get("type") in (None, "text") and not block.get("thought"):
                    parts.append(str(block.get("text") or ""))
            return "".join(parts).strip()
        return str(content or "").strip()

    @staticmethod
    def __extract_json(text: str) -> Any:
        text = (text or "").strip()
        m = re.match(r"^```(?:json)?\s*([\s\S]*?)\s*```$", text, re.IGNORECASE)
        if m:
            text = m.group(1).strip()
        if not (text.startswith("{") and text.endswith("}")):
            s, e = text.find("{"), text.rfind("}")
            if s >= 0 and e > s:
                text = text[s:e + 1]
        try:
            return json.loads(text)
        except Exception:
            return None

    def __validate_mapping(self, items: list, rels: List[str], parsed: Dict[str, dict], primary: dict,
                           structure: dict) -> Dict[str, dict]:
        """
        LLM 只有提案权:标记文件一律 extra;季集必须存在于目标编号体系、不重复;正片的季集必须是文件名季集
        在目标体系下的合理落点之一(直接对应 / 绝对集数换算 / 按季换算);正片体积不能远小于同批正片中位数;
        不通过一律 unknown(跳过,绝不猜)
        """
        got: Dict[str, dict] = {}
        for it in items:
            if not isinstance(it, dict):
                continue
            try:
                idx = int(it.get("i"))
            except (TypeError, ValueError):
                continue
            if 1 <= idx <= len(rels):
                got[rels[idx - 1]] = it
        is_tv = primary["type"] == MediaType.TV.value
        seasons = structure.get("seasons") or {}
        files: Dict[str, dict] = {}
        used: Dict[tuple, str] = {}
        for rel in rels:
            v = parsed[rel]
            if v["marker"]:
                files[rel] = {"c": "extra", "how": "rule", "note": v["marker"]}
                continue
            it = got.get(rel) or {}
            cls = str(it.get("c") or "unknown").lower()
            if cls not in ("main", "special", "extra", "other"):
                cls = "unknown"
            entry: Dict[str, Any] = {"c": cls, "how": "llm"}
            if cls in ("extra", "other") and v.get("episode") is not None \
                    and not EXTRA_HINT_RE.search(Path(rel).stem):
                # 文件名解析出了集号、规则也没认出特典标记,LLM 却判成特典/杂项:不能悄悄跳过(不通知、还可能被当残留清走),
                # 交人工确认(unknown 会通知);llm_c 记下 LLM 的原判,正片搬走后不让残留文件重建清单时被当正片映射
                entry = {"c": "unknown", "how": "llm", "llm_c": cls,
                         "note": f"LLM 判为{'特典' if cls == 'extra' else '杂项'},但文件名带集号 E{v['episode']},不采信"}
            if cls in ("main", "special") and is_tv:
                try:
                    s, e = int(it.get("s")), int(it.get("e"))
                except (TypeError, ValueError):
                    s, e = None, None
                plausible = self.__plausible_targets(structure, v["season"], v["episode"]) if cls == "main" else None
                if s is None or s not in seasons:
                    entry = {"c": "unknown", "how": "llm", "note": f"LLM 未给出有效季集({it.get('s')}/{it.get('e')})"}
                elif not 1 <= e <= self.__season_size(structure, s):
                    entry = {"c": "unknown", "how": "llm", "note": f"S{s}E{e} 超出范围"}
                elif v.get("episode") is None and v.get("season_declared") and v.get("season") is not None \
                        and s != int(v["season"]):
                    # 无集号时没有换算约束:识别词写明的季号就是约束
                    entry = {"c": "unknown", "how": "llm",
                             "note": f"识别词写明第 {v['season']} 季,LLM 给的是 S{s}E{e}"}
                elif plausible is not None and not plausible:
                    entry = {"c": "unknown", "how": "llm",
                             "note": f"文件名 S{v['season'] or '?'}E{v['episode']} 无法换算到目标编号体系"}
                elif plausible is not None and (s, e) not in plausible:
                    entry = {"c": "unknown", "how": "llm",
                             "note": f"LLM S{s}E{e} 与文件名 S{v['season'] or '?'}E{v['episode']} 的换算结果"
                                     f"{sorted(plausible)}不符"}
                elif (s, e) in used:
                    entry = {"c": "unknown", "how": "llm", "note": f"S{s}E{e} 与 {Path(used[(s, e)]).name} 重复"}
                    files[used[(s, e)]] = {"c": "unknown", "how": "llm", "note": f"S{s}E{e} 与 {Path(rel).name} 重复"}
                else:
                    used[(s, e)] = rel
                    entry.update({"s": s, "e": e})
                    if cls == "main" and v["episode"] is not None and \
                            (v["episode"] != e or (v["season"] is not None and v["season"] != s)):
                        entry["note"] = f"由文件名 S{v['season'] or '?'}E{v['episode']} 换算"
            files[rel] = entry
        mains = [(rel, parsed[rel]["size"]) for rel, v in files.items() if v.get("c") == "main"]
        if len(mains) >= 3:
            median = sorted(sz for _, sz in mains)[len(mains) // 2]
            for rel, sz in mains:
                if median and sz < 0.3 * median:
                    files[rel] = {"c": "unknown", "how": "llm",
                                  "note": f"体积 {self.__fmt_size(sz)} 远小于正片中位数 {self.__fmt_size(median)}"}
        return files

    def __aux_gate(self, file_path: Path, batch_root: Optional[Path], snapshot: tuple, file_meta,
                   is_subtitle: bool) -> Optional[str]:
        """
        回退路径的辅助识别门控:辅助识别(ChatGPT 插件)按文件名猜测并会整体覆盖季集,
        与文件名解析的集号冲突、或文件体积远小于同批主体文件时拒绝,返回原因
        """
        if is_subtitle:
            return None
        parsed_ep = snapshot[2]
        if parsed_ep is not None and file_meta.begin_episode is not None and parsed_ep != file_meta.begin_episode:
            return f"文件名集号 {parsed_ep} 与辅助识别 {file_meta.begin_episode} 不一致"
        if batch_root is not None and file_meta.begin_episode is not None:
            sizes = sorted((m[1] for m in self.__batch_listing(batch_root)["media"]), reverse=True)
            if len(sizes) >= 3:
                top = sizes[:max(1, int(len(sizes) * 0.4))]
                median = top[len(top) // 2]
                try:
                    size = file_path.stat().st_size
                except Exception:
                    size = 0
                if median and size < 0.3 * median:
                    return f"体积 {self.__fmt_size(size)} 远小于批次主体文件 {self.__fmt_size(median)},疑似特典/预告"
        return None

    # endregion

    # region Emby 联动:系列信息(剧集组)/覆盖重探/剧集组同步

    def __emby_server(self) -> Optional[Tuple[str, str]]:
        if self._emby_cache is None:
            self._emby_cache = {}
        if "server" in self._emby_cache:
            return self._emby_cache["server"]
        server = None
        try:
            for s in (self.systemconfig.get(SystemConfigKey.MediaServers) or []):
                if str(s.get("type") or "").lower() != "emby" or s.get("enabled") is False:
                    continue
                conf = s.get("config") or {}
                host, key = (conf.get("host") or "").rstrip("/"), conf.get("apikey")
                if host and key:
                    server = (host, key)
                    break
        except Exception as e:
            logger.debug(f"读取媒体服务器配置失败:{str(e)}")
        self._emby_cache["server"] = server
        return server

    def __emby_admin_id(self) -> Optional[str]:
        server = self.__emby_server()
        if not server:
            return None
        if self._emby_cache.get("admin"):
            return self._emby_cache["admin"]
        host, key = server
        try:
            users = requests.get(f"{host}/emby/Users", params={"api_key": key}, timeout=10).json() or []
            admin = next((u for u in users if (u.get("Policy") or {}).get("IsAdministrator")), None) \
                or (users[0] if users else None)
            if admin:
                self._emby_cache["admin"] = str(admin.get("Id"))
                return self._emby_cache["admin"]
        except Exception as e:
            logger.debug(f"查询 Emby 用户失败:{str(e)}")
        return None

    def __emby_series_info(self, tmdbid, refresh: bool = False) -> Optional[dict]:
        """Emby 中该剧(按 TMDB id)的条目信息:{"id","name","tmdbeg","display_order"};不存在返回 None(缓存 10 分钟)"""
        server = self.__emby_server()
        if not server or tmdbid is None:
            return None
        host, key = server
        cache = self._emby_cache.setdefault("series", {})
        hit = cache.get(str(tmdbid))
        if hit and not refresh and time.time() - hit[0] < 600:
            return hit[1]
        info = None
        try:
            resp = requests.get(f"{host}/emby/Items", params={
                "IncludeItemTypes": "Series", "Recursive": "true", "AnyProviderIdEquals": f"tmdb.{tmdbid}",
                "Fields": "ProviderIds,DisplayOrder", "api_key": key}, timeout=10).json()
            items = resp.get("Items") or []
            if items:
                it = items[0]
                pids = {str(k).lower(): v for k, v in (it.get("ProviderIds") or {}).items()}
                info = {"id": str(it.get("Id")), "name": it.get("Name"),
                        "tmdbeg": pids.get("tmdbeg") or None, "display_order": it.get("DisplayOrder")}
        except Exception as e:
            logger.debug(f"查询 Emby 系列失败:{str(e)}")
            return None
        cache[str(tmdbid)] = (time.time(), info)
        return info

    def __emby_episode_item(self, tmdbid, season, episode) -> Optional[str]:
        """Emby 中该剧对应季集的条目 id,不存在返回 None"""
        server = self.__emby_server()
        if not server or tmdbid is None or season is None or episode is None:
            return None
        host, key = server
        try:
            info = self.__emby_series_info(tmdbid)
            if not info:
                return None
            resp = requests.get(f"{host}/emby/Shows/{info['id']}/Episodes",
                                params={"Season": int(season), "api_key": key}, timeout=10).json()
            for it in resp.get("Items") or []:
                if it.get("ParentIndexNumber") == int(season) and it.get("IndexNumber") == int(episode):
                    return str(it.get("Id"))
        except Exception as e:
            logger.debug(f"查询 Emby 条目失败:{str(e)}")
        return None

    def __emby_reprobe_later(self, item_id: str, label: str, delay: int = 45):
        """入库覆盖了 Emby 已有条目的文件后,延迟通知 StrmAssistant 强制重探媒体信息(持久化媒体信息不会自动更新)"""
        server = self.__emby_server()
        if not server:
            return
        host, key = server

        def _run():
            try:
                resp = requests.post(f"{host}/emby/Items/{item_id}/ReprobeMediaInfo",
                                     params={"api_key": key}, timeout=30)
                if resp.status_code in (200, 204):
                    logger.info(f"已通知 Emby 重探媒体信息:{label}(条目 {item_id})")
                else:
                    logger.warn(f"Emby 重探接口返回 {resp.status_code}:{label}(需 StrmAssistant ≥ 2.3.1)")
            except Exception as e:
                logger.warn(f"通知 Emby 重探失败:{label} - {str(e)}")

        timer = threading.Timer(delay, _run)
        timer.daemon = True
        timer.start()

    def __emby_set_group(self, series_id: str, group_id: str) -> bool:
        """给 Emby 系列写入 TMDB 剧集组(ProviderIds.tmdbeg,与 Emby 元数据编辑器勾选剧集组等效)并整体刷新元数据"""
        server = self.__emby_server()
        admin = self.__emby_admin_id()
        if not server or not admin:
            return False
        host, key = server
        try:
            dto = requests.get(f"{host}/emby/Users/{admin}/Items/{series_id}", params={"api_key": key},
                               timeout=15).json()
            if not dto or not dto.get("Id"):
                return False
            pids = dict(dto.get("ProviderIds") or {})
            pids["tmdbeg"] = str(group_id)
            dto["ProviderIds"] = pids
            resp = requests.post(f"{host}/emby/Items/{series_id}", params={"api_key": key}, json=dto, timeout=30)
            if resp.status_code not in (200, 204):
                logger.warn(f"Emby 更新系列 {series_id} 剧集组失败:HTTP {resp.status_code} {resp.text[:120]}")
                return False
            requests.post(f"{host}/emby/Items/{series_id}/Refresh", params={
                "Recursive": "true", "MetadataRefreshMode": "FullRefresh", "ImageRefreshMode": "Default",
                "ReplaceAllMetadata": "true", "api_key": key}, timeout=30)
            return True
        except Exception as e:
            logger.warn(f"Emby 设置剧集组失败:{str(e)}")
            return False

    def __emby_sync_group(self, tmdbid, group_id: str, title: str):
        """
        新剧按剧集组入库后,等 Emby 建出该系列(最多 10 分钟),把同一个剧集组写入 Emby 并刷新元数据,
        保证 Emby 取元数据的顺序与文件命名一致。Emby 已配置其他剧集组时不覆盖,只告警。
        """
        if not self._emby_sync_group or not group_id or tmdbid is None:
            return
        key = str(tmdbid)
        if self._emby_sync_pending is None:
            self._emby_sync_pending = set()
        if key in self._emby_sync_pending:
            return
        self._emby_sync_pending.add(key)

        def _run():
            try:
                for _ in range(10):
                    time.sleep(60)
                    info = self.__emby_series_info(tmdbid, refresh=True)
                    if not info:
                        continue
                    if info.get("tmdbeg") == str(group_id):
                        logger.info(f"Emby 已为《{title}》配置剧集组 {group_id},无需同步")
                        return
                    if info.get("tmdbeg"):
                        logger.warn(f"Emby 为《{title}》配置的剧集组 {info.get('tmdbeg')} 与入库使用的 {group_id} 不同,"
                                    f"未覆盖;请统一后重新整理")
                        return
                    if self.__emby_set_group(info["id"], str(group_id)):
                        logger.info(f"已为 Emby 系列《{title}》设置剧集组 {group_id} 并触发元数据刷新")
                        if self._notify:
                            self.post_message(mtype=NotificationType.Manual,
                                              title=f"{title}:Emby 已同步剧集组",
                                              text=f"剧集组 {group_id} 已写入 Emby 并刷新元数据,季集顺序与入库文件一致")
                    return
                logger.warn(f"等待 10 分钟后 Emby 仍未出现《{title}》,剧集组 {group_id} 未同步;下次入库会再尝试")
            finally:
                self._emby_sync_pending.discard(key)

        worker = threading.Thread(target=_run, daemon=True)
        worker.start()

    # endregion

    def __clean_leftover_dirs(self):
        """
        整理残留清理:监控目录下一级子目录内已无待整理正片/字幕(剩余仅特典、附加、音乐、扫图等),
        且 30 分钟内无变动时,按策略隔离或删除整个目录。仅对移动模式的监控目录生效(复制/链接模式源文件是种子,不动);
        存在整理失败记录的目录保守不动。
        """
        policy = (self._leftover_policy or "off").lower()
        if policy not in ("quarantine", "delete"):
            return
        media_exts = [str(e).lower() for e in settings.RMT_MEDIAEXT]
        sub_exts = [str(e).lower() for e in settings.RMT_SUBEXT]
        keywords = self.__exclude_patterns()
        for mon_path in list(self._dirconf.keys()):
            if self._dirconf.get(mon_path) is None:
                continue
            transfer_type = self._transferconf.get(mon_path) or self._transfer_type
            if transfer_type not in ("move", "rclone_move"):
                continue
            root = Path(mon_path)
            if not root.exists():
                continue
            leftover_dir = Path(self._leftover_dir) if self._leftover_dir else root.parent / "整理残留"
            for child in root.iterdir():
                try:
                    if not child.is_dir() or child.name.startswith("."):
                        continue
                    if str(child) == str(leftover_dir):
                        continue
                    files = [f for f in child.rglob("*") if f.is_file()]
                    if not files:
                        continue
                    newest = max(f.stat().st_mtime for f in files)
                    if time.time() - newest < 1800:
                        continue
                    if ((self._manifests or {}).get(str(child)) or {}).get("status") == "retry" \
                            or str(child) in (self._recheck_pending or ()):
                        # 挂起中的批次还在等作品判定(认不出/分不清/TMDB 故障),或清单还没定(稳定窗口内、排着复查):不是残留
                        continue
                    pending, failed = [], []
                    for f in files:
                        suffix = f.suffix.lower()
                        if suffix not in media_exts and suffix not in sub_exts:
                            continue
                        if any(x in str(f) for x in ("/@Recycle/", "/#recycle/", "/.", "/@eaDir")):
                            # 回收站/隐藏文件永远不会被整理(与 __handle_file 一致),不挡残留清理
                            continue
                        his = self.transferhis.get_by_src(str(f))
                        if his:
                            if his.status:
                                continue
                            failed.append(f)
                            continue
                        if self.__match_any(keywords, str(f)):
                            continue
                        mf = (self._manifests or {}).get(str(child))
                        # 旧版规则建立的全标记清单不可信(可能把 The Menu 这类电影当特典),按文件名判定
                        entry = self.__manifest_entry(mf, child, f) \
                            if mf and mf.get("status") == "ok" and not self.__stale_marker_manifest(mf) else None
                        if entry:
                            if entry.get("c") in ("extra", "other"):
                                continue
                            pending.append(f)
                            continue
                        # 没有当前清单(建立失败/旧版清单):只有确定是特典的(特典目录、文件名硬标记)才算残留;
                        # 特典词可能是片名的(软标记)宁可留着,不把电影当残留清走
                        if self.__lexical_extra(f, mon_path):
                            continue
                        pending.append(f)
                    if failed:
                        logger.info(f"残留清理:{child.name} 含 {len(failed)} 个整理失败文件,保守不动")
                        continue
                    if pending:
                        continue
                    if policy == "delete":
                        logger.warn(f"残留清理:删除 {child}(剩余 {len(files)} 个特典/附加/杂项文件)")
                        shutil.rmtree(child, ignore_errors=True)
                    else:
                        leftover_dir.mkdir(parents=True, exist_ok=True)
                        dest = leftover_dir / child.name
                        if dest.exists():
                            dest = leftover_dir / f"{child.name}_{int(time.time())}"
                        logger.warn(f"残留清理:隔离 {child} -> {dest}(剩余 {len(files)} 个特典/附加/杂项文件)")
                        shutil.move(str(child), str(dest))
                except Exception as e:
                    logger.warn(f"残留清理 {child} 失败:{str(e)}")

    def event_handler(self, event, mon_path: str, text: str, event_path: str):
        """
        处理文件变化
        :param event: 事件
        :param mon_path: 监控目录
        :param text: 事件描述
        :param event_path: 事件文件路径
        """
        if not event.is_directory:
            # 文件发生变化
            logger.debug("文件%s：%s" % (text, event_path))
            self.__handle_file(event_path=event_path, mon_path=mon_path)

    def __handle_file(self, event_path: str, mon_path: str, from_reconcile: bool = False,
                      force_notify: bool = False):
        """
        同步一个文件
        :param event_path: 事件文件路径
        :param mon_path: 监控目录
        :param from_reconcile: 是否来自定时对账（失败记录允许有限次重试）
        :param force_notify: 通知型目录忽略已通知集合,强制重发(立即运行一次)
        """
        file_path = Path(event_path)
        try:
            if not file_path.exists():
                return
            # 全程加锁
            with lock:
                if self._handoffs and (event_path in self._handoffs or str(file_path) in self._handoffs):
                    # 已交给 MP 整理队列、还没结算:不重复整理(否则会重复调用整理、推迟结算等待期限,结算后还可能重复补做
                    # 后续步骤);也不占对账重试次数
                    logger.debug(f"{file_path.name} 已交给 MP 整理队列,等待结算,本轮跳过")
                    return
                transfer_history = self.transferhis.get_by_src(event_path)
                if transfer_history:
                    # 成功记录直接跳过；失败记录仅在对账时有限次重试
                    if transfer_history.status or not from_reconcile:
                        logger.debug("文件已处理过：%s" % event_path)
                        return
                    retried = self._retry_counts.get(event_path, 0) if self._retry_counts is not None else 0
                    if retried >= 3:
                        logger.debug(f"{event_path} 整理失败已重试 {retried} 次，不再重试")
                        return
                    if self._retry_counts is not None:
                        self._retry_counts[event_path] = retried + 1
                    logger.info(f"{event_path} 上次整理失败，对账重试第 {retried + 1} 次")

                # 回收站及隐藏的文件不处理
                if event_path.find('/@Recycle/') != -1 \
                        or event_path.find('/#recycle/') != -1 \
                        or event_path.find('/.') != -1 \
                        or event_path.find('/@eaDir') != -1:
                    logger.debug(f"{event_path} 是回收站或隐藏的文件")
                    return

                # 命中过滤关键字不处理（内置垃圾模式 + 用户配置合并生效）
                for keyword in self.__exclude_patterns():
                    try:
                        if keyword and re.findall(keyword, event_path):
                            logger.info(f"{event_path} 命中过滤关键字 {keyword}，不处理")
                            return
                    except Exception:
                        continue

                # 整理屏蔽词不处理
                transfer_exclude_words = self.systemconfig.get(SystemConfigKey.TransferExcludeWords)
                if transfer_exclude_words:
                    for keyword in transfer_exclude_words:
                        if not keyword:
                            continue
                        if keyword and re.search(r"%s" % keyword, event_path, re.IGNORECASE):
                            logger.info(f"{event_path} 命中整理屏蔽词 {keyword}，不处理")
                            return

                # 只处理媒体文件与字幕文件
                is_subtitle = file_path.suffix.lower() in [str(ext).lower() for ext in settings.RMT_SUBEXT]
                if file_path.suffix.lower() not in [str(e).lower() for e in settings.RMT_MEDIAEXT] and not is_subtitle:
                    logger.debug(f"{event_path} 不是媒体或字幕文件")
                    return

                # 批次清单:批次级识别(候选作品投票 + TMDB 季集结构 + 规则/LLM 映射 + 确定性校验),
                # 正片/特别篇按清单季集直接入库,特典/附加/未知一律跳过;无清单时回退 启发式 + 逐文件识别。
                # 最后一层由监控目录覆盖模式 size 兜底:更小的文件永远顶不掉已入库正片
                batch_root = self.__batch_root(file_path, mon_path)
                manifest = None
                entry = None
                # 直接位于监控目录根下的单个媒体文件(单文件种子):按"只有一个文件的批次"走同一套清单逻辑
                manifest_root = batch_root if batch_root is not None else (None if is_subtitle else file_path)
                if manifest_root is not None and self._dirconf.get(mon_path) is not None:
                    manifest = self.__get_manifest(manifest_root)
                    if not is_subtitle and not (manifest and manifest.get("pending")) and manifest_root.is_dir() \
                            and not self.__listed(manifest_root, file_path):
                        # 批次列举里还没有这个媒体文件(60 秒列举缓存之后才出现,或上次列举失败):丢掉缓存重取清单,
                        # 按新文件走稳定窗口/重建,不拿缺它条目的旧清单去走逐文件识别
                        if self._batch_list_cache:
                            self._batch_list_cache.pop(str(manifest_root), None)
                        first = manifest
                        manifest = self.__get_manifest(manifest_root)
                        if manifest is None and first and first.get("status") == "ok":
                            # 重新列举失败:沿用刚才的清单,不因此落到逐文件识别
                            manifest = first
                    if manifest and manifest.get("pending"):
                        logger.debug(f"批次 {manifest_root.name} 暂缓整理({manifest.get('reason') or '文件仍在变化'}),"
                                     f"本轮跳过:{file_path.name},{manifest.get('retry_in')} 秒后自动复查")
                        self.__schedule_batch_recheck(manifest_root, mon_path, int(manifest.get("retry_in") or 60))
                        return
                    if manifest:
                        entry = self.__manifest_entry(manifest, manifest_root, file_path)
                if entry:
                    if entry.get("c") in ("extra", "other"):
                        logger.info(f"{file_path.name} 批次清单判定为{entry.get('c')}"
                                    f"({entry.get('note') or entry.get('how')}),跳过整理")
                        return
                    if entry.get("c") == "unknown" or (
                            (manifest.get("media") or {}).get("type") == MediaType.TV.value
                            and (entry.get("s") is None or entry.get("e") is None)):
                        logger.info(f"{file_path.name} 批次清单无法确定季集({entry.get('note') or '未知'}),"
                                    f"跳过整理待人工处理")
                        return
                else:
                    verdict = self.__extra_verdict(file_path, mon_path)
                    if verdict:
                        logger.info(f"{file_path.name} 判定为{verdict},跳过整理:{file_path}")
                        return

                # 判断是不是蓝光目录
                if re.search(r"BDMV[/\\]STREAM", event_path, re.IGNORECASE):
                    # 截取BDMV前面的路径
                    blurray_dir = event_path[:event_path.find("BDMV")]
                    file_path = Path(blurray_dir)
                    logger.info(f"{event_path} 是蓝光目录，更正文件路径为：{str(file_path)}")
                    # 查询历史记录，已转移的不处理
                    if self.transferhis.get_by_src(str(file_path)):
                        logger.info(f"{file_path} 已整理过")
                        return

                # 元数据:清单命中时按清单季集构造(不依赖文件名解析);否则用清洗后的文件名解析(剥 CRC/集标题)
                if entry:
                    media = manifest.get("media") or {}
                    file_meta = MetaInfoPath(file_path)
                    try:
                        file_meta.type = MediaType(media.get("type"))
                    except Exception:
                        file_meta.type = MediaType.TV
                    if media.get("title"):
                        file_meta.name = media.get("title")
                    file_meta.year = media.get("year")
                    if entry.get("s") is not None:
                        file_meta.begin_season = int(entry["s"])
                        file_meta.end_season = None
                    if entry.get("e") is not None:
                        file_meta.begin_episode = int(entry["e"])
                        file_meta.end_episode = None
                    if media.get("episode_group"):
                        try:
                            file_meta.episode_group = media.get("episode_group")
                        except Exception:
                            pass
                else:
                    file_meta = self.__clean_meta(file_path)
                if not file_meta.name:
                    logger.error(f"{file_path.name} 无法识别有效信息")
                    return

                # 判断文件大小（字幕文件不受大小限制）
                if not is_subtitle and self._size and float(self._size) > 0 \
                        and file_path.stat().st_size < float(self._size) * 1024 ** 3:
                    logger.info(f"{file_path} 文件大小小于监控文件大小，不处理")
                    return

                # 查询转移目的目录
                target: Path = self._dirconf.get(mon_path)

                if self._strm and target is None:
                    # 通知型目录:只对新出现/内容变化(大小或 mtime)的文件发事件,避免对账/探测重复打扰下游
                    key = self.__notify_key(file_path)
                    notified = self._notified.setdefault(mon_path, {}) if self._notified is not None else {}
                    if not force_notify and key and notified.get(str(file_path)) == key:
                        logger.debug(f"{file_path} 已通知过 strm 助手,跳过")
                        return
                    logger.info(f"{file_path} 直接通知strm助手生成strm!")
                    self.eventmanager.send_event(EventType.PluginAction, {
                        'file_path': str(file_path),
                        'action': 'cloudstrm_file'
                    })
                    if key:
                        notified[str(file_path)] = key
                        self._notified_dirty = True
                        if not from_reconcile:
                            self.__save_notified()
                    return

                # 查询转移方式
                transfer_type = self._transferconf.get(mon_path)

                # 查找这个文件项
                file_item = self.storagechain.get_file_item(storage="local", path=file_path)
                if not file_item:
                    logger.warn(f"{file_path.name} 未找到对应的文件")
                    return
                if self._handoffs and str(file_item.path) in self._handoffs:
                    # 蓝光原盘按整盘目录交给整理队列
                    logger.debug(f"{file_path.name} 已交给 MP 整理队列,等待结算,本轮跳过")
                    return

                # 转移前校验源文件真实可读：挂载读空窗期(缓存陈旧)执行move，rename失败
                # 会退化为copy+delete，copy读到0字节即造成数据损坏(0字节上传+原文件删除)。
                # 不可读则本轮跳过(不记历史)，由后续对账自然重试。
                if not self.__source_readable(str(file_path)):
                    logger.warn(f"{event_path} 源文件暂不可读（读空窗），本轮跳过，待后续对账重试")
                    return

                # 字幕语言标记归一化：MP核心的字幕语言正则不识别 zh-Hans/zh-Hant，
                # 会导致简繁字幕改名后同名互相覆盖。归一化为 chs/cht 让核心正确追加
                # .chi.zh-cn / .zh-tw 后缀。
                if is_subtitle and file_item.name:
                    normalized_name = re.sub(r"zh[-_]?hant", "cht", file_item.name, flags=re.I)
                    normalized_name = re.sub(r"zh[-_]?hans", "chs", normalized_name, flags=re.I)
                    if normalized_name != file_item.name:
                        logger.info(f"字幕语言标记归一化：{file_item.name} -> {normalized_name}")
                        file_item.name = normalized_name
                if entry:
                    # 清单命中:按 TMDB id 直接取媒体信息,不做标题搜索、不触发辅助识别
                    media = manifest.get("media") or {}
                    mediainfo: MediaInfo = self.mediaChain.recognize_media(
                        meta=file_meta, mtype=file_meta.type, media_source=MediaSource.TMDB,
                        media_id=str(media.get("tmdbid")), episode_group=media.get("episode_group") or None)
                else:
                    # 回退路径:chain 层完整识别链(原生失败回退辅助识别);辅助识别改写了季集时必须过门控
                    snapshot = (file_meta.name, file_meta.begin_season, file_meta.begin_episode)
                    mediainfo: MediaInfo = self.mediaChain.recognize_by_meta(file_meta)
                    if mediainfo and (file_meta.name, file_meta.begin_season, file_meta.begin_episode) != snapshot:
                        reason = self.__aux_gate(file_path, batch_root, snapshot, file_meta, is_subtitle)
                        if reason:
                            logger.warn(f"{file_path.name} 辅助识别结果未通过校验({reason}),跳过整理待人工处理")
                            if self._history:
                                self.transferhis.add_fail(fileitem=file_item, mode=transfer_type, meta=file_meta)
                            return
                if not mediainfo:
                    logger.warn(f'未识别到媒体信息，标题：{file_meta.name}')
                    # 新增转移成功历史记录
                    his = self.transferhis.add_fail(
                        fileitem=file_item,
                        mode=transfer_type,
                        meta=file_meta
                    )
                    if self._notify:
                        self.post_message(
                            mtype=NotificationType.Manual,
                            title=f"{file_path.name} 未识别到媒体信息，无法入库！\n"
                                  f"回复：```\n/redo {his.id} [tmdbid]|[类型]\n``` 手动识别转移。"
                        )
                    return

                # 如果未开启新增已入库媒体是否跟随TMDB信息变化则根据tmdbid查询之前的title
                if not getattr(settings, "SCRAP_FOLLOW_TMDB", True):
                    transfer_history = None
                    if getattr(mediainfo, "media_source", None) and getattr(mediainfo, "media_id", None):
                        transfer_history = self.transferhis.get_by_media_identity(
                            media_source=mediainfo.media_source,
                            media_id=str(mediainfo.media_id),
                            mtype=mediainfo.type.value)
                    if transfer_history:
                        mediainfo.title = transfer_history.title
                logger.info(f"{file_path.name} 识别为：{mediainfo.type.value} {mediainfo.title_year}")

                # 获取集数据
                if mediainfo.type == MediaType.TV:
                    episode_group = ((manifest.get("media") or {}).get("episode_group") if entry else None) \
                        or getattr(mediainfo, "episode_group", None) or None
                    episodes_info = self.tmdbchain.tmdb_episodes(tmdbid=mediainfo.tmdb_id,
                                                                 season=1 if file_meta.begin_season is None else file_meta.begin_season,
                                                                 episode_group=episode_group)
                else:
                    episodes_info = None

                # TMDB集数校验：LLM辅助识别只看文件名，无法得知该季真实集数，
                # [25][SP]这类会被判为正篇第25集。集数超出该季范围且文件名带特典
                # 标记时划入特典季(Season 0)；超范围但无标记(可能为绝对集数命名)
                # 时保持原判并告警。
                if mediainfo.type == MediaType.TV and file_meta.begin_episode and episodes_info:
                    valid_episodes = {getattr(e, "episode_number", None) for e in episodes_info}
                    valid_episodes.discard(None)
                    if valid_episodes and file_meta.begin_episode not in valid_episodes:
                        if re.search(r"(?:^|[\[\s._-])(SP|OVA|OAD|SPECIALS?|特典|特別篇|特别篇|番外|NCOP\d*|NCED\d*|MENU\d*|CM\d+|PV\d+|PREVIEW|TEASER|COMMENTARY|DOCUMENTARY|SAMPLE)(?:[\]\s._-]|$)",
                                     file_path.name, re.IGNORECASE):
                            logger.info(f"{file_path.name} 集数 {file_meta.begin_episode} 超出该季TMDB范围"
                                        f"且含特典标记，划入特典季 Season 0")
                            file_meta.begin_season = 0
                            episodes_info = self.tmdbchain.tmdb_episodes(tmdbid=mediainfo.tmdb_id, season=0)
                        else:
                            logger.warn(f"{file_path.name} 集数 {file_meta.begin_episode} 超出TMDB该季集数范围"
                                        f"（可能为绝对集数命名），按原识别结果整理")

                # Emby 已有同季集条目时,入库覆盖后要通知 StrmAssistant 重探媒体信息(持久化媒体信息不会自动更新)
                pre_item = None
                if self._emby_reprobe and not is_subtitle and mediainfo.type == MediaType.TV \
                        and file_meta.begin_episode is not None:
                    pre_item = self.__emby_episode_item(
                        mediainfo.tmdb_id,
                        1 if file_meta.begin_season is None else file_meta.begin_season,
                        file_meta.begin_episode)

                # 查询转移目的目录
                target_dir = DirectoryHelper().get_dir(mediainfo, src_path=Path(mon_path))
                if not target_dir or not target_dir.library_path or not target_dir.download_path.startswith(mon_path):
                    target_dir = TransferDirectoryConf()
                    target_dir.library_path = target
                    target_dir.transfer_type = transfer_type
                    target_dir.scraping = self._scrape
                    target_dir.renaming = True
                    target_dir.notify = False
                    target_dir.overwrite_mode = self._overwrite_mode.get(mon_path) or 'never'
                    target_dir.library_storage = "local"
                    target_dir.library_type_folder = self._type
                    target_dir.library_category_folder = self._category
                else:
                    target_dir.transfer_type = transfer_type
                    target_dir.scraping = self._scrape

                if not target_dir.library_path:
                    logger.error(f"未配置监控目录 {mon_path} 的目的目录")
                    return

                # 转移前记下该源的整理记录现状:交给整理队列时据此判断队列是否结算了"这一次"
                try:
                    prev_row = self.__history_sig(self.transferhis.get_by_src(file_item.path, file_item.storage))
                except Exception:
                    prev_row = None
                # 转移文件
                transferinfo: TransferInfo = self.chain.transfer(fileitem=file_item,
                                                                 meta=file_meta,
                                                                 mediainfo=mediainfo,
                                                                 target_directory=target_dir,
                                                                 episodes_info=episodes_info)

                if not transferinfo:
                    logger.error("文件转移模块运行失败")
                    return

                if not transferinfo.success and str(transferinfo.message or "").strip() in QUEUE_HANDOFF_MESSAGES:
                    # 同步调用失败、任务留在 MP 持久化整理队列里由后台接着做(闯关东 E44:报失败 13 秒后队列入库成功):
                    # 不报失败、不写失败记录(队列结算会写),记下来等结算后补做刮削/通知/strm;不在全局锁里干等
                    self.__defer_handoff(file_item, prev_row, dict(
                        file_path=file_path, mon_path=mon_path, file_meta=file_meta, mediainfo=mediainfo,
                        transfer_type=transfer_type, is_subtitle=is_subtitle, pre_item=pre_item, entry=entry,
                        manifest=manifest))
                    return

                if not transferinfo.success:
                    # 转移失败
                    logger.warn(f"{file_path.name} 入库失败：{transferinfo.message}")

                    if self._history:
                        # 新增转移失败历史记录
                        self.transferhis.add_fail(
                            fileitem=file_item,
                            mode=transfer_type,
                            meta=file_meta,
                            mediainfo=mediainfo,
                            transferinfo=transferinfo
                        )
                    if self._notify:
                        self.post_message(
                            mtype=NotificationType.Manual,
                            title=f"{mediainfo.title_year}{file_meta.season_episode} 入库失败！",
                            text=f"原因：{transferinfo.message or '未知'}",
                            image=mediainfo.get_message_image()
                        )
                    return

                if self._history:
                    # 新增转移成功历史记录
                    self.transferhis.add_success(
                        fileitem=file_item,
                        mode=transfer_type,
                        meta=file_meta,
                        mediainfo=mediainfo,
                        transferinfo=transferinfo
                    )

                self.__after_transfer(file_path=file_path, mon_path=mon_path, file_meta=file_meta,
                                      mediainfo=mediainfo, transferinfo=transferinfo, transfer_type=transfer_type,
                                      is_subtitle=is_subtitle, pre_item=pre_item, entry=entry, manifest=manifest)

        except Exception as e:
            logger.error("目录监控发生错误：%s - %s" % (str(e), traceback.format_exc()))

    @staticmethod
    def __history_sig(his) -> Optional[tuple]:
        """整理记录的指纹(同一源路径的记录会被结算原地更新,只看 id 不够)"""
        if not his:
            return None
        return getattr(his, "id", None), bool(getattr(his, "status", False)), str(getattr(his, "date", "") or ""), \
            str(getattr(his, "dest", "") or "")

    def __defer_handoff(self, file_item, prev_row, ctx: dict):
        """记下交给 MP 整理队列的整理,由 __resolve_handoffs 在锁外等它结算"""
        if self._handoffs is None:
            self._handoffs = {}
        key = str(file_item.path)
        old = self._handoffs.get(key)
        # 同一源重复交接时保留最早的等待期限,不能靠重复交接无限推迟"超时报失败"
        self._handoffs[key] = {"storage": file_item.storage or "local", "prev": prev_row,
                               "deadline": old["deadline"] if old else time.time() + HANDOFF_WAIT, "ctx": ctx,
                               "file_item": file_item}
        logger.info(f"{ctx['file_path'].name} 同步整理调用未完成,MP 整理队列在后台接手;"
                    f"结算后补做刮削/通知/strm(最长等 {HANDOFF_WAIT // 60} 分钟)")
        self.__arm_handoff_timer()

    def __arm_handoff_timer(self):
        if self._handoff_timer and self._handoff_timer.is_alive():
            return

        def _run():
            try:
                self.__resolve_handoffs()
            except Exception as e:
                logger.error(f"处理整理队列接手的文件出错:{str(e)} - {traceback.format_exc()}")
            finally:
                self._handoff_timer = None
                if self._handoffs:
                    self.__arm_handoff_timer()

        timer = threading.Timer(30, _run)
        timer.daemon = True
        self._handoff_timer = timer
        timer.start()

    def __resolve_handoffs(self):
        """
        交给整理队列的文件:整理记录出现新的成功结算 -> 按成功补做后续步骤(与同步成功完全相同);
        新的失败结算或超时 -> 报失败(用队列给的原因)。记录按源路径查(蓝光按整盘目录),只认转移之后的新结算
        """
        for key, h in list((self._handoffs or {}).items()):
            ctx = h["ctx"]
            name = ctx["file_path"].name
            try:
                his = self.transferhis.get_by_src(key, h["storage"])
            except Exception as e:
                logger.debug(f"查询整理记录失败 {key}:{str(e)}")
                continue
            sig = self.__history_sig(his)
            settled = sig is not None and sig != h["prev"]
            if settled and his.status:
                if self._handoffs.pop(key, None) is None:
                    continue
                if not his.dest:
                    # V3 把无集数的特典/附加文件判为"跳过正片集数整理":成功但没有目标文件,与同步整理一样不做后续步骤
                    logger.info(f"{name} 已由 MP 整理队列按特典/附加文件处理,无目标文件,跳过刮削与通知")
                    continue
                transferinfo = self.__handoff_transferinfo(his, h["file_item"], ctx["transfer_type"])
                if not transferinfo:
                    logger.warn(f"{name} 已由 MP 整理队列整理成功({his.dest}),但读不到目标文件,"
                                f"strm 由 Strm 助手的全量对账补上")
                    continue
                if str(getattr(his, "media_id", "") or "") and ctx.get("mediainfo") is not None \
                        and str(his.media_id) != str(getattr(ctx["mediainfo"], "tmdb_id", "")):
                    logger.warn(f"{name} 由整理队列入库的作品(TMDB {his.media_id})与本次整理的"
                                f"(TMDB {ctx['mediainfo'].tmdb_id})不同,以实际入库位置补做后续步骤:{his.dest}")
                logger.info(f"{name} 已由 MP 整理队列接手整理成功:{his.dest},补做后续步骤")
                with lock:
                    self.__after_transfer(transferinfo=transferinfo, **ctx)
            elif settled and not his.status:
                if self._handoffs.pop(key, None) is None:
                    continue
                self.__handoff_failed(ctx, getattr(his, "errmsg", None) or "MP 整理队列整理失败")
            elif time.time() > h["deadline"]:
                with lock:
                    # 等锁期间队列可能刚好结算:重读一次,结算了就交给下一轮按结算处理(不能拿失败记录盖掉成功结算)
                    try:
                        latest = self.transferhis.get_by_src(key, h["storage"])
                    except Exception as e:
                        logger.debug(f"查询整理记录失败 {key}:{str(e)}")
                        continue
                    latest_sig = self.__history_sig(latest)
                    if latest_sig is not None and latest_sig != h["prev"]:
                        continue
                    if self._handoffs.pop(key, None) is None:
                        continue
                    if self._history:
                        # 与同步整理失败一致记失败:对账按失败有限次重试、残留清理保守不动;
                        # 队列之后若结算成功,会原地改写这条记录
                        try:
                            self.transferhis.add_fail(fileitem=h["file_item"], mode=ctx["transfer_type"],
                                                      meta=ctx["file_meta"], mediainfo=ctx["mediainfo"])
                        except Exception as e:
                            logger.warn(f"{name} 写入整理失败记录出错:{str(e)}")
                self.__handoff_failed(ctx, f"MP 整理队列 {HANDOFF_WAIT // 60} 分钟内没有结算"
                                           f"(若之后整理成功,strm 由 Strm 助手的全量对账补上)")

    def __handoff_failed(self, ctx: dict, reason: str):
        name = ctx["file_path"].name
        logger.warn(f"{name} 入库失败:{reason}")
        mediainfo, file_meta = ctx.get("mediainfo"), ctx.get("file_meta")
        if self._notify and mediainfo is not None and file_meta is not None:
            try:
                self.post_message(mtype=NotificationType.Manual,
                                  title=f"{mediainfo.title_year}{file_meta.season_episode} 入库失败！",
                                  text=f"原因:{reason}", image=mediainfo.get_message_image())
            except Exception as e:
                logger.warn(f"发送入库失败通知出错:{str(e)}")

    def __handoff_transferinfo(self, his, file_item, transfer_type: str) -> Optional[TransferInfo]:
        """按整理队列的成功记录重建整理结果(目标文件/目录项与同步整理返回的一致)"""
        dest_storage = getattr(his, "dest_storage", None) or "local"
        target_item = None
        try:
            target_item = self.storagechain.get_file_item(storage=dest_storage, path=Path(his.dest))
        except Exception as e:
            logger.debug(f"读取整理目标文件失败 {his.dest}:{str(e)}")
        raw = getattr(his, "dest_fileitem", None)
        if not target_item and raw:
            try:
                target_item = schemas.FileItem(**(raw if isinstance(raw, dict) else json.loads(raw)))
            except Exception as e:
                logger.debug(f"解析整理记录目标文件失败 {his.dest}:{str(e)}")
        if not target_item:
            return None
        parent = Path(target_item.path).parent
        target_diritem = None
        try:
            target_diritem = self.storagechain.get_file_item(storage=target_item.storage or dest_storage, path=parent)
        except Exception as e:
            logger.debug(f"读取整理目标目录失败 {parent}:{str(e)}")
        if not target_diritem:
            target_diritem = schemas.FileItem(storage=target_item.storage or dest_storage, type="dir",
                                              path=str(parent).rstrip("/") + "/", name=parent.name)
        return TransferInfo(success=True, fileitem=file_item, target_item=target_item,
                            target_diritem=target_diritem, transfer_type=transfer_type,
                            file_list=[str(file_item.path)], file_list_new=[target_item.path],
                            total_size=target_item.size or 0, file_count=1)


    def __after_transfer(self, file_path: Path, mon_path: str, file_meta, mediainfo, transferinfo: TransferInfo,
                         transfer_type: str, is_subtitle: bool, pre_item, entry, manifest):
        """整理成功后的后续步骤:刮削、入库消息汇总、事件、软连接/strm、Emby 重探/剧集组同步、移动模式清空目录"""
        # V3 会把无集数的特典/附加文件判为"跳过正片集数整理":
        # success=True 但无 target_item/target_diritem,
        # 后续刮削/通知/软连接/strm 均无目标可用,记录历史后直接结束
        if not transferinfo.target_item:
            logger.info(f"{file_path.name} 按特典/附加文件处理,无目标文件项,"
                        f"跳过刮削与通知")
            return

        # 刮削（字幕文件无需刮削）
        if self._scrape and not is_subtitle:
            self.scrapingchain.scrape_metadata(fileitem=transferinfo.target_diritem,
                                               meta=file_meta,
                                               mediainfo=mediainfo)
        """
        {
            "title_year season": {
                "files": [
                    {
                        "path":,
                        "mediainfo":,
                        "file_meta":,
                        "transferinfo":
                    }
                ],
                "time": "2023-08-24 23:23:23.332"
            }
        }
        """
        if self._notify and not is_subtitle:
            # 发送消息汇总
            media_list = self._medias.get(mediainfo.title_year + " " + file_meta.season) or {}
            if media_list:
                media_files = media_list.get("files") or []
                if media_files:
                    file_exists = False
                    for file in media_files:
                        if str(file_path) == file.get("path"):
                            file_exists = True
                            break
                    if not file_exists:
                        media_files.append({
                            "path": str(file_path),
                            "mediainfo": mediainfo,
                            "file_meta": file_meta,
                            "transferinfo": transferinfo
                        })
                else:
                    media_files = [
                        {
                            "path": str(file_path),
                            "mediainfo": mediainfo,
                            "file_meta": file_meta,
                            "transferinfo": transferinfo
                        }
                    ]
                media_list = {
                    "files": media_files,
                    "time": datetime.datetime.now()
                }
            else:
                media_list = {
                    "files": [
                        {
                            "path": str(file_path),
                            "mediainfo": mediainfo,
                            "file_meta": file_meta,
                            "transferinfo": transferinfo
                        }
                    ],
                    "time": datetime.datetime.now()
                }
            self._medias[mediainfo.title_year + " " + file_meta.season] = media_list

        if self._refresh and not is_subtitle:
            # 广播事件
            self.eventmanager.send_event(EventType.TransferComplete, {
                'meta': file_meta,
                'mediainfo': mediainfo,
                'transferinfo': transferinfo
            })

        if self._softlink and not is_subtitle:
            # 通知实时软连接生成
            self.eventmanager.send_event(EventType.PluginAction, {
                'file_path': str(transferinfo.target_item.path),
                'action': 'softlink_file'
            })

        if self._strm:
            # 通知前强刷目标目录(OpenList->rclone 双层,按目录60秒去重),
            # 消除读空窗,strm助手的就绪门控可首查即过
            self.__refresh_target_dir(str(Path(transferinfo.target_item.path).parent))
            # 通知Strm助手生成（媒体文件生成strm，字幕文件由助手复制到strm目录）
            self.eventmanager.send_event(EventType.PluginAction, {
                'file_path': str(transferinfo.target_item.path),
                'action': 'cloudstrm_file'
            })

        if pre_item:
            self.__emby_reprobe_later(pre_item, file_path.name)
        if entry and manifest.get("sync_emby") and not is_subtitle:
            media = manifest.get("media") or {}
            self.__emby_sync_group(media.get("tmdbid"), media.get("episode_group"), media.get("title") or "")

        # 移动模式删除空目录（必须确认媒体/字幕/音轨文件都已清空，防止误删未整理的字幕）
        if transfer_type == "move":
            for file_dir in file_path.parents:
                if len(str(file_dir)) <= len(str(Path(mon_path))):
                    # 重要，删除到监控目录为止
                    break
                files = SystemUtils.list_files(
                    file_dir,
                    settings.RMT_MEDIAEXT + settings.RMT_SUBEXT
                    + getattr(settings, "RMT_AUDIOEXT", []) + settings.DOWNLOAD_TMPEXT)
                if not files:
                    logger.warn(f"移动模式，删除空目录：{file_dir}")
                    shutil.rmtree(file_dir, ignore_errors=True)

    def send_msg(self):
        """
        定时检查是否有媒体处理完，发送统一消息
        """
        if not self._medias or not self._medias.keys():
            return

        # 遍历检查是否已刮削完，发送消息
        for medis_title_year_season in list(self._medias.keys()):
            media_list = self._medias.get(medis_title_year_season)
            logger.info(f"开始处理媒体 {medis_title_year_season} 消息")

            if not media_list:
                continue

            # 获取最后更新时间
            last_update_time = media_list.get("time")
            media_files = media_list.get("files")
            if not last_update_time or not media_files:
                continue

            transferinfo = media_files[0].get("transferinfo")
            file_meta = media_files[0].get("file_meta")
            mediainfo = media_files[0].get("mediainfo")
            # 判断剧集最后更新时间距现在是已超过10秒或者电影，发送消息
            if (datetime.datetime.now() - last_update_time).total_seconds() > int(self._interval) \
                    or mediainfo.type == MediaType.MOVIE:
                # 发送通知
                if self._notify:

                    # 汇总处理文件总大小
                    total_size = 0
                    file_count = 0

                    # 剧集汇总
                    episodes = []
                    for file in media_files:
                        transferinfo = file.get("transferinfo")
                        total_size += transferinfo.total_size
                        file_count += 1

                        file_meta = file.get("file_meta")
                        if file_meta and file_meta.begin_episode:
                            episodes.append(file_meta.begin_episode)

                    transferinfo.total_size = total_size
                    # 汇总处理文件数量
                    transferinfo.file_count = file_count

                    # 剧集季集信息 S01 E01-E04 || S01 E01、E02、E04
                    season_episode = None
                    # 处理文件多，说明是剧集，显示季入库消息
                    if mediainfo.type == MediaType.TV:
                        # 季集文本
                        season_episode = f"{file_meta.season} {StringUtils.format_ep(episodes)}"
                    # 发送消息
                    self.transferchian.send_transfer_message(meta=file_meta,
                                                             mediainfo=mediainfo,
                                                             transferinfo=transferinfo,
                                                             season_episode=season_episode)
                # 发送完消息，移出key
                del self._medias[medis_title_year_season]
                continue

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        定义远程控制命令
        :return: 命令关键字、事件、描述、附带数据
        """
        return [{
            "cmd": "/cloud_link_sync",
            "event": EventType.PluginAction,
            "desc": "云盘实时监控同步",
            "category": "",
            "data": {
                "action": "cloud_link_sync"
            }
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        return [{
            "path": "/cloud_link_sync",
            "endpoint": self.sync,
            "methods": ["GET"],
            "summary": "云盘实时监控同步",
            "description": "云盘实时监控同步",
        }, {
            "path": "/batch_manifest",
            "endpoint": self.batch_manifest,
            "methods": ["GET"],
            "summary": "批次清单预览",
            "description": "对指定批次目录建立/查看批次清单(作品识别 + 季集映射),不整理文件;rebuild=true 强制重建",
        }]

    def batch_manifest(self, path: str, rebuild: bool = False, ignore_emby: bool = False,
                       force_default: bool = False) -> schemas.Response:
        """
        API:批次清单预览(不整理文件);ignore_emby/force_default 用于模拟"Emby 无此剧"/"按默认顺序"的场景
        """
        batch_root = Path(os.path.normpath(path))
        if not batch_root.exists():
            return schemas.Response(success=False, message=f"路径不存在:{path}")
        self._order_override = {"ignore_emby": bool(ignore_emby), "force_default": bool(force_default)} \
            if (ignore_emby or force_default) else None
        key = str(batch_root)
        if not self._order_override and not self.__monitor_of_batch(key):
            # 不是整理型监控目录下的第一级条目(如测试批次、子目录):只读预览,不落盘、不发通知
            self._order_override = {"preview_only": True}
        if rebuild and not self._order_override and self._batch_seen:
            self._batch_seen.pop(key, None)
        if rebuild and self._batch_list_cache:
            self._batch_list_cache.pop(key, None)
        self._override_tls.no_settle = True
        # 强制重建:不沿用已有清单/退避/上次的 LLM 答案;已有清单不删,挂起中的批次保留连续次数与通知状态,
        # 重建失败时继续暂缓(不回退逐文件识别)
        self._override_tls.rebuild = bool(rebuild)
        try:
            mf = self.__get_manifest(batch_root)
        finally:
            self._override_tls.no_settle = False
            self._override_tls.rebuild = False
            self._order_override = None
        data = mf if mf else (self._manifests or {}).get(key)
        return schemas.Response(success=bool(mf), message="" if mf else "清单未建立(见插件日志)", data=data)

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        [{
            "id": "服务ID",
            "name": "服务名称",
            "trigger": "触发器：cron/interval/date/CronTrigger.from_crontab()",
            "func": self.xxx,
            "kwargs": {} # 定时器参数
        }]
        """
        services = []
        if self._enabled and self._cron:
            try:
                services.append({
                    "id": "CloudLinkMonitor",
                    "name": "云盘目录定时对账服务",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.sync_reconcile,
                    "kwargs": {}
                })
            except Exception as e:
                logger.error(f"注册定时对账服务失败，请检查cron表达式：{str(e)}")
        if self._enabled and self._fast_interval and int(self._fast_interval) > 0 \
                and self._openlist_url and self._openlist_token:
            services.append({
                "id": "CloudLinkMonitorFastProbe",
                "name": "云盘目录快速探测",
                "trigger": "interval",
                "func": self.fast_probe,
                "kwargs": {"seconds": int(self._fast_interval)}
            })
        return services

    def sync(self) -> schemas.Response:
        """
        API调用目录同步
        """
        self.sync_all()
        return schemas.Response(success=True)

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'notify',
                                            'label': '发送通知',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VForm',
                        'content': [
                            {
                                'component': 'VRow',
                                'content': [
                                    {
                                        'component': 'VCol',
                                        'props': {
                                            'cols': 12,
                                            'md': 4
                                        },
                                        'content': [
                                            {
                                                'component': 'VSwitch',
                                                'props': {
                                                    'model': 'history',
                                                    'label': '存储历史记录',
                                                }
                                            }
                                        ]
                                    },
                                    {
                                        'component': 'VCol',
                                        'props': {
                                            'cols': 12,
                                            'md': 4
                                        },
                                        'content': [
                                            {
                                                'component': 'VSwitch',
                                                'props': {
                                                    'model': 'scrape',
                                                    'label': '是否刮削',
                                                }
                                            }
                                        ]
                                    },
                                    {
                                        'component': 'VCol',
                                        'props': {
                                            'cols': 12,
                                            'md': 4
                                        },
                                        'content': [
                                            {
                                                'component': 'VSwitch',
                                                'props': {
                                                    'model': 'type',
                                                    'label': '是否按类型分类',
                                                }
                                            }
                                        ]
                                    },
                                    {
                                        'component': 'VCol',
                                        'props': {
                                            'cols': 12,
                                            'md': 4
                                        },
                                        'content': [
                                            {
                                                'component': 'VSwitch',
                                                'props': {
                                                    'model': 'category',
                                                    'label': '是否二级分类',
                                                }
                                            }
                                        ]
                                    },
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VForm',
                        'content': [
                            {
                                'component': 'VRow',
                                'content': [
                                    {
                                        'component': 'VCol',
                                        'props': {
                                            'cols': 12,
                                            'md': 4
                                        },
                                        'content': [
                                            {
                                                'component': 'VSwitch',
                                                'props': {
                                                    'model': 'refresh',
                                                    'label': '刷新媒体库',
                                                },
                                            }
                                        ]
                                    },
                                    {
                                        'component': 'VCol',
                                        'props': {
                                            'cols': 12,
                                            'md': 4
                                        },
                                        'content': [
                                            {
                                                'component': 'VSwitch',
                                                'props': {
                                                    'model': 'softlink',
                                                    'label': '联动实时软连接',
                                                },
                                            }
                                        ]
                                    },
                                    {
                                        'component': 'VCol',
                                        'props': {
                                            'cols': 12,
                                            'md': 4
                                        },
                                        'content': [
                                            {
                                                'component': 'VSwitch',
                                                'props': {
                                                    'model': 'strm',
                                                    'label': '联动Strm生成',
                                                },
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'mode',
                                            'label': '监控模式',
                                            'items': [
                                                {'title': '兼容模式', 'value': 'compatibility'},
                                                {'title': '性能模式', 'value': 'fast'}
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'transfer_type',
                                            'label': '转移方式',
                                            'items': [
                                                {'title': '移动', 'value': 'move'},
                                                {'title': '复制', 'value': 'copy'},
                                                {'title': '硬链接', 'value': 'link'},
                                                {'title': '软链接', 'value': 'softlink'},
                                                {'title': 'Rclone复制', 'value': 'rclone_copy'},
                                                {'title': 'Rclone移动', 'value': 'rclone_move'}
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'interval',
                                            'label': '入库消息延迟',
                                            'placeholder': '10'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'monitor_dirs',
                                            'label': '监控目录',
                                            'rows': 5,
                                            'placeholder': '每一行一个目录，支持以下几种配置方式，转移方式支持 move、copy、link、softlink、rclone_copy、rclone_move：\n'
                                                           '监控目录:转移目的目录\n'
                                                           '监控目录:转移目的目录#转移方式\n'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'exclude_keywords',
                                            'label': '排除关键词',
                                            'rows': 2,
                                            'placeholder': '每一行一个关键词'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '对账周期(cron)',
                                            'placeholder': '*/10 * * * *'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'fast_interval',
                                            'label': '快速探测间隔(秒,0关闭)',
                                            'placeholder': '60'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'junk_clean',
                                            'label': '对账时清理垃圾目录',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'junk_exts',
                                            'label': '垃圾文件扩展名',
                                            'placeholder': '.url,.html,.htm,.txt'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'llm_classify',
                                            'label': 'LLM 批次清单(作品识别+季集映射)',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'leftover_policy',
                                            'label': '整理残留处理',
                                            'items': [
                                                {'title': '不处理', 'value': 'off'},
                                                {'title': '隔离到残留目录', 'value': 'quarantine'},
                                                {'title': '直接删除', 'value': 'delete'}
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'leftover_dir',
                                            'label': '残留隔离目录',
                                            'placeholder': '留空=监控目录同级的 整理残留'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'batch_settle',
                                            'label': '批次稳定等待(秒)',
                                            'placeholder': '120'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'emby_reprobe',
                                            'label': '覆盖后通知 Emby 重探媒体信息',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'emby_sync_group',
                                            'label': '新剧自动同步剧集组到 Emby',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'group_map',
                                            'label': '剧集组映射(tmdbid=剧集组id)',
                                            'rows': 2,
                                            'placeholder': '65942=641eb9d6b234b9007ac67063'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'notify_probe_interval',
                                            'label': '通知型目录探测间隔(秒,0=关)',
                                            'placeholder': '180'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'density': 'compact',
                                            'text': '批次清单:同一发布目录先整体识别作品、决定编号体系(Emby 已配置的剧集组 > '
                                                    '下载记录/订阅的剧集组 > 映射 > 合并季自动选组并同步 Emby > TMDB 默认顺序),'
                                                    '拉取该体系下的季集结构;绝对集数/按季编号的资源都换算到目标体系;'
                                                    '命名不规整时由 LLM 一次给出分类与季集,再经范围/换算/重复/体积校验;'
                                                    '不确定的文件跳过并通知,绝不猜。'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'openlist_url',
                                            'label': 'OpenList地址',
                                            'placeholder': 'http://127.0.0.1:5244'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 8
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'openlist_token',
                                            'label': 'OpenList令牌',
                                            'placeholder': '用于对账前强制刷新网盘列表'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'openlist_base',
                                            'label': 'OpenList根路径',
                                            'placeholder': '/115'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'rc_mount_prefix',
                                            'label': '容器内挂载前缀',
                                            'placeholder': '/115_rclone'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'rc_fs',
                                            'label': 'rclone远端(fs)',
                                            'placeholder': 'remote:path'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'rc_socket',
                                            'label': 'rclone rc socket',
                                            'placeholder': '/var/run/rclone/rcd_1000.sock'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '定时对账：先通过OpenList API强制刷新网盘真实列表，再刷新rclone挂载缓存，'
                                                    '然后扫描监控目录处理遗漏的媒体/字幕文件（含失败重试，最多3次）。'
                                                    '快速探测：每隔N秒强刷监控目录浅层列表（顶层+一级子目录，成本约1次API调用），'
                                                    '发现变化立即触发对账，把检测延迟从对账周期压缩到探测间隔。'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '入库消息延迟默认10s，如网络较慢可酌情调大，有助于发送统一入库消息。'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '如果监控目录与目录设置一致，则默认使用目录设置配置。否则可在监控目录后拼接@覆盖方式（默认never覆盖方式）。'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '开启联动实时软连接/Strm会在监控转移后联动【实时软连接】/【云盘Strm[助手]】插件生成软连接/Strm（只处理媒体文件，不处理刮削文件）。'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "notify": False,
            "onlyonce": False,
            "history": False,
            "scrape": False,
            "type": True,
            "category": False,
            "refresh": True,
            "softlink": False,
            "strm": False,
            "mode": "fast",
            "transfer_type": "filesoftlink",
            "monitor_dirs": "",
            "exclude_keywords": "",
            "interval": 10,
            "cron": "",
            "size": 0,
            "openlist_url": "",
            "openlist_token": "",
            "openlist_base": "",
            "rc_socket": "",
            "rc_fs": "",
            "rc_mount_prefix": "",
            "junk_clean": False,
            "junk_exts": ".url,.html,.htm,.txt",
            "fast_interval": 0,
            "llm_classify": True,
            "leftover_policy": "quarantine",
            "leftover_dir": "",
            "batch_settle": 120,
            "emby_reprobe": True,
            "emby_sync_group": True,
            "group_map": "",
            "notify_probe_interval": 180
        }

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        """
        退出插件
        """
        if self._observer:
            for observer in self._observer:
                try:
                    observer.stop()
                    observer.join()
                except Exception as e:
                    print(str(e))
        self._observer = []
        self.__cancel_rechecks()
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                self._event.set()
                self._scheduler.shutdown()
                self._event.clear()
            self._scheduler = None
