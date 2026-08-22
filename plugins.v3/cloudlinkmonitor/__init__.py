import asyncio
import datetime
import hashlib
import inspect
import json
import re
import shutil
import socket
import threading
import time
import traceback
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

# 特典/附加子目录名(任意一级子目录命中即整体跳过;不作用于种子根目录)
SP_DIR_RE = re.compile(
    r"(?:^|[\s_\-.\[\(（【])(bonus(?:es)?|extras?|specials?|sps?|nc(?:op|ed)?|pv|cm|menus?|previews?|trailers?|"
    r"scans?|cds?|bk|booklet|gallery|fonts?|samples?|others?)(?:$|[\s_\-.\]\)）】])"
    r"|特典|番外|映像|特報|预告|扫图|画集|原声|音乐", re.IGNORECASE)
# 文件名特典标记(无条件按特典处理;SP/OVA/OAD/番外 这类有内容的特别篇不在此列,由批次清单按 TMDB 第 0 季映射)
SP_FILE_RE = re.compile(
    r"(?:^|[\s_\-.\[\(（【])(NC(?:OP|ED)\d*|MENU\d*|PV\d*|CM\d*|(?:WEB\s*|THEATER\s*)?PREVIEW\s*\d*|TEASER\d*|"
    r"TRAILER\d*|COMMENTARY|DOCUMENTARY\d*|SAMPLE|INTERVIEW|MAKING|DIGEST|LOGO|TV\s*SPOT|CREDITLESS|PROMO|"
    r"映像特典|特典映像|予告編?|次回予告|预告|預告|宣传|特報|特报)(?:$|[\s_\-.\]\)）】\(（#])", re.IGNORECASE)
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
    "6) 若该批次实际属于备选作品列表中的另一部作品,在 series_tmdbid 中给出其 TMDB id;否则给出当前作品 id。\n"
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
    plugin_version = "3.3.2"
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
    _order_override = None
    _group_notified = None
    _emby_sync_pending = None
    _recheck_pending = None
    # 批次清单(持久化)与进程内缓存
    _manifests = None
    _batch_seen = None
    _recog_cache = None
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
            self._group_map = config.get("group_map") or ""

        # 重置失败重试计数与快速探测指纹
        self._retry_counts = {}
        self._fast_fp = None
        self._busy = threading.Lock()
        self._batch_list_cache = {}
        self._manifests = self.get_data("batch_manifest") or {}
        self._series_groups = self.get_data("series_groups") or {}
        self._order_override = None
        self._group_notified = set()
        self._emby_sync_pending = set()
        self._recheck_pending = set()
        self._batch_seen = {}
        self._recog_cache = {}
        self._tmdb_struct_cache = {}
        self._emby_cache = {}
        self._manifest_lock = threading.RLock()

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
        # 遍历所有监控目录
        for mon_path in self._dirconf.keys():
            logger.info(f"开始处理监控目录 {mon_path} ...")
            media_files = SystemUtils.list_files(Path(mon_path), settings.RMT_MEDIAEXT)
            sub_files = SystemUtils.list_files(Path(mon_path), settings.RMT_SUBEXT)
            logger.info(f"监控目录 {mon_path} 共发现 {len(media_files)} 个媒体文件、{len(sub_files)} 个字幕文件")
            # 遍历目录下所有文件（先媒体后字幕，保证字幕改名与视频一致时目标目录已就绪）
            for file_path in media_files + sub_files:
                logger.info(f"开始处理文件 {file_path} ...")
                self.__handle_file(event_path=str(file_path), mon_path=mon_path, from_reconcile=True)
        logger.info("全量同步云盘实时监控目录完成！")

    def sync_reconcile(self):
        """
        定时对账：强制刷新云端列表与挂载缓存后，扫描监控目录中未处理的媒体与字幕文件。
        仅处理配置了目的目录的监控目录；失败记录会有限次重试。
        """
        if self._busy and not self._busy.acquire(blocking=False):
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
                            logger.info(f"批次 {batch_root.name} 尚未稳定(文件仍在变化),本轮跳过 {len(group)} 个文件,"
                                        f"{mf.get('retry_in')} 秒后自动复查")
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
            if self._busy:
                self._busy.release()

    def fast_probe(self):
        """
        快速探测：低成本强刷监控目录浅层列表（顶层+一级子目录），
        指纹变化时立即触发对账。监控目录平时为空，稳态开销约为每轮1次API调用。
        """
        if not self._enabled:
            return
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

    def __vfs_refresh(self, local_path: str) -> bool:
        """
        强制刷新 rclone VFS 目录缓存（递归）
        """
        if not self._rc_fs or not self._rc_mount_prefix \
                or not str(local_path).startswith(self._rc_mount_prefix):
            return False
        rel = str(local_path)[len(self._rc_mount_prefix):].strip("/")
        if not rel:
            return False
        return self.__rc_call("vfs/refresh", {"fs": self._rc_fs, "dir": rel, "recursive": "true"})

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
        cleaned = CRC_RE.sub(" ", name or "")
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

    def __extra_verdict(self, file_path: Path, mon_path: str) -> Optional[str]:
        """
        启发式特典判定(无批次清单时的回退,也用于残留清理):① 子目录名(不含种子根目录)
        ② 清洗后文件名标记(带显式 SxxExx 的视为正片,避免剧集标题误伤);None 表示按正片处理
        """
        file_path = Path(file_path)
        try:
            rel = file_path.relative_to(Path(mon_path))
        except ValueError:
            rel = None
        sub_dirs = list(rel.parts[1:-1]) if rel and len(rel.parts) > 2 else []
        for part in sub_dirs:
            if SP_DIR_RE.search(str(part)):
                return f"特典/附加目录({part})"
        stem = self.__clean_name(file_path.stem)
        if not re.search(r"S\d{1,2}E\d{1,3}", stem, re.IGNORECASE) and SP_FILE_RE.search(stem):
            return "特典/附加文件(文件名标记)"
        return None

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

    def __get_manifest(self, batch_root: Path) -> Optional[dict]:
        """
        取批次清单:None=无法建立(回退逐文件识别);{"pending": True}=批次未稳定(文件仍在变化),本轮跳过。
        清单按批次目录持久化;文件被搬走不重算,出现新文件才重建;建立失败按次数退避重试(10 分钟起,最长 6 小时)。
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
            old = self._manifests.get(key)
            if old:
                if old.get("status") == "ok" and (old.get("digest") == digest
                                                  or set(rels) <= set((old.get("files") or {}).keys())):
                    return old
                if old.get("status") in ("failed", "no_media") and old.get("digest") == digest:
                    attempts = int(old.get("attempts") or 1)
                    if now - (old.get("time") or 0) < (600 if attempts <= 1 else 1200 if attempts == 2 else 6 * 3600):
                        return None
            if self._batch_seen is None:
                self._batch_seen = {}
            seen = self._batch_seen.get(key)
            if not seen or seen[0] != digest:
                seen = (digest, now)
                self._batch_seen[key] = seen
            settle = max(0, int(self._batch_settle or 0))
            newest = max(m[2] for m in media)
            if settle and (now - seen[1] < settle or now - newest < settle):
                # 返回剩余稳定时间,调用方据此安排复查(未来时间戳的文件按"刚出现"处理)
                remaining = settle - min(now - seen[1], max(0.0, now - newest))
                return {"pending": True, "retry_in": int(max(5, min(settle, remaining)) + 3)}
            mf = self.__build_manifest(batch_root, listing, digest)
            mf["attempts"] = (int(old.get("attempts") or 0) if old and old.get("digest") == digest else 0) + 1
            if self._order_override:
                # preview API with test switches (ignore_emby/force_default): never persist or reuse
                self._manifests.pop(key, None)
            else:
                self._manifests[key] = mf
                self.__save_manifests()
            return mf if mf.get("status") == "ok" else None

    def __schedule_batch_recheck(self, batch_root: Path, mon_path: str, delay: int):
        """批次未稳定时,在稳定窗口过后自动复查该批次(同一批次只挂一个复查),不必等下一次定时对账"""
        key = str(batch_root)
        if self._recheck_pending is None:
            self._recheck_pending = set()
        if key in self._recheck_pending:
            return
        self._recheck_pending.add(key)

        def _run():
            acquired = False
            try:
                if self._busy:
                    acquired = self._busy.acquire(timeout=600)
                    if not acquired:
                        logger.warn(f"批次 {batch_root.name} 复查等待对账锁超时,留待下次对账")
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
                    logger.info(f"批次 {batch_root.name} 复查时仍未稳定,{mf.get('retry_in')} 秒后再查")
                    self.__schedule_batch_recheck(batch_root, mon_path, int(mf.get("retry_in") or 60))
                    return
                logger.info(f"批次 {batch_root.name} 已稳定,开始整理 {len(files)} 个文件")
                for file_path in self.__order_batch(mf, batch_root, files) if not batch_root.is_file() else files:
                    self.__handle_file(event_path=str(file_path), mon_path=mon_path, from_reconcile=True)
            except Exception as e:
                logger.error(f"批次 {batch_root.name} 复查失败:{str(e)} - {traceback.format_exc()}")
            finally:
                self._recheck_pending.discard(key)
                if acquired:
                    self._busy.release()

        timer = threading.Timer(max(5, int(delay)), _run)
        timer.daemon = True
        timer.start()

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
        media = listing["media"]
        rels = [m[0] for m in media]
        mf = {"digest": digest, "time": time.time(), "status": "failed", "files": {}, "media": None,
              "how": "", "unknown": []}
        try:
            parsed: Dict[str, dict] = {}
            for rel, size, _ in media:
                p = Path(rel)
                cleaned = self.__clean_name(p.stem) or p.stem
                meta = MetaInfo(title=cleaned)
                marker = None
                for part in p.parts[:-1]:
                    if SP_DIR_RE.search(str(part)):
                        marker = f"目录 {part}"
                        break
                if not marker and not re.search(r"S\d{1,2}E\d{1,3}", cleaned, re.IGNORECASE) \
                        and SP_FILE_RE.search(cleaned):
                    marker = "文件名标记"
                season = meta.begin_season
                if season is not None and not EXPLICIT_SEASON_RE.search(cleaned):
                    # 解析器凭空给出的季号(如 "[4th - 08]" -> S01E08)不可信,按未知处理,交给下载记录/目录名/LLM
                    season = None
                parsed[rel] = {"name": meta.name or "", "season": season, "episode": meta.begin_episode,
                               "marker": marker, "size": size, "pattern": re.sub(r"\d+", "#", cleaned.lower())}
            batch_name = batch_root.stem if batch_root.is_file() else batch_root.name
            candidates = self.__series_candidates(batch_name, parsed)
            if not candidates:
                logger.warn(f"批次 {batch_name}:无法识别所属作品,回退逐文件识别")
                mf["status"] = "no_media"
                return mf
            primary = candidates[0]

            def build_structure(series: dict):
                if series["type"] != MediaType.TV.value:
                    return {"group_id": None, "source": "电影", "auto": False}, {}
                _order = self.__resolve_order(series, parsed, batch_name)
                return _order, self.__tmdb_structure(series, _order)

            order, structure = build_structure(primary)
            binding = self.__history_binding(batch_name, parsed, primary)
            if binding:
                primary["binding"] = binding
                logger.info(f"批次 {batch_name}:绑定下载记录 S{binding['season']:02d} "
                            f"{binding.get('episodes_text') or ''} <= {binding['torrent_name'][:80]}")
            files = self.__rule_mapping(batch_name, parsed, primary, structure)
            how = "规则"
            if files is None:
                if not self._llm_classify:
                    logger.warn(f"批次 {batch_name}:命名不规整且未启用 LLM 批次清单,回退逐文件识别")
                    mf["status"] = "no_media"
                    return mf
                result = self.__llm_mapping(batch_name, rels, parsed, primary, candidates, structure)
                if result is None:
                    return mf
                try:
                    alt_id = int(result.get("series_tmdbid")) if result.get("series_tmdbid") is not None else None
                except (TypeError, ValueError):
                    alt_id = None
                if alt_id and alt_id != primary["tmdbid"]:
                    alt = next((c for c in candidates if c["tmdbid"] == alt_id), None)
                    if alt:
                        logger.info(f"批次 {batch_name}:LLM 判定属于备选作品 {alt['title']} ({alt['year']}) TMDB {alt_id}")
                        primary = alt
                        order, structure = build_structure(primary)
                    else:
                        logger.warn(f"批次 {batch_name}:LLM 给出的作品 TMDB {alt_id} 不在候选内,忽略")
                files = self.__validate_mapping(result.get("items") or [], rels, parsed, primary, structure)
                how = "LLM"
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
            unknown = [r for r, v in files.items() if v.get("c") == "unknown"]
            mf["unknown"] = unknown
            if unknown:
                detail = "; ".join(f"{Path(r).name}({files[r].get('note') or '?'})" for r in unknown[:20])
                logger.warn(f"批次 {batch_name} 有 {len(unknown)} 个文件无法确定季集,已跳过待人工处理:{detail}")
                if self._notify:
                    self.post_message(mtype=NotificationType.Manual,
                                      title=f"{primary['title']} 批次有 {len(unknown)} 个文件无法确定季集,已跳过",
                                      text="\n".join(Path(r).name for r in unknown[:15]))
            return mf
        except Exception as e:
            logger.error(f"建立批次清单失败 {batch_root}:{str(e)} - {traceback.format_exc()}")
            return mf

    def __recognize_title(self, title: str):
        """按标题原生识别(不触发辅助识别),进程内缓存"""
        if self._recog_cache is None:
            self._recog_cache = {}
        if title in self._recog_cache:
            return self._recog_cache[title] or None
        mediainfo = None
        try:
            meta = MetaInfo(title=title)
            if meta.name:
                mediainfo = self.mediaChain.recognize_media(meta=meta)
        except Exception as e:
            logger.debug(f"识别 {title} 失败:{str(e)}")
        self._recog_cache[title] = mediainfo or False
        return mediainfo

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

        names = Counter(v["name"] for v in parsed.values() if v["name"] and not v["marker"])
        for name, cnt in names.most_common(6):
            mi = self.__recognize_title(name)
            if mi and getattr(mi, "tmdb_id", None):
                add(mi.tmdb_id, mi.title, mi.year, mi.type.value, cnt, "文件名", groups=mi_groups(mi))
        mi = self.__recognize_title(batch_name)
        if mi and getattr(mi, "tmdb_id", None):
            add(mi.tmdb_id, mi.title, mi.year, mi.type.value, 3, "目录名", groups=mi_groups(mi))
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
        if result:
            logger.info("批次候选作品:" + "; ".join(
                f"{c['title']} ({c['year']}) TMDB {c['tmdbid']} 票数 {c['votes']} [{'/'.join(c['src'])}]"
                for c in result[:4]))
        return result

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
    def __pick_season_group(groups: list) -> Optional[dict]:
        """在 TMDB 剧集组里挑"按季"类型(原播出/制作/电视 顺序,且 ≥2 季)的最完整一个;DVD/绝对/故事线/流媒体顺序不选"""
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
        if len(regular) == 1 and (self.__season_size(default_struct, regular[0]) >= 40 or release_season >= 2):
            pick = self.__pick_season_group(groups)
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
            batch_season = MetaInfo(title=self.__clean_name(batch_name) or batch_name).begin_season
            if batch_season is not None and not EXPLICIT_SEASON_RE.search(self.__clean_name(batch_name) or batch_name):
                batch_season = None
        except Exception:
            batch_season = None
        binding = primary.get("binding") or {}
        if binding.get("season") is not None:
            # 下载记录(订阅匹配时判定的季)优先于目录名;与文件名显式季号冲突时不猜,交给 LLM
            for v in cands.values():
                if v["season"] is not None and int(v["season"]) != int(binding["season"]):
                    logger.warn(f"批次 {batch_name}:文件名季号 S{v['season']} 与下载记录 S{binding['season']} 冲突,交给 LLM 判断")
                    return None
            batch_season = int(binding["season"])
        regular = sorted(structure.get("offsets") or {})
        mapping: Dict[str, Tuple[int, int]] = {}
        for rel, v in cands.items():
            if v["episode"] is None:
                return None
            ep = int(v["episode"])
            if v["season"] is not None:
                target = self.__season_ep_to_target(structure, int(v["season"]), ep)
            else:
                options = self.__plausible_targets(structure, None, ep) or set()
                target = None
                if batch_season is not None:
                    # 目录名给出季号:优先该季直接对应,其次按季换算(合并季目标);集号超出该季范围则按绝对集数处理
                    if (batch_season, ep) in options:
                        target = (batch_season, ep)
                    else:
                        target = self.__season_ep_to_target(structure, int(batch_season), ep)
                if target is None:
                    if len(options) == 1:
                        target = next(iter(options))
                    elif len(regular) == 1 and (regular[0], ep) in options:
                        target = (regular[0], ep)
            if not target:
                return None
            mapping[rel] = target
        if len(set(mapping.values())) != len(mapping):
            return None
        sizes = sorted(v["size"] for v in cands.values())
        median = sizes[len(sizes) // 2]
        if median and sizes[0] < 0.35 * median:
            return None
        for rel, (s, e) in mapping.items():
            note = None
            if parsed[rel]["episode"] != e or (parsed[rel]["season"] is not None and parsed[rel]["season"] != s):
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
        alts = [c for c in candidates if c["tmdbid"] != primary["tmdbid"]][:5]
        alt_text = "; ".join(f"TMDB {c['tmdbid']} {c['title']} ({c['year']}) {c['type']}" for c in alts) or "无"
        hint = f"\n提示: {primary['hint']}" if primary.get("hint") else ""
        if primary.get("binding"):
            b = primary["binding"]
            hint += (f"\n下载记录(MP 订阅匹配时已判定,可信): 该批次属于 S{b['season']:02d} {b.get('episodes_text') or ''}"
                     f",种子名 {b['torrent_name'][:100]}")
        user_msg = (f"发布目录名: {batch_name}\n当前作品: TMDB {primary['tmdbid']} {primary['title']} "
                    f"({primary['year']}) 类型:{primary['type']}{hint}\n文件列表(编号. 大小 文件名):\n{listing}\n\n"
                    f"季集结构:\n{self.__structure_text(structure)}\n\n备选作品(可能相关): {alt_text}")
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
                    pending, failed = [], []
                    for f in files:
                        suffix = f.suffix.lower()
                        if suffix not in media_exts and suffix not in sub_exts:
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
                        entry = self.__manifest_entry(mf, child, f) if mf and mf.get("status") == "ok" else None
                        if entry:
                            if entry.get("c") in ("extra", "other"):
                                continue
                            pending.append(f)
                            continue
                        if self.__extra_verdict(f, mon_path):
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

    def __handle_file(self, event_path: str, mon_path: str, from_reconcile: bool = False):
        """
        同步一个文件
        :param event_path: 事件文件路径
        :param mon_path: 监控目录
        :param from_reconcile: 是否来自定时对账（失败记录允许有限次重试）
        """
        file_path = Path(event_path)
        try:
            if not file_path.exists():
                return
            # 全程加锁
            with lock:
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
                if file_path.suffix not in settings.RMT_MEDIAEXT and not is_subtitle:
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
                    if manifest and manifest.get("pending"):
                        logger.debug(f"批次 {manifest_root.name} 尚未稳定,本轮跳过:{file_path.name},"
                                     f"{manifest.get('retry_in')} 秒后自动复查")
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
                    # 通知Strm助手生成
                    logger.info(f"{file_path} 直接通知strm助手生成strm!")
                    self.eventmanager.send_event(EventType.PluginAction, {
                        'file_path': str(file_path),
                        'action': 'cloudstrm_file'
                    })
                    return

                # 查询转移方式
                transfer_type = self._transferconf.get(mon_path)

                # 查找这个文件项
                file_item = self.storagechain.get_file_item(storage="local", path=file_path)
                if not file_item:
                    logger.warn(f"{event_path.name} 未找到对应的文件")
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

                # 转移文件
                transferinfo: TransferInfo = self.chain.transfer(fileitem=file_item,
                                                                 meta=file_meta,
                                                                 mediainfo=mediainfo,
                                                                 target_directory=target_dir,
                                                                 episodes_info=episodes_info)

                if not transferinfo:
                    logger.error("文件转移模块运行失败")
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

        except Exception as e:
            logger.error("目录监控发生错误：%s - %s" % (str(e), traceback.format_exc()))

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
        batch_root = Path(path)
        if not batch_root.exists():
            return schemas.Response(success=False, message=f"路径不存在:{path}")
        self._order_override = {"ignore_emby": bool(ignore_emby), "force_default": bool(force_default)} \
            if (ignore_emby or force_default) else None
        key = str(batch_root)
        if rebuild:
            if self._manifests:
                self._manifests.pop(key, None)
            if self._batch_seen:
                self._batch_seen.pop(key, None)
            if self._batch_list_cache:
                self._batch_list_cache.pop(key, None)
        saved = self._batch_settle
        self._batch_settle = 0
        try:
            mf = self.__get_manifest(batch_root)
        finally:
            self._batch_settle = saved
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
            "group_map": ""
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
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                self._event.set()
                self._scheduler.shutdown()
                self._event.clear()
            self._scheduler = None
