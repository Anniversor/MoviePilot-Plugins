# -*- coding: utf-8 -*-
"""
PanSou索引:将本地 pansou 聚合搜索的 BT(磁力)结果接入 MoviePilot 内建搜索。

原理(与 SeedHub索引 同款机制):
- SitesHelper().add_indexer() 注册索引定义 + 站点管理表记录(索引池 = 两者交集),
  认证站点加载晚于插件初始化,用延迟定时器注册并回读确认;
- get_module() 提供 search_torrents 插件模块,搜索结果与系统模块合并;
- 数据源为 pansou /api/search(res=merge),仅取 merged_by_type.magnet(BT 磁力),
  网盘链接与 ed2k 不采集(MP 下载链对非 magnet enclosure 按 HTTP 抓种子,ed2k 不可用);
- 磁力天然被 115离线下载器 的公开资源认领规则接住,无需额外配置。
"""
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.core.config import settings
from app.helper.sites import SitesHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import MediaType

# 搜索链路使用 app.core.context.TorrentInfo(dataclass,带 to_dict);
# 部分版本迁移到 app.schemas(pydantic),做兼容导入
try:
    from app.core.context import TorrentInfo
except ImportError:
    from app.schemas import TorrentInfo


class PanSouIndexer(_PluginBase):
    # 插件名称
    plugin_name = "PanSou索引"
    # 插件描述
    plugin_desc = "将 pansou 聚合搜索的磁力资源接入内建搜索，作为公开 BT 兜底资源源。"
    # 插件图标
    plugin_icon = "spider.png"
    # 插件版本
    plugin_version = "1.2.0"
    # 插件作者
    plugin_author = "Anniversor"
    # 作者主页
    author_url = "https://github.com/Anniversor"
    # 插件配置项ID前缀
    plugin_config_prefix = "pansouindexer_"
    # 加载顺序
    plugin_order = 21
    # 可使用的用户级别
    auth_level = 1

    # 站点标识(索引定义 id 占位;合并后真实 id 为站点表自增 id)
    SITE_ID = 92102
    SITE_NAME = "PanSou"
    DOMAIN_KEY = "pansou.local"

    UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

    # 私有属性
    _enabled = False
    _pansou_url = "http://127.0.0.1:18050"
    _src = "all"
    _max_results = 50
    _timeout = 30
    _probe_size = True
    _probe_limit = 25
    _registered = False

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = bool(config.get("enabled"))
            self._pansou_url = (config.get("pansou_url") or "http://127.0.0.1:18050").strip().rstrip("/")
            self._src = (config.get("src") or "all").strip() or "all"
            self._max_results = self.__to_int(config.get("max_results"), 50, 5, 200)
            self._timeout = self.__to_int(config.get("timeout"), 30, 5, 120)
            self._probe_size = bool(config.get("probe_size", True))
            self._probe_limit = self.__to_int(config.get("probe_limit"), 25, 0, 50)

        # 磁力元数据缓存(btih -> {size,name,count},磁力不可变可永久复用)
        self._meta_cache: Dict[str, dict] = self.get_data("meta_cache") or {}
        self._meta_lock = threading.Lock()
        self._probe_failed = set()
        self._probe_cooldown = 0.0
        # 已知空关键词(keyword -> 失效时间),避免重复梯度重查
        self._empty_kw: Dict[str, float] = {}

        self.__cancel_ensure_timer()
        if self._enabled:
            self._ensure_attempts = 0
            self.__schedule_ensure(delay=15)

    @staticmethod
    def __to_int(value, default, lo, hi):
        try:
            return max(lo, min(hi, int(str(value).strip())))
        except (TypeError, ValueError):
            return default

    # region 站点注册(索引池 = 索引定义 ∩ 站点管理表)

    def __build_indexer(self) -> dict:
        return {
            "id": self.SITE_ID,
            "name": self.SITE_NAME,
            "domain": f"https://{self.DOMAIN_KEY}/",
            "url": f"https://{self.DOMAIN_KEY}/",
            "encoding": "UTF-8",
            "public": True,
            "proxy": False,
            "language": "zh",
            "ua": self.UA,
        }

    def __cancel_ensure_timer(self):
        timer = getattr(self, "_ensure_timer", None)
        if timer:
            try:
                timer.cancel()
            except Exception:
                pass
            self._ensure_timer = None

    def __schedule_ensure(self, delay: int):
        self.__cancel_ensure_timer()
        timer = threading.Timer(delay, self.__ensure_registered)
        timer.daemon = True
        self._ensure_timer = timer
        timer.start()

    def __pool_entry(self, helper) -> Optional[dict]:
        for indexer in (helper.get_indexers() or []):
            dom = str(indexer.get("domain") or indexer.get("url") or "").lower()
            if self.DOMAIN_KEY in dom:
                return indexer
        return None

    def __ensure_site_row(self) -> bool:
        try:
            from app.db.site_oper import SiteOper
            oper = SiteOper()
            if oper.exists(self.DOMAIN_KEY):
                return True
            ok, msg = oper.add(
                name=self.SITE_NAME,
                url=f"https://{self.DOMAIN_KEY}/",
                domain=self.DOMAIN_KEY,
                ua=self.UA,
                pri=22,
                rss="",
                cookie="",
                public=1,
                proxy=0,
                render=0,
                timeout=self._timeout,
            )
            logger.info(f"PanSou 站点管理记录创建:{ok} {msg}")
            return ok or "已存在" in (msg or "")
        except Exception as e:
            logger.error(f"PanSou 站点管理记录创建出错:{str(e)}")
            return False

    def __ensure_registered(self):
        try:
            helper = SitesHelper()
            pool = helper.get_indexers() or []
            if not pool:
                logger.info("PanSou 索引池尚未就绪(认证加载未完成),稍后重试注册...")
                self.__retry_ensure()
                return
            entry = self.__pool_entry(helper)
            if entry:
                self._registered = True
                logger.info(f"PanSou 已在搜索索引池:id={entry.get('id')}")
                return
            self.__ensure_site_row()
            ret = helper.add_indexer(self.DOMAIN_KEY, self.__build_indexer())
            entry = self.__pool_entry(helper)
            self._registered = entry is not None
            logger.info(f"PanSou 注册:add_indexer={ret},回读在列={self._registered}"
                        f"{',id=' + str(entry.get('id')) if entry else ''}")
            if not self._registered:
                self.__retry_ensure()
        except Exception as e:
            logger.error(f"PanSou 注册确认出错:{str(e)}")
            self.__retry_ensure()

    def __retry_ensure(self):
        self._ensure_attempts = getattr(self, "_ensure_attempts", 0) + 1
        if self._ensure_attempts < 20:
            self.__schedule_ensure(delay=30)
        else:
            logger.error("PanSou 多次注册后仍未进入索引池,放弃。")

    # endregion

    # region 插件模块(搜索入口)

    def get_module(self) -> Dict[str, Any]:
        return {
            "search_torrents": self.search_torrents,
            "async_search_torrents": self.search_torrents,
        }

    def search_torrents(self, site: dict = None, keyword=None,
                        mtype: MediaType = None, cat: Optional[str] = None,
                        page: Optional[int] = 0, **kwargs) -> Optional[List[TorrentInfo]]:
        if not self.get_state():
            return None
        if not site:
            return None
        site_domain = str(site.get("domain") or site.get("url") or "").lower()
        if self.DOMAIN_KEY not in site_domain:
            return None
        if page:
            return []
        keywords = [k for k in (keyword if isinstance(keyword, list) else [keyword]) if k]
        if not keywords:
            # 浏览模式(最新种子)不支持
            return []
        # 链路按关键词轮询且一有结果就停,英文原名往往轮不到;而聚合源对
        # 中文官方译名常返回裸标题垃圾,真资源多为英文 scene 命名。
        # 媒体搜索场景(mtype 已知)下反查识别缓存补充英文原名一起搜。
        media_year = None
        if mtype:
            keywords, media_year = self.__enrich_keywords(keywords)
        try:
            # 所有关键词并行搜(串行冷查每个都顶满 pansou 10 秒异步窗口,
            # 并行后墙钟时间 = 单查耗时);0 结果的关键词在各自线程内等
            # 4 秒重查一次(pansou 异步后台已填缓存),不拖累其他关键词
            use_keywords = [str(k) for k in keywords[:3]]
            with ThreadPoolExecutor(max_workers=len(use_keywords)) as executor:
                per_kw = list(executor.map(
                    lambda kw: self.__search_with_retry(site, kw), use_keywords))
            merged = self.__merge_round_robin(per_kw)
            # 磁力元数据探测:补大小 + 真实种子名替换聚合标题;
            # 单轮、带硬时限,电影场景只探标题带年份的可匹配条目
            if self._probe_size and self._probe_limit:
                movie_year = media_year if mtype == MediaType.MOVIE else None
                self.__enrich_metadata(merged, movie_year=movie_year)
            return merged
        except Exception as e:
            logger.error(f"PanSou 搜索出错:{str(e)}")
            return []

    @staticmethod
    def __btih_of(magnet: str) -> Optional[str]:
        m = re.search(r"urn:btih:([0-9a-fA-F]{40}|[A-Za-z2-7]{32})", magnet or "")
        return m.group(1).lower() if m else None

    def __probe_magnet(self, btih: str) -> Tuple[Optional[dict], bool]:
        """
        经 whatslink.info 探测磁力元数据(大小/真实名/文件数)。
        返回 (元数据, 是否临时失败)——限流/超时是临时失败,不应拉黑。
        """
        try:
            r = requests.get("https://whatslink.info/api/v1/link",
                             params={"url": f"magnet:?xt=urn:btih:{btih}"},
                             headers={"User-Agent": self.UA}, timeout=6)
            if r.status_code != 200:
                return None, True
            d = r.json() or {}
            if d.get("error") or not d.get("size"):
                return None, False
            return {"size": d.get("size") or 0,
                    "name": (d.get("name") or "").strip(),
                    "count": d.get("count") or 0}, False
        except Exception:
            return None, True

    def __apply_meta(self, torrent: TorrentInfo, info: dict):
        torrent.size = float(info.get("size") or 0)
        name = info.get("name")
        if name and len(name) >= 8:
            # 原聚合标题挪到副标题(常含中文名,还能吃到 MP 的副标题匹配)
            torrent.description = f"{torrent.title} | {torrent.description or ''}".strip(" |")
            torrent.title = name

    def __enrich_metadata(self, torrents: List[TorrentInfo], movie_year=None):
        """
        为无大小的结果补磁力元数据:缓存优先,未命中的并发探测。
        单轮执行 + 10 秒硬时限;电影场景只探标题带年份±1 的条目
        (其余过不了 MP 电影匹配的年份规则,探了也不会展示)。
        """
        try:
            candidates = []
            for t in torrents:
                btih = self.__btih_of(t.enclosure)
                if not btih:
                    continue
                hit = self._meta_cache.get(btih)
                if hit:
                    self.__apply_meta(t, hit)
                elif (t.size or 0) <= 0 and btih not in self._probe_failed:
                    candidates.append((t, btih))
            # 探测服务限流冷却期:只吃缓存,不发新探测
            if time.monotonic() < getattr(self, "_probe_cooldown", 0):
                return
            if movie_year:
                try:
                    y = int(movie_year)
                    years = {str(y - 1), str(y), str(y + 1)}
                    candidates = [c for c in candidates
                                  if any(yy in (c[0].title or "") for yy in years)]
                except (TypeError, ValueError):
                    pass
            else:
                # 标题带年份的优先(更可能是可匹配的真资源)
                candidates.sort(key=lambda x: 0 if re.search(r"\b(19|20)\d{2}\b", x[0].title or "") else 1)
            pending = candidates[:self._probe_limit]
            if not pending:
                return
            ok = 0
            temp_fails = 0
            executor = ThreadPoolExecutor(max_workers=6)
            futures = {executor.submit(self.__probe_magnet, btih): (t, btih)
                       for t, btih in pending}
            try:
                for future in as_completed(futures, timeout=10):
                    t, btih = futures[future]
                    info, temporary = None, False
                    try:
                        info, temporary = future.result()
                    except Exception:
                        temporary = True
                    if info:
                        ok += 1
                        with self._meta_lock:
                            self._meta_cache[btih] = info
                        self.__apply_meta(t, info)
                    elif temporary:
                        # 限流/超时:不拉黑,下次搜索再探
                        temp_fails += 1
                    else:
                        self._probe_failed.add(btih)
            except Exception:
                # 到达硬时限:未完成的探测放弃(不计失败,下次搜索续探)
                pass
            finally:
                executor.shutdown(wait=False, cancel_futures=True)
            if temp_fails >= 3 and ok == 0:
                # 探测服务疑似限流,冷却 10 分钟避免空耗搜索时长
                self._probe_cooldown = time.monotonic() + 600
                logger.warn("PanSou 元数据探测服务疑似限流,冷却 10 分钟")
            with self._meta_lock:
                if len(self._meta_cache) > 8000:
                    # 粗略瘦身,防止无限膨胀
                    keys = list(self._meta_cache.keys())[:4000]
                    for k in keys:
                        self._meta_cache.pop(k, None)
                self.save_data("meta_cache", self._meta_cache)
            logger.info(f"PanSou 磁力元数据探测:{ok}/{len(pending)} 命中")
        except Exception as e:
            logger.warn(f"PanSou 元数据探测出错:{str(e)}")

    @staticmethod
    def __enrich_keywords(keywords: List[str]) -> Tuple[List[str], Optional[str]]:
        """
        反查识别缓存,为中文关键词补充英文原名(精确搜索场景识别刚发生过,
        TMDB 缓存必命中,不会触发网络/AI 识别)。返回 (关键词列表, 媒体年份)。
        """
        media_year = None
        try:
            from app.chain.media import MediaChain
            from app.core.metainfo import MetaInfo as _MetaInfo
            media = MediaChain().recognize_by_meta(_MetaInfo(keywords[0]))
            if media:
                media_year = getattr(media, "year", None)
                # 英文原名(BT 真资源多为英文 scene 命名)
                extras = [getattr(media, "original_title", None),
                          getattr(media, "en_title", None)]
                # 一个中文别名(如"魔戒3:王者归来"——中文圈资源常用别名命名)
                for name in (getattr(media, "names", None) or []):
                    if name and re.search(r"[一-鿿]", str(name)) \
                            and str(name) not in keywords:
                        extras.append(str(name))
                        break
                for extra in extras:
                    if extra and extra not in keywords:
                        keywords.append(extra)
        except Exception as e:
            logger.debug(f"PanSou 关键词补充失败:{str(e)}")
        return keywords, media_year

    def __search_with_retry(self, site: dict, keyword: str) -> List[TorrentInfo]:
        results = self.__do_search(site, keyword)
        if results:
            self._empty_kw.pop(keyword, None)
            return results
        # 已知空关键词(10分钟内验证过)不再重查,免得每次搜索空耗等待
        if time.monotonic() < self._empty_kw.get(keyword, 0):
            return results
        # pansou 异步模型:全新关键词秒回空结果,后台聚合需 10-30 秒;
        # 梯度重查直到出结果或预算耗尽,保证一次搜索拿到完整结果
        for delay in (4, 6, 8):
            time.sleep(delay)
            results = self.__do_search(site, keyword)
            if results:
                return results
        self._empty_kw[keyword] = time.monotonic() + 600
        return results

    def __merge_round_robin(self, per_kw: List[List[TorrentInfo]]) -> List[TorrentInfo]:
        """
        轮转合并各关键词结果并按 btih 跨关键词去重,截断到结果上限。
        """
        merged: List[TorrentInfo] = []
        seen = set()
        idx = 0
        while len(merged) < self._max_results:
            found = False
            for lst in per_kw:
                if idx < len(lst):
                    found = True
                    t = lst[idx]
                    key = self.__btih_of(t.enclosure) or t.enclosure
                    if key not in seen:
                        seen.add(key)
                        merged.append(t)
                        if len(merged) >= self._max_results:
                            break
            if not found:
                break
            idx += 1
        return merged

    def __do_search(self, site: dict, keyword: str, seen: set = None) -> List[TorrentInfo]:
        if seen is None:
            seen = set()
        try:
            # 超时压到 15 秒:pansou 对全新关键词的冷查询(TG 同步抓取)可能
            # 拖 20-30 秒,但其后台会继续抓取并写缓存,超时放弃后重查即可
            # 低成本拿到结果,不值得全程陪跑
            res = requests.get(
                f"{self._pansou_url}/api/search",
                params={"kw": keyword, "res": "merge", "src": self._src},
                headers={"User-Agent": self.UA},
                timeout=min(self._timeout, 15),
            )
        except Exception as e:
            logger.warn(f"PanSou 请求失败:{str(e)}")
            return []
        if res.status_code != 200:
            logger.warn(f"PanSou 请求失败:HTTP {res.status_code}")
            return []
        data = res.json() or {}
        if data.get("code") != 0:
            logger.warn(f"PanSou 返回异常:{data.get('code')} {data.get('message')}")
            return []
        magnets = ((data.get("data") or {}).get("merged_by_type") or {}).get("magnet") or []
        if not magnets:
            logger.info(f"PanSou 搜索无磁力结果:{keyword}")
            return []

        torrents = []
        for m in magnets:
            url = (m.get("url") or "").strip()
            if not url.startswith("magnet:"):
                continue
            # 按 btih 去重(同一资源常被多个聚合源收录)
            btih_m = re.search(r"urn:btih:([0-9a-fA-F]{40}|[A-Za-z2-7]{32})", url)
            key = btih_m.group(1).lower() if btih_m else url
            if key in seen:
                continue
            seen.add(key)
            note = (m.get("note") or "").strip()
            # 回声垃圾过滤:note 就是搜索词本身的条目(聚合站 SEO spam,
            # 无年份无画质信息,MP 电影匹配的年份规则也必拒)
            if note == keyword.strip():
                continue
            title, size = self.__parse_note(note, url)
            torrents.append(TorrentInfo(
                site=site.get("id"),
                site_name=site.get("name") or self.SITE_NAME,
                site_ua=self.UA,
                site_proxy=False,
                site_order=site.get("pri") or 0,
                site_downloader=site.get("downloader"),
                title=title,
                description=str(m.get("source") or ""),
                enclosure=url,
                page_url=None,
                size=size,
                seeders=0,
                peers=0,
                grabs=0,
                pubdate=self.__parse_datetime(m.get("datetime")),
                uploadvolumefactor=1.0,
                downloadvolumefactor=0.0,
            ))
            if len(torrents) >= self._max_results:
                break
        logger.info(f"PanSou 搜索 {keyword} 返回 {len(torrents)} 条磁力(原始 {len(magnets)} 条)")
        return torrents

    @staticmethod
    def __parse_note(note: str, magnet: str) -> Tuple[str, float]:
        """
        从 note 提取标题与大小(尾部 [17.9G] 标记),缺失时回退磁力 dn 参数。
        """
        title = note
        size = 0.0
        m = re.search(r"\[([\d.]+)\s*([KMGTP])B?]\s*$", note, re.I)
        if m:
            mult = {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3,
                    "T": 1024 ** 4, "P": 1024 ** 5}[m.group(2).upper()]
            try:
                size = float(m.group(1)) * mult
            except ValueError:
                size = 0.0
            title = note[:m.start()].strip()
        if not title:
            dn = re.search(r"[?&]dn=([^&]+)", magnet)
            if dn:
                try:
                    from urllib.parse import unquote_plus
                    title = unquote_plus(dn.group(1))
                except Exception:
                    title = dn.group(1)
        return title or "未知资源", size

    @staticmethod
    def __parse_datetime(value) -> Optional[str]:
        if not value:
            return None
        m = re.match(r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}(?::\d{2})?)", str(value))
        if not m:
            return None
        t = m.group(2)
        if len(t) == 5:
            t += ":00"
        return f"{m.group(1)} {t}"

    # endregion

    # region 插件框架接口

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        return []

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
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'enabled', 'label': '启用插件'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 5},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'pansou_url', 'label': 'PanSou 地址',
                                              'placeholder': 'http://127.0.0.1:18050'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 2},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'max_results', 'label': '结果上限',
                                              'placeholder': '50'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 2},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'timeout', 'label': '超时(秒)',
                                              'placeholder': '30'}
                                }]
                            },
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSelect',
                                    'props': {'model': 'src', 'label': '数据来源',
                                              'items': [
                                                  {'title': '全部(TG+插件)', 'value': 'all'},
                                                  {'title': '仅TG频道', 'value': 'tg'},
                                                  {'title': '仅插件', 'value': 'plugin'},
                                              ]}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'probe_size',
                                              'label': '磁力元数据探测'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'probe_limit',
                                              'label': '单次探测上限', 'placeholder': '25'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VAlert',
                                    'props': {
                                        'type': 'info',
                                        'variant': 'tonal',
                                        'text': '仅采集 pansou 聚合结果中的磁力(BT)资源,自动被 115离线下载器 认领。'
                                                '磁力元数据探测(whatslink.info)补全大小并以真实种子名替换聚合标题,'
                                                '显著提高媒体匹配率;结果按 btih 永久缓存,重复搜索零开销。'
                                    }
                                }]
                            },
                        ]
                    },
                ]
            }
        ], {
            "enabled": False,
            "pansou_url": "http://127.0.0.1:18050",
            "src": "all",
            "max_results": 50,
            "timeout": 30,
            "probe_size": True,
            "probe_limit": 25,
        }

    def get_page(self) -> List[dict]:
        return []

    def stop_service(self):
        self.__cancel_ensure_timer()

    # endregion
