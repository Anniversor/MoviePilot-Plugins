# -*- coding: utf-8 -*-
"""
SeedHub索引:将 SeedHub(sidhub.cc)接入 MoviePilot 内建搜索。

原理:
- 通过 SitesHelper().add_indexer() 将 SeedHub 注册进搜索站点池(出现在搜索站点范围中);
- 通过 get_module() 提供 search_torrents 插件模块,搜索时对本站点返回结果,
  与系统模块结果合并进入统一搜索结果流;
- SeedHub 为两级结构(搜索页 -> 作品页 -> 磁力列表),磁力链接以 base64 形式
  内嵌在 /link_start/ 中转页脚本中,插件逐条解析并持久缓存(seed_id 与磁力不变);
- 站点存在换域名历史,自带 /worker-json-hosts/ 镜像接口,插件每日巡检自动切换。
"""
import base64
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.helper.sites import SitesHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import TorrentInfo
from app.schemas.types import MediaType


class SeedHubIndexer(_PluginBase):
    # 插件名称
    plugin_name = "SeedHub索引"
    # 插件描述
    plugin_desc = "将 SeedHub 接入内建搜索，公开磁力资源进入统一搜索结果，支持镜像域名自动切换。"
    # 插件图标
    plugin_icon = "https://sidhub.cc/favicon.ico"
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "Anniversor"
    # 作者主页
    author_url = "https://github.com/Anniversor"
    # 插件配置项ID前缀
    plugin_config_prefix = "seedhubindexer_"
    # 加载顺序
    plugin_order = 21
    # 可使用的用户级别
    auth_level = 1

    # 站点标识(注册进索引池;TorrentInfo.site 为 int 型,须用数字)
    SITE_ID = 92101
    SITE_NAME = "SeedHub"

    # 默认镜像(与站点 /worker-json-hosts/ 保持一致)
    DEFAULT_MIRRORS = [
        "https://sidhub.cc",
        "https://seedog.cc",
        "https://seeduck.cc",
        "https://hubdog.cc",
    ]

    UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

    # 私有属性
    _enabled = False
    _proxy = False
    _domain = "https://sidhub.cc"
    _max_movies = 5
    _max_seeds = 20
    _resolve_budget = 30
    _timeout = 10
    _registered = False

    def __init__(self):
        super().__init__()
        self._magnet_cache: Dict[str, str] = {}
        self._cache_lock = threading.Lock()

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = bool(config.get("enabled"))
            self._proxy = bool(config.get("proxy"))
            self._domain = (config.get("domain") or "https://sidhub.cc").strip().rstrip("/")
            if not self._domain.startswith("http"):
                self._domain = f"https://{self._domain}"
            self._max_movies = self.__to_int(config.get("max_movies"), 5, 1, 20)
            self._max_seeds = self.__to_int(config.get("max_seeds"), 20, 1, 100)
            self._resolve_budget = self.__to_int(config.get("resolve_budget"), 30, 1, 100)
            self._timeout = self.__to_int(config.get("timeout"), 10, 3, 60)

        # 磁力缓存(seed_id -> magnet,磁力不变可长期复用)
        self._magnet_cache = self.get_data("magnet_map") or {}

        if self._enabled:
            self.__register_indexer()

    @staticmethod
    def __to_int(value, default, lo, hi):
        try:
            return max(lo, min(hi, int(str(value).strip())))
        except (TypeError, ValueError):
            return default

    # region 站点注册

    def __build_indexer(self) -> dict:
        """
        构造注册用索引器定义。不含 search/torrents 配置,
        系统通用爬虫对其空转返回 [],结果完全由本插件模块提供。
        """
        return {
            "id": self.SITE_ID,
            "name": self.SITE_NAME,
            "domain": f"{self._domain}/",
            "url": f"{self._domain}/",
            "encoding": "UTF-8",
            "public": True,
            "proxy": self._proxy,
            "language": "zh",
            "ua": self.UA,
        }

    def __register_indexer(self):
        try:
            domain_key = re.sub(r"^https?://", "", self._domain).strip("/")
            ret = SitesHelper().add_indexer(domain_key, self.__build_indexer())
            self._registered = bool(ret)
            logger.info(f"SeedHub 索引站点注册{'成功' if ret else '失败'}:{domain_key} (id={self.SITE_ID})")
        except Exception as e:
            self._registered = False
            logger.error(f"SeedHub 索引站点注册出错:{str(e)}")

    # endregion

    # region 插件模块(搜索入口)

    def get_module(self) -> Dict[str, Any]:
        """
        声明插件模块:同步与异步搜索共用同一实现(异步链路会自动切线程池)。
        """
        return {
            "search_torrents": self.search_torrents,
            "async_search_torrents": self.search_torrents,
        }

    def search_torrents(self, site: dict = None, keyword=None,
                        mtype: MediaType = None, cat: Optional[str] = None,
                        page: Optional[int] = 0, **kwargs) -> Optional[List[TorrentInfo]]:
        """
        插件搜索模块:仅处理本站点,其他站点返回 None 放行系统模块。
        """
        if not self.get_state():
            return None
        if not site or site.get("id") != self.SITE_ID:
            return None
        # 域名巡检切换后,旧注册项残留在索引池中,按域名过滤避免重复搜索
        if (site.get("domain") or "").rstrip("/") != self._domain:
            return []
        if page:
            return []
        # 关键词可能为列表(批量),取前两个依次尝试
        keywords = [k for k in (keyword if isinstance(keyword, list) else [keyword]) if k]
        if not keywords:
            # 浏览模式(无关键词)不支持
            return []
        try:
            for kw in keywords[:2]:
                results = self.__do_search(str(kw), mtype)
                if results:
                    return results
            return []
        except Exception as e:
            logger.error(f"SeedHub 搜索出错:{str(e)}")
            return []

    # endregion

    # region 站点抓取

    def __request(self, url: str):
        from app.utils.http import RequestUtils
        return RequestUtils(
            ua=self.UA,
            timeout=self._timeout,
            referer=f"{self._domain}/",
            proxies=settings.PROXY if self._proxy else None
        ).get_res(url, allow_redirects=True)

    def __do_search(self, keyword: str, mtype: MediaType = None) -> List[TorrentInfo]:
        # 1. 搜索页:取作品条目
        search_url = f"{self._domain}/s/{quote(keyword, safe='')}/"
        res = self.__request(search_url)
        if res is None or res.status_code != 200:
            logger.warn(f"SeedHub 搜索页请求失败:{search_url} "
                        f"status={res.status_code if res is not None else None}")
            return []
        movies = self.__parse_search_page(res.text)
        if not movies:
            logger.info(f"SeedHub 搜索无结果:{keyword}")
            return []

        # 2. 作品页:取磁力行
        rows: List[dict] = []
        for mid, mtitle in movies[:self._max_movies]:
            try:
                rows.extend(self.__fetch_movie_rows(mid, mtitle, mtype))
            except Exception as e:
                logger.warn(f"SeedHub 作品页解析失败 movies/{mid}:{str(e)}")

        if not rows:
            return []

        # 3. 解析磁力(带持久缓存与单次预算)
        resolved, cache_hits, fetched, skipped = self.__resolve_rows(rows)

        # 4. 构造 TorrentInfo
        torrents = []
        for row in resolved:
            torrents.append(TorrentInfo(
                site=self.SITE_ID,
                site_name=self.SITE_NAME,
                site_ua=self.UA,
                site_proxy=self._proxy,
                site_order=0,
                title=row["title"],
                description=row["description"],
                enclosure=row["magnet"],
                page_url=row["page_url"],
                size=row["size"],
                seeders=0,
                peers=0,
                grabs=0,
                pubdate=row["pubdate"],
                uploadvolumefactor=1.0,
                downloadvolumefactor=0.0,
            ))
        logger.info(f"SeedHub 搜索 {keyword} 返回 {len(torrents)} 条"
                    f"(缓存命中 {cache_hits},新解析 {fetched},超预算跳过 {skipped})")
        return torrents

    def __parse_search_page(self, html: str) -> List[Tuple[str, str]]:
        """
        解析搜索页,返回 [(movie_id, 标题)],按页面顺序去重。
        """
        items: List[Tuple[str, str]] = []
        seen = set()
        for m in re.finditer(r'<a[^>]+href="/movies/(\d+)/"[^>]*>(.*?)</a>', html, re.S):
            mid = m.group(1)
            text = re.sub(r"<[^>]+>", "", m.group(2)).strip()
            if mid in seen:
                continue
            if not text or text == "#":
                # 海报/锚点链接,标题在同 id 的另一个链接上
                continue
            seen.add(mid)
            items.append((mid, text))
        # 仅有海报链接没抓到标题的兜底
        if not items:
            for m in re.finditer(r'href="/movies/(\d+)/"', html):
                mid = m.group(1)
                if mid not in seen:
                    seen.add(mid)
                    items.append((mid, ""))
        return items

    def __fetch_movie_rows(self, mid: str, mtitle: str, mtype: MediaType = None) -> List[dict]:
        """
        拉取作品页磁力列表,返回未解析磁力的行信息。
        """
        page_url = f"{self._domain}/movies/{mid}/"
        res = self.__request(page_url)
        if res is None or res.status_code != 200:
            logger.warn(f"SeedHub 作品页请求失败:{page_url}")
            return []
        html = res.text

        # 站内分类:1=电影 2=动漫 3=剧集;动漫两类均可能,仅排除明确不符的
        cat_m = re.search(r'href="/categories/(\d)/types/', html)
        cat_id = int(cat_m.group(1)) if cat_m else 0
        if mtype == MediaType.MOVIE and cat_id == 3:
            return []
        if mtype == MediaType.TV and cat_id == 1:
            return []

        # 磁力列表限定在第一个 seeds 列表内(其后的 pan-links 为网盘,不采集)
        block_m = re.search(r'<ul class="seeds".*?</ul>', html, re.S)
        if not block_m:
            return []

        rows = []
        for li in re.finditer(r"<li>(.*?)</li>", block_m.group(0), re.S):
            chunk = li.group(1)
            sid_m = re.search(r"seed_id=(\d+)", chunk)
            title_m = re.search(r'title="([^"]+)"', chunk)
            if not sid_m or not title_m:
                continue
            raw_title = title_m.group(1).strip()
            # 去掉标题尾部的 [62G] 大小标记
            clean_title = re.sub(r"\s*\[[^\[\]]*]$", "", raw_title).strip()
            size_m = re.search(r'<code class="size">([^<]+)</code>', chunk)
            feats = re.findall(r'<code class="seed-feature">([^<]+)</code>', chunk)
            time_m = re.search(r'<span class="create-time"[^>]*>([^<]+)</span>', chunk)
            pubdate = (time_m.group(1).strip() + ":00") if time_m else None
            rows.append({
                "seed_id": sid_m.group(1),
                "title": clean_title,
                "description": " ".join(x for x in [mtitle] + feats if x),
                "size": self.__parse_size(size_m.group(1) if size_m else ""),
                "pubdate": pubdate,
                "page_url": page_url,
            })
            if len(rows) >= self._max_seeds:
                break
        return rows

    @staticmethod
    def __parse_size(text: str) -> float:
        """
        解析 62G/57.31G/800M 形式的大小为字节数。
        """
        m = re.match(r"([\d.]+)\s*([KMGTP]?)B?$", (text or "").strip(), re.I)
        if not m:
            return 0.0
        mult = {"": 1, "K": 1024, "M": 1024 ** 2, "G": 1024 ** 3,
                "T": 1024 ** 4, "P": 1024 ** 5}[m.group(2).upper()]
        try:
            return float(m.group(1)) * mult
        except ValueError:
            return 0.0

    def __resolve_rows(self, rows: List[dict]) -> Tuple[List[dict], int, int, int]:
        """
        为磁力行补齐 magnet:优先取持久缓存,未命中的按预算并发解析。
        返回 (成功行, 缓存命中数, 新解析数, 超预算跳过数)
        """
        resolved: List[dict] = []
        to_fetch: List[dict] = []
        cache_hits = 0
        for row in rows:
            magnet = self._magnet_cache.get(row["seed_id"])
            if magnet:
                row["magnet"] = magnet
                resolved.append(row)
                cache_hits += 1
            elif len(to_fetch) < self._resolve_budget:
                to_fetch.append(row)
        skipped = len(rows) - cache_hits - len(to_fetch)

        fetched = 0
        if to_fetch:
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {executor.submit(self.__resolve_magnet, r["seed_id"]): r
                           for r in to_fetch}
                for future in as_completed(futures):
                    row = futures[future]
                    try:
                        magnet = future.result()
                    except Exception as e:
                        logger.debug(f"SeedHub 磁力解析异常 seed_id={row['seed_id']}:{str(e)}")
                        magnet = None
                    if magnet:
                        row["magnet"] = magnet
                        resolved.append(row)
                        fetched += 1
            if fetched:
                with self._cache_lock:
                    self.save_data("magnet_map", self._magnet_cache)
        return resolved, cache_hits, fetched, skipped

    def __resolve_magnet(self, seed_id: str) -> Optional[str]:
        res = self.__request(f"{self._domain}/link_start/?seed_id={seed_id}")
        if res is None or res.status_code != 200:
            return None
        m = re.search(r'const\s+data\s*=\s*"([A-Za-z0-9+/=]+)"', res.text)
        if not m:
            return None
        try:
            magnet = base64.b64decode(m.group(1)).decode("utf-8", "ignore")
        except Exception:
            return None
        if not magnet.startswith("magnet:"):
            return None
        with self._cache_lock:
            self._magnet_cache[seed_id] = magnet
        return magnet

    # endregion

    # region 域名巡检

    def __domain_check(self):
        """
        每日巡检:当前域名可用则刷新镜像列表;不可用则依次切换镜像。
        """
        mirrors = self.__probe_mirrors(self._domain)
        if mirrors is not None:
            self.save_data("mirrors", mirrors)
            logger.info(f"SeedHub 域名巡检正常:{self._domain},镜像 {mirrors}")
            return
        logger.warn(f"SeedHub 当前域名不可用:{self._domain},尝试切换镜像...")
        candidates = []
        for host in (self.get_data("mirrors") or []) + self.DEFAULT_MIRRORS:
            url = host if str(host).startswith("http") else f"https://{host}"
            url = url.rstrip("/")
            if url != self._domain and url not in candidates:
                candidates.append(url)
        for url in candidates:
            mirrors = self.__probe_mirrors(url)
            if mirrors is not None:
                old = self._domain
                self._domain = url
                self.update_config({
                    "enabled": self._enabled,
                    "proxy": self._proxy,
                    "domain": self._domain,
                    "max_movies": self._max_movies,
                    "max_seeds": self._max_seeds,
                    "resolve_budget": self._resolve_budget,
                    "timeout": self._timeout,
                })
                self.__register_indexer()
                self.save_data("mirrors", mirrors)
                logger.info(f"SeedHub 域名已切换:{old} -> {self._domain}")
                return
        logger.error("SeedHub 所有镜像域名均不可用")

    def __probe_mirrors(self, base: str) -> Optional[List[str]]:
        """
        探测域名可用性,可用返回镜像列表(可为空列表),不可用返回 None。
        """
        try:
            res = self.__request(f"{base}/worker-json-hosts/")
            if res is not None and res.status_code == 200:
                import json as _json
                hosts = _json.loads(res.text)
                if isinstance(hosts, list):
                    return [str(h) for h in hosts]
            return None
        except Exception:
            return None

    # endregion

    # region 插件框架接口

    def get_state(self) -> bool:
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled:
            return [{
                "id": "SeedHubDomainCheck",
                "name": "SeedHub 域名巡检",
                "trigger": CronTrigger.from_crontab("40 6 * * *"),
                "func": self.__domain_check,
                "kwargs": {}
            }]
        return []

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
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
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'enabled', 'label': '启用插件'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'proxy', 'label': '使用代理访问'}
                                }]
                            },
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'domain', 'label': '站点地址',
                                              'placeholder': 'https://sidhub.cc'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'max_movies', 'label': '单次解析作品数',
                                              'placeholder': '5'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'max_seeds', 'label': '单作品磁力上限',
                                              'placeholder': '20'}
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
                                    'component': 'VTextField',
                                    'props': {'model': 'resolve_budget',
                                              'label': '单次新解析磁力上限', 'placeholder': '30'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'timeout', 'label': '请求超时(秒)',
                                              'placeholder': '10'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VAlert',
                                    'props': {
                                        'type': 'info',
                                        'variant': 'tonal',
                                        'text': '公开磁力站,注册后自动进入搜索站点范围;'
                                                '磁力解析结果持久缓存,重复搜索开销极低;'
                                                '域名失效时每日巡检自动切换镜像。'
                                    }
                                }]
                            },
                        ]
                    },
                ]
            }
        ], {
            "enabled": False,
            "proxy": False,
            "domain": "https://sidhub.cc",
            "max_movies": 5,
            "max_seeds": 20,
            "resolve_budget": 30,
            "timeout": 10,
        }

    def get_page(self) -> List[dict]:
        return []

    def stop_service(self):
        pass

    # endregion
