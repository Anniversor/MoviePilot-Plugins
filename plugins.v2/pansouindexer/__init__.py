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
    plugin_version = "1.0.0"
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
    _registered = False

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = bool(config.get("enabled"))
            self._pansou_url = (config.get("pansou_url") or "http://127.0.0.1:18050").strip().rstrip("/")
            self._src = (config.get("src") or "all").strip() or "all"
            self._max_results = self.__to_int(config.get("max_results"), 50, 5, 200)
            self._timeout = self.__to_int(config.get("timeout"), 30, 5, 120)

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
        try:
            for kw in keywords[:2]:
                results = self.__do_search(site, str(kw))
                if results:
                    return results
            return []
        except Exception as e:
            logger.error(f"PanSou 搜索出错:{str(e)}")
            return []

    def __do_search(self, site: dict, keyword: str) -> List[TorrentInfo]:
        try:
            res = requests.get(
                f"{self._pansou_url}/api/search",
                params={"kw": keyword, "res": "merge", "src": self._src},
                headers={"User-Agent": self.UA},
                timeout=self._timeout,
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
        seen = set()
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
                                'props': {'cols': 12, 'md': 9},
                                'content': [{
                                    'component': 'VAlert',
                                    'props': {
                                        'type': 'info',
                                        'variant': 'tonal',
                                        'text': '仅采集 pansou 聚合结果中的磁力(BT)资源,'
                                                '网盘链接不采集;磁力自动被 115离线下载器 认领。'
                                                '聚合源无做种数信息,结果排序自然靠后,定位为兜底资源源。'
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
        }

    def get_page(self) -> List[dict]:
        return []

    def stop_service(self):
        self.__cancel_ensure_timer()

    # endregion
