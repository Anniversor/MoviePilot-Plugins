# -*- coding: utf-8 -*-
"""
115离线下载器:通过 OpenList 的 115 离线下载 API 承接 MoviePilot 下载任务。

原理:
- 通过 get_module() 提供 download / list_torrents 插件模块;
- 站点(如 SeedHub)的 downloader 字段设为本下载器名称后,该站点资源的下载
  会路由到本插件(download 模块对非本下载器名返回 None 放行 qBittorrent);
- 磁力链接经 POST /api/fs/add_offline_download(tool=115 Open)提交 115 云端
  离线,btih 作为任务 Hash 登记下载历史;
- 后台轮询 /api/admin/task/offline_download/{undone,done},进度伪装为
  下载器任务显示在下载管理页,完成/失败发送通知;
- 下载完成后文件出现在 115 目标目录(如 媒体库监控目录),由 CloudLinkMonitor
  接手整理/strm/入库,本插件不参与转移(list_torrents 对可转移状态恒返回空)。
"""
import base64
import hashlib
import json
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import requests

from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import NotificationType, TorrentStatus

try:
    from app.schemas import DownloaderTorrent
except ImportError:
    from app.schemas.transfer import DownloaderTorrent


class U115OfflineDownloader(_PluginBase):
    # 插件名称
    plugin_name = "115离线下载器"
    # 插件描述
    plugin_desc = "将磁力下载路由到 OpenList 的 115 离线下载,进度显示与完成通知,配合目录监控实现全自动云端入库。"
    # 插件图标
    plugin_icon = "115.png"
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "Anniversor"
    # 作者主页
    author_url = "https://github.com/Anniversor"
    # 插件配置项ID前缀
    plugin_config_prefix = "u115offlinedownloader_"
    # 加载顺序
    plugin_order = 22
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled = False
    _downloader_name = "115离线"
    _openlist_url = "http://127.0.0.1:5244"
    _openlist_token = ""
    _target_path = "/115/媒体库监控目录"
    _tool = "115 Open"
    _poll_interval = 30
    _notify = True
    _bind_domains = "sidhub.cc"

    def __init__(self):
        super().__init__()
        self._tasks: Dict[str, dict] = {}
        self._tasks_lock = threading.Lock()
        self._poll_thread = None
        self._poll_stop = None

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = bool(config.get("enabled"))
            self._downloader_name = (config.get("downloader_name") or "115离线").strip()
            self._openlist_url = (config.get("openlist_url") or "http://127.0.0.1:5244").strip().rstrip("/")
            self._openlist_token = (config.get("openlist_token") or "").strip()
            self._target_path = (config.get("target_path") or "/115/媒体库监控目录").strip().rstrip("/") or "/115/媒体库监控目录"
            self._tool = (config.get("tool") or "115 Open").strip()
            self._poll_interval = self.__to_int(config.get("poll_interval"), 30, 10, 600)
            self._notify = bool(config.get("notify", True))
            self._bind_domains = (config.get("bind_domains") or "").strip()

        # 跟踪中的离线任务(btih -> info),持久化防重启丢失
        self._tasks = self.get_data("offline_tasks") or {}

        self.__stop_poller()
        if self._enabled:
            self.__bind_site_downloader()
            self.__start_poller()

    @staticmethod
    def __to_int(value, default, lo, hi):
        try:
            return max(lo, min(hi, int(str(value).strip())))
        except (TypeError, ValueError):
            return default

    # region 站点下载器绑定

    def __bind_site_downloader(self):
        """
        把配置域名的站点(站点管理表)downloader 字段指到本下载器,
        使该站点的资源自动路由到 115 离线。
        """
        if not self._bind_domains:
            return
        try:
            from app.db.site_oper import SiteOper
            oper = SiteOper()
            for domain in [d.strip().lower() for d in re.split(r"[,\n]", self._bind_domains) if d.strip()]:
                site = oper.get_by_domain(domain)
                if not site:
                    logger.warn(f"115离线:站点 {domain} 不存在,跳过绑定")
                    continue
                if site.downloader != self._downloader_name:
                    oper.update(site.id, {"downloader": self._downloader_name})
                    logger.info(f"115离线:站点 {domain} 下载器已绑定为 {self._downloader_name}")
        except Exception as e:
            logger.error(f"115离线:绑定站点下载器出错:{str(e)}")

    # endregion

    # region OpenList API

    def __api(self, path: str, method: str = "GET", body: dict = None) -> Optional[dict]:
        try:
            res = requests.request(
                method, f"{self._openlist_url}{path}",
                headers={"Authorization": self._openlist_token, "Content-Type": "application/json"},
                json=body, timeout=15
            )
            if res.status_code != 200:
                logger.warn(f"115离线:OpenList {path} HTTP {res.status_code}")
                return None
            data = res.json()
            if data.get("code") != 200:
                logger.warn(f"115离线:OpenList {path} 返回 {data.get('code')}:{data.get('message')}")
                return None
            return data
        except Exception as e:
            logger.error(f"115离线:OpenList {path} 请求出错:{str(e)}")
            return None

    def __submit_offline(self, magnet: str) -> Tuple[bool, str]:
        data = self.__api("/api/fs/add_offline_download", "POST", {
            "path": self._target_path,
            "urls": [magnet],
            "tool": self._tool,
            "delete_policy": "delete_on_upload_succeed",
        })
        if data is None:
            return False, "OpenList 离线任务提交失败(详见日志)"
        return True, ""

    def __fetch_tasks(self, kind: str) -> List[dict]:
        data = self.__api(f"/api/admin/task/offline_download/{kind}")
        if data and isinstance(data.get("data"), list):
            return data["data"]
        return []

    # endregion

    # region 下载模块

    def get_module(self) -> Dict[str, Any]:
        return {
            "download": self.download,
            "list_torrents": self.list_torrents,
        }

    def download(self, content: Union[str, bytes] = None, download_dir=None, cookie: str = None,
                 episodes=None, category: Optional[str] = None, label: Optional[str] = None,
                 downloader: Optional[str] = None, **kwargs
                 ) -> Optional[Tuple[Optional[str], Optional[str], Optional[str], str]]:
        """
        下载模块:仅认领 downloader == 本下载器名称的任务,其余返回 None 放行。
        返回 (下载器名称, 种子Hash, 内容布局, 错误原因)
        """
        if not self.get_state():
            return None
        if not downloader or downloader != self._downloader_name:
            return None

        # 仅支持磁力链接(115 离线的能力边界)
        magnet = None
        if isinstance(content, str) and content.startswith("magnet:"):
            magnet = content
        elif isinstance(content, bytes) and content.startswith(b"magnet:"):
            magnet = content.decode("utf-8", "ignore")
        if not magnet:
            return None, None, None, "115离线下载器仅支持磁力链接"

        btih = self.__parse_btih(magnet)
        title = self.__parse_dn(magnet) or btih

        # 已在跟踪中的任务不重复提交
        with self._tasks_lock:
            exists = btih in self._tasks
        if not exists:
            ok, err = self.__submit_offline(magnet)
            if not ok:
                return None, None, None, err
            with self._tasks_lock:
                self._tasks[btih] = {
                    "title": title,
                    "magnet": magnet,
                    "path": self._target_path,
                    "submitted": int(time.time()),
                    "progress": 0.0,
                    "status": "已提交",
                }
                self.save_data("offline_tasks", self._tasks)
            logger.info(f"115离线:已提交离线任务 {title} ({btih}) -> {self._target_path}")
        else:
            logger.info(f"115离线:任务已在跟踪中,不重复提交 {btih}")

        return self._downloader_name, btih, None, ""

    def list_torrents(self, status: TorrentStatus = None, hashs: Union[list, str] = None,
                      downloader: Optional[str] = None, include_all_tags: bool = False,
                      **kwargs) -> Optional[List[DownloaderTorrent]]:
        """
        任务列表模块:进行中的离线任务伪装为下载器任务(供下载管理页显示进度);
        云端任务永远没有"可转移"状态,不参与本地整理。
        """
        if not self.get_state():
            return None
        if downloader and downloader != self._downloader_name:
            return None
        if status == TorrentStatus.TRANSFER:
            return []
        with self._tasks_lock:
            tasks = dict(self._tasks)
        if hashs:
            wanted = {h.lower() for h in ([hashs] if isinstance(hashs, str) else hashs)}
            tasks = {k: v for k, v in tasks.items() if k.lower() in wanted}
        result = []
        for btih, info in tasks.items():
            result.append(DownloaderTorrent(
                downloader=self._downloader_name,
                hash=btih,
                title=info.get("title"),
                size=float(info.get("size") or 0.0),
                progress=float(info.get("progress") or 0.0),
                state="downloading",
                dlspeed=info.get("speed"),
                save_path=info.get("path"),
                tags=self._downloader_name,
            ))
        return result

    @staticmethod
    def __parse_btih(magnet: str) -> str:
        m = re.search(r"urn:btih:([0-9a-fA-F]{40}|[A-Za-z2-7]{32})", magnet)
        if m:
            infohash = m.group(1)
            if len(infohash) == 32:
                # base32 -> hex
                try:
                    infohash = base64.b32decode(infohash.upper()).hex()
                except Exception:
                    pass
            return infohash.lower()
        return hashlib.md5(magnet.encode()).hexdigest()

    @staticmethod
    def __parse_dn(magnet: str) -> Optional[str]:
        m = re.search(r"[?&]dn=([^&]+)", magnet)
        if not m:
            return None
        try:
            from urllib.parse import unquote_plus
            return unquote_plus(m.group(1))
        except Exception:
            return m.group(1)

    # endregion

    # region 任务轮询

    def __start_poller(self):
        self._poll_stop = threading.Event()
        self._poll_thread = threading.Thread(target=self.__poll_loop, daemon=True)
        self._poll_thread.start()

    def __stop_poller(self):
        if self._poll_stop:
            self._poll_stop.set()
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=5)
        self._poll_thread = None
        self._poll_stop = None

    def __poll_loop(self):
        logger.info(f"115离线:任务轮询启动(间隔 {self._poll_interval}s)")
        stop = self._poll_stop
        tick = 0
        while stop is not None and not stop.wait(self._poll_interval):
            tick += 1
            try:
                with self._tasks_lock:
                    has_tasks = bool(self._tasks)
                if has_tasks:
                    self.__poll_once()
                # 定期重新绑定站点下载器(防 UI 编辑站点时被覆盖)
                if tick % 20 == 0:
                    self.__bind_site_downloader()
            except Exception as e:
                logger.error(f"115离线:轮询出错:{str(e)}")

    def __poll_once(self):
        undone = self.__fetch_tasks("undone")
        done = self.__fetch_tasks("done")
        changed = False

        with self._tasks_lock:
            tracked = dict(self._tasks)

        for btih, info in tracked.items():
            needle = btih.lower()
            # 进行中:更新进度
            u = next((t for t in undone if needle in str(t.get("name", "")).lower()), None)
            if u is not None:
                progress = round(float(u.get("progress") or 0.0), 1)
                status = str(u.get("status") or "")
                with self._tasks_lock:
                    if btih in self._tasks:
                        self._tasks[btih]["progress"] = progress
                        self._tasks[btih]["status"] = status
                continue
            # 已结束:通知并移除跟踪
            d = next((t for t in done if needle in str(t.get("name", "")).lower()), None)
            if d is not None:
                state = d.get("state")
                error = d.get("error") or d.get("status") or ""
                success = state == 2
                title = info.get("title") or btih
                if self._notify:
                    if success:
                        self.post_message(
                            mtype=NotificationType.Download,
                            title="✅ 115离线下载完成",
                            text=f"{title}\n目录:{info.get('path')}\n后续由目录监控自动整理入库"
                        )
                    else:
                        self.post_message(
                            mtype=NotificationType.Download,
                            title="❌ 115离线下载失败",
                            text=f"{title}\n{error}"
                        )
                logger.info(f"115离线:任务结束 {title} success={success} {error}")
                with self._tasks_lock:
                    self._tasks.pop(btih, None)
                changed = True
                continue
            # 两个列表都不在:可能 OpenList 清理了记录,超时后放弃跟踪
            if int(time.time()) - int(info.get("submitted") or 0) > 7 * 86400:
                logger.warn(f"115离线:任务 {info.get('title')} 超过 7 天未见状态,停止跟踪")
                with self._tasks_lock:
                    self._tasks.pop(btih, None)
                changed = True

        if changed:
            with self._tasks_lock:
                self.save_data("offline_tasks", self._tasks)

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
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'notify', 'label': '完成/失败通知'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'downloader_name', 'label': '下载器名称',
                                              'placeholder': '115离线'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'poll_interval', 'label': '轮询间隔(秒)',
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
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'openlist_url', 'label': 'OpenList 地址',
                                              'placeholder': 'http://127.0.0.1:5244'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'openlist_token', 'label': 'OpenList Token',
                                              'type': 'password'}
                                }]
                            },
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'target_path', 'label': '离线下载目标目录',
                                              'placeholder': '/115/媒体库监控目录'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'tool', 'label': '离线工具',
                                              'placeholder': '115 Open'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'bind_domains', 'label': '自动绑定站点(域名,逗号分隔)',
                                              'placeholder': 'sidhub.cc'}
                                }]
                            },
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [{
                                    'component': 'VAlert',
                                    'props': {
                                        'type': 'info',
                                        'variant': 'tonal',
                                        'text': '绑定站点的磁力资源将自动经 OpenList 提交 115 云端离线下载;'
                                                '目标目录指向目录监控范围即可全自动整理入库。'
                                                '进行中任务显示在下载管理页(进度来自 OpenList);'
                                                '云端任务不参与 MoviePilot 本地整理。'
                                    }
                                }]
                            },
                        ]
                    },
                ]
            }
        ], {
            "enabled": False,
            "notify": True,
            "downloader_name": "115离线",
            "poll_interval": 30,
            "openlist_url": "http://127.0.0.1:5244",
            "openlist_token": "",
            "target_path": "/115/媒体库监控目录",
            "tool": "115 Open",
            "bind_domains": "sidhub.cc",
        }

    def get_page(self) -> List[dict]:
        return []

    def stop_service(self):
        self.__stop_poller()

    # endregion
