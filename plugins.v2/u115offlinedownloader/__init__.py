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
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import quote

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
    plugin_version = "1.3.0"

    # 下载器列表注册用的自定义类型(qb/tr 模块按 type 匹配配置,不会认领此类型)
    DOWNLOADER_TYPE = "u115offline"
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
    _claim_public = True

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
            self._claim_public = bool(config.get("claim_public", True))

        # 跟踪中的离线任务(btih -> info),持久化防重启丢失
        self._tasks = self.get_data("offline_tasks") or {}

        self.__stop_poller()
        if self._enabled:
            self.__bind_site_downloader()
            self.__ensure_downloader_entry()
            self.__start_poller()
        else:
            self.__remove_downloader_entry()

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

    # region 下载器列表注册

    def __get_downloaders_config(self):
        from app.db.systemconfig_oper import SystemConfigOper
        from app.schemas.types import SystemConfigKey
        oper = SystemConfigOper()
        return oper, SystemConfigKey.Downloaders, (oper.get(SystemConfigKey.Downloaders) or [])

    def __ensure_downloader_entry(self):
        """
        把本下载器注册进系统下载器列表(自定义 type,qb/tr 模块按 type
        匹配配置不会认领),使其出现在下载弹窗/站点编辑的下载器下拉中。
        不设为默认,不改动用户对 enabled/default 的调整。
        """
        try:
            oper, key, downloaders = self.__get_downloaders_config()
            entry = next((d for d in downloaders
                          if d.get("name") == self._downloader_name), None)
            if entry:
                if entry.get("type") != self.DOWNLOADER_TYPE:
                    entry["type"] = self.DOWNLOADER_TYPE
                    oper.set(key, downloaders)
                return
            downloaders.append({
                "name": self._downloader_name,
                "type": self.DOWNLOADER_TYPE,
                "default": False,
                "enabled": True,
                "config": {},
                "path_mapping": [],
            })
            oper.set(key, downloaders)
            logger.info(f"115离线:已注册到下载器列表:{self._downloader_name}")
        except Exception as e:
            logger.error(f"115离线:注册下载器列表出错:{str(e)}")

    def __remove_downloader_entry(self):
        """
        插件停用时从下载器列表移除本条目,保持下拉选项真实。
        """
        try:
            oper, key, downloaders = self.__get_downloaders_config()
            remain = [d for d in downloaders
                      if not (d.get("name") == self._downloader_name
                              and d.get("type") == self.DOWNLOADER_TYPE)]
            if len(remain) != len(downloaders):
                oper.set(key, remain)
                logger.info(f"115离线:已从下载器列表移除:{self._downloader_name}")
        except Exception as e:
            logger.error(f"115离线:移除下载器列表条目出错:{str(e)}")

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
            "downloader_info": self.downloader_info,
            "remove_torrents": self.remove_torrents,
            "start_torrents": self.start_torrents,
            "stop_torrents": self.stop_torrents,
        }

    def __owned_hashes(self, hashs: Union[str, list], downloader: Optional[str] = None) -> Optional[List[str]]:
        """
        本下载器认领的 Hash 列表:显式指定了其他下载器或没有任何本插件跟踪中的任务时返回 None(放行其他模块)
        """
        if not self.get_state():
            return None
        if downloader and downloader != self._downloader_name:
            return None
        wanted = [str(h).lower() for h in ([hashs] if isinstance(hashs, str) else (hashs or []))]
        with self._tasks_lock:
            owned = [h for h in wanted if h in self._tasks]
        return owned or None

    def __find_openlist_task(self, btih: str) -> Tuple[Optional[dict], Optional[str]]:
        """按 btih(hex/base32 双形式)在 OpenList 未完成/已完成任务中查找,返回 (任务, 所在列表)"""
        forms = self.__btih_forms(btih)
        for kind in ("undone", "done"):
            for task in self.__fetch_tasks(kind):
                if any(f in str(task.get("name", "")).lower() for f in forms):
                    return task, kind
        return None, None

    def remove_torrents(self, hashs: Union[str, list], delete_file: Optional[bool] = True,
                        downloader: Optional[str] = None, **kwargs) -> Optional[bool]:
        """
        删除任务模块:取消并删除 OpenList 离线任务(115 Open 工具的 Remove 会同步删除 115 侧离线任务,
        已落盘文件保留),并停止本插件跟踪。非本插件任务返回 None 放行。
        """
        owned = self.__owned_hashes(hashs, downloader)
        if not owned:
            return None
        ok_all = True
        for btih in owned:
            with self._tasks_lock:
                info = dict(self._tasks.get(btih) or {})
            title = info.get("title") or btih
            task, kind = self.__find_openlist_task(btih)
            if task:
                tid = task.get("id")
                if kind == "undone":
                    self.__api(f"/api/admin/task/offline_download/cancel?tid={tid}", "POST")
                    # 等任务退出运行态(最多 ~8s)再删记录,否则 OpenList 会拒绝删除运行中的任务
                    for _ in range(8):
                        time.sleep(1)
                        again, kind_again = self.__find_openlist_task(btih)
                        if again is None or kind_again == "done":
                            break
                deleted = self.__api(f"/api/admin/task/offline_download/delete?tid={tid}", "POST")
                if deleted is None:
                    ok_all = False
                    logger.warn(f"115离线:OpenList 任务 {title} ({tid}) 删除失败,已停止跟踪,请在 OpenList 任务页手动清理")
                else:
                    logger.info(f"115离线:已取消并删除 OpenList 任务 {title} ({tid})")
            else:
                logger.info(f"115离线:OpenList 中未找到 {title} 的任务记录,仅停止跟踪")
            with self._tasks_lock:
                self._tasks.pop(btih, None)
                self.save_data("offline_tasks", self._tasks)
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Download,
                    title="🗑️ 115离线任务已删除",
                    text=f"{title}\n115 侧离线任务已取消;已落盘的文件(如有)保留在 {info.get('path') or self._target_path}"
                )
        return ok_all

    def stop_torrents(self, hashs: Union[list, str], downloader: Optional[str] = None,
                      **kwargs) -> Optional[bool]:
        """
        暂停模块:115 云端离线任务不支持暂停,明确返回失败(如需停止请删除任务);非本插件任务放行。
        """
        owned = self.__owned_hashes(hashs, downloader)
        if not owned:
            return None
        logger.warn(f"115离线:云端离线任务不支持暂停({', '.join(owned)}),如需停止请删除任务")
        return False

    def start_torrents(self, hashs: Union[list, str], downloader: Optional[str] = None,
                       **kwargs) -> Optional[bool]:
        """
        开始模块:115 云端离线任务提交后即由 115 执行,无需也无法手动开始;非本插件任务放行。
        """
        owned = self.__owned_hashes(hashs, downloader)
        if not owned:
            return None
        logger.info(f"115离线:云端离线任务无需手动开始({', '.join(owned)})")
        return True

    def downloader_info(self, downloader: Optional[str] = None, **kwargs) -> Optional[list]:
        """
        下载器信息模块:供设置页卡片与仪表板聚合(云端离线无本地速度,报零)。
        """
        if not self.get_state():
            return None
        if downloader and downloader != self._downloader_name:
            return None
        try:
            from app.schemas import DownloaderInfo
        except ImportError:
            from app.schemas.dashboard import DownloaderInfo
        return [DownloaderInfo()]

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

        explicit = downloader == self._downloader_name
        if downloader and not explicit:
            # 显式指定了其他下载器(如 qbtorrent),尊重选择
            return None

        magnet = None
        title_hint = None
        if isinstance(content, str) and content.startswith("magnet:"):
            magnet = content
        elif isinstance(content, bytes) and content.startswith(b"magnet:"):
            magnet = content.decode("utf-8", "ignore")

        if magnet:
            # 磁力:显式路由必接;自动模式按开关认领(SeedHub 缓存命中兜底)
            if not explicit and not self._claim_public and not self.__is_bound_magnet(magnet):
                return None
        else:
            # 种子内容(公开 BT 站如 Nyaa/ACG.RIP 的 enclosure 是 .torrent,
            # 链路已下载为字节):解析 infohash 转磁力;private=1 为 PT 种子,
            # 自动模式放行 qb(转磁力会丢 passkey,115 也拉不动,不该接)
            if not explicit and not self._claim_public:
                return None
            torrent, err = self.__parse_torrent(content)
            if torrent is None:
                if explicit:
                    return None, None, None, f"115离线下载器:{err}"
                return None
            if getattr(torrent, "private", False) and not explicit:
                return None
            magnet = f"magnet:?xt=urn:btih:{str(torrent.info_hash).lower()}"
            name = getattr(torrent, "name", None)
            if name:
                title_hint = str(name)
                magnet += f"&dn={quote(title_hint)}"

        btih = self.__parse_btih(magnet)
        # 磁力 hash 归一化为 hex(动漫花园等站点给的是 base32),否则 OpenList 任务名里是 base32,
        # 轮询用 hex 永远匹配不上 -> 任务永远"进行中"(混沌武士实测)
        magnet = self.__normalize_magnet(magnet, btih)
        title = title_hint or self.__parse_dn(magnet) or btih

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
    def __parse_torrent(content) -> Tuple[Optional[Any], Optional[str]]:
        """
        种子内容(bytes/Path)解析为 torrentool Torrent 对象。
        """
        data = None
        if isinstance(content, (bytes, bytearray)):
            data = bytes(content)
        elif isinstance(content, (str, Path)):
            try:
                p = Path(content)
                if p.exists():
                    data = p.read_bytes()
            except Exception:
                data = None
        if not data:
            return None, "无法读取种子内容"
        try:
            from torrentool.api import Torrent
            return Torrent.from_string(data), None
        except Exception as e:
            return None, f"种子解析失败:{str(e)}"

    def __is_bound_magnet(self, magnet: str) -> bool:
        """
        判断磁力是否出自 SeedHub 索引(其 seed_id->magnet 持久缓存)。
        """
        try:
            btih = None
            m = re.search(r"urn:btih:([0-9a-fA-F]{40}|[A-Za-z2-7]{32})", magnet)
            if m:
                btih = m.group(1).lower()
            if not btih:
                return False
            from app.db.plugindata_oper import PluginDataOper
            magnet_map = PluginDataOper().get_data("SeedHubIndexer", "magnet_map") or {}
            for cached in magnet_map.values():
                if btih in str(cached).lower():
                    return True
            return False
        except Exception as e:
            logger.debug(f"115离线:磁力来源判断出错:{str(e)}")
            return False

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
    def __normalize_magnet(magnet: str, btih: str) -> str:
        m = re.search(r"urn:btih:([0-9a-fA-F]{40}|[A-Za-z2-7]{32})", magnet)
        if m and len(m.group(1)) == 32 and len(btih) == 40:
            return magnet[:m.start(1)] + btih + magnet[m.end(1):]
        return magnet

    @staticmethod
    def __btih_forms(btih: str) -> list:
        """同一 infohash 的 hex 与 base32 两种写法(兼容历史任务名保留原始磁力的情况)"""
        forms = [btih.lower()]
        try:
            if len(btih) == 40:
                forms.append(base64.b32encode(bytes.fromhex(btih)).decode("ascii").lower())
        except Exception:
            pass
        return forms

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
                # 定期自愈:站点下载器绑定 + 下载器列表条目(防 UI 保存时被覆盖)
                if tick % 20 == 0:
                    self.__bind_site_downloader()
                    self.__ensure_downloader_entry()
            except Exception as e:
                logger.error(f"115离线:轮询出错:{str(e)}")

    def __enrich_titles(self):
        """
        磁力无 dn 时提交时刻拿不到标题(下载历史在模块返回后才落库),
        轮询时从下载历史按 Hash 反查补齐。
        """
        try:
            from app.db.downloadhistory_oper import DownloadHistoryOper
            oper = DownloadHistoryOper()
            with self._tasks_lock:
                pending = {h: v for h, v in self._tasks.items()
                           if (v.get("title") or "") == h}
            for btih, info in pending.items():
                his = None
                try:
                    his = oper.get_by_hash(btih)
                except AttributeError:
                    result = oper.get_by_hashes([btih]) or {}
                    his = result.get(btih) if isinstance(result, dict) else (result[0] if result else None)
                if not his:
                    continue
                name = getattr(his, "torrent_name", None) or getattr(his, "title", None)
                if name:
                    with self._tasks_lock:
                        if btih in self._tasks:
                            self._tasks[btih]["title"] = str(name)
        except Exception as e:
            logger.debug(f"115离线:标题补齐出错:{str(e)}")

    def __poll_once(self):
        self.__enrich_titles()
        undone = self.__fetch_tasks("undone")
        done = self.__fetch_tasks("done")
        changed = False

        with self._tasks_lock:
            tracked = dict(self._tasks)

        for btih, info in tracked.items():
            forms = self.__btih_forms(btih)
            # 进行中:更新进度
            u = next((t for t in undone
                      if any(f in str(t.get("name", "")).lower() for f in forms)), None)
            if u is not None:
                progress = round(float(u.get("progress") or 0.0), 1)
                status = str(u.get("status") or "")
                with self._tasks_lock:
                    if btih in self._tasks:
                        self._tasks[btih]["progress"] = progress
                        self._tasks[btih]["status"] = status
                continue
            # 已结束:通知并移除跟踪
            d = next((t for t in done
                      if any(f in str(t.get("name", "")).lower() for f in forms)), None)
            if d is not None:
                state = d.get("state")
                error = d.get("error") or d.get("status") or ""
                success = state == 2
                # 115 返回 10008"任务已存在":该磁力已在 115 离线列表
                # (此前提交过,或 OpenList 首次 add 已在 115 生效但其内部
                # 重试撞了重复),下载在 115 侧实际正常,不应报失败
                duplicate = (not success) and (
                    "任务已存在" in str(error) or "10008" in str(error))
                title = info.get("title") or btih
                if self._notify:
                    if success:
                        self.post_message(
                            mtype=NotificationType.Download,
                            title="✅ 115离线下载完成",
                            text=f"{title}\n目录:{info.get('path')}\n后续由目录监控自动整理入库"
                        )
                    elif duplicate:
                        self.post_message(
                            mtype=NotificationType.Download,
                            title="✅ 115离线任务已存在",
                            text=f"{title}\n该磁力已在 115 离线任务中(可能已完成),"
                                 f"文件到位后由目录监控自动整理入库"
                        )
                    else:
                        self.post_message(
                            mtype=NotificationType.Download,
                            title="❌ 115离线下载失败",
                            text=f"{title}\n{error}"
                        )
                logger.info(f"115离线:任务结束 {title} success={success} "
                            f"duplicate={duplicate} {error}")
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
                                    'component': 'VSwitch',
                                    'props': {'model': 'claim_public',
                                              'label': '自动认领公开资源'}
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
                                        'text': '自动认领公开资源:下载器为"默认"时,磁力与公开种子'
                                                '(private≠1,如 Nyaa/动漫花园/ACG.RIP)自动转 115 云端离线,'
                                                'PT 种子(private=1)仍走 qBittorrent;显式选择下载器时尊重选择。'
                                                '目标目录指向目录监控范围即可全自动整理入库;'
                                                '进行中任务显示在下载管理页,云端任务不参与本地整理。'
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
            "claim_public": True,
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
