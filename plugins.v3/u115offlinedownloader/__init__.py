# -*- coding: utf-8 -*-
"""
115离线下载器:通过 OpenList 的 115 离线下载 API 承接 MoviePilot 下载任务(V3)。

原理:
- 通过 get_module() 提供 download / list_torrents 等下载器模块;
- 站点(如 SeedHub)的 downloader 字段设为本下载器名称后,该站点资源的下载
  会路由到本插件(download 模块对非本下载器名返回 None 放行 qBittorrent);
- 磁力链接经 POST /api/fs/add_offline_download(tool=115 Open)提交 115 云端
  离线,btih 作为任务 Hash 登记下载历史;种子转磁力时带上 dn 和种子自带的 tracker;
- 任务状态以 115 自己的离线任务列表为准(开放平台 /open/offline/get_task_list,
  access_token 取自 OpenList 的 115 Open 存储)。OpenList 的任务记录只在提交初期参考:
  它轮询 115 状态一出错就重试 add,撞 10008"任务已存在"后整条任务记失败,
  不代表 115 那边的下载结果;
- 下载失败(115 判定失败,或提交根本没到 115)时与 MP 处理下载器提交失败保持一致:
  该资源写入 MP 的下载失败冷却台账(订阅冷却期内跳过该资源、改试其他资源),
  订阅里这几集回退到提交前的状态(未下载/原洗版优先级),清理 115 失败任务以便重试,
  并立即重新搜索该订阅;本插件还有某订阅的任务没下完时否决该订阅"完成",
  免得最后一集还没下完订阅就结束了、失败后无处回退;
- 下载完成后文件出现在 115 目标目录(如 媒体库监控目录),由 CloudLinkMonitor
  接手整理/strm/入库,本插件不参与转移(list_torrents 对可转移状态恒返回空)。
"""
import base64
import hashlib
import json
import re
import threading
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import quote, unquote_plus

import requests

from app import schemas
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import ChainEventType, MediaType, NotificationType

try:
    from app.schemas import DownloaderTorrent
except ImportError:
    from app.schemas.transfer import DownloaderTorrent

# 115 开放平台接口
PROAPI = "https://proapi.115.com"
# 115 离线任务状态:-1 失败、2 完成,其余(0 等待/1 下载中)为进行中
TASK_FAILED = -1
TASK_DONE = 2
# 115 离线列表按添加时间倒序:每轮先翻前两页,仍没找到的任务每 10 分钟翻一次全表
QUICK_PAGES = 2
DEEP_SCAN_INTERVAL = 600
MAX_PAGES = 100
# 种子转磁力最多附带的 tracker 数
MAX_TRACKERS = 50
# 任务最长跟踪天数(115 对卡住的任务一般 8 天左右判失败)
TRACK_DAYS = 30
# 同一订阅两次自动重新搜索的最小间隔(秒)
RESEARCH_GAP = 120
# 写入下载失败冷却台账时从提交上下文带过去的字段(DownloadFailureWrite)
FAILURE_FIELDS = ("media_type", "title", "year", "media_source", "media_id", "seasons", "episodes",
                  "site", "site_name", "torrent_id", "torrent_name", "torrent_size", "source")


class U115OfflineDownloader(_PluginBase):
    # 插件名称
    plugin_name = "115离线下载器"
    # 插件描述
    plugin_desc = "将磁力下载路由到 OpenList 的 115 离线下载,按 115 离线列表跟踪进度;下载失败时回退订阅重新找资源,与普通下载器一致。"
    # 插件图标
    plugin_icon = "115.png"
    # 插件版本
    plugin_version = "2.0.0"

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
    _fail_rollback = True
    _fail_cooldown = 24

    def __init__(self):
        super().__init__()
        self._tasks: Dict[str, dict] = {}
        self._tasks_lock = threading.Lock()
        self._poll_thread = None
        self._poll_stop = None
        # ResourceDownload 事件与随后的 download 模块调用在同一线程,用线程局部变量交接提交上下文
        self._capture = threading.local()
        # 115 开放平台:调用节流、token 缓存、全表扫描与告警节流
        self._api115_lock = threading.Lock()
        self._api115_last = 0.0
        self._token115: Optional[str] = None
        self._last_deep_scan = 0.0
        self._last_115_warn = 0.0
        self._last_research: Dict[int, float] = {}

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
            self._fail_rollback = bool(config.get("fail_rollback", True))
            self._fail_cooldown = self.__to_int(config.get("fail_cooldown"), 24, 0, 720)

        # 跟踪中的离线任务(btih -> info),持久化防重启丢失
        self._tasks = self.get_data("offline_tasks") or {}
        self._token115 = None

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

    # region 115 开放平台 API

    def __token115(self, refresh: bool = False) -> Optional[str]:
        """
        取 OpenList 里 115 Open 存储(挂载点覆盖目标目录的那个)的 access_token;
        OpenList 的 SDK 遇到过期会自动续期并回写存储,这里只读
        """
        if self._token115 and not refresh:
            return self._token115
        data = self.__api("/api/admin/storage/list")
        best = None
        for storage in ((data or {}).get("data") or {}).get("content") or []:
            if storage.get("driver") != "115 Open" or storage.get("disabled"):
                continue
            mount = (storage.get("mount_path") or "").rstrip("/")
            if self._target_path == mount or self._target_path.startswith(mount + "/"):
                if best is None or len(mount) > len(best[0]):
                    best = (mount, storage)
        token = None
        if best:
            try:
                token = json.loads(best[1].get("addition") or "{}").get("access_token")
            except Exception:
                token = None
        self._token115 = token or None
        return self._token115

    def __nudge_openlist(self):
        """
        让 OpenList 实际调用一次 115(强刷目标目录),它的 SDK 会在 token 过期时续期并写回存储
        """
        self.__api("/api/fs/list", "POST", {"path": self._target_path, "refresh": True,
                                           "page": 1, "per_page": 1})

    def __api115(self, path: str, method: str = "GET", params: dict = None,
                 data: dict = None) -> Tuple[Optional[dict], str]:
        """
        调 115 开放平台(与 OpenList 共用应用 token):调用间隔 ≥1.1 秒,不和 OpenList 抢 QPS;
        token 失效时让 OpenList 刷一次目录换新 token 再重试一次。返回 (响应, 错误)
        """
        for attempt in range(2):
            token = self.__token115(refresh=attempt > 0)
            if not token:
                return None, "读取 115 access_token 失败(OpenList 115 Open 存储)"
            with self._api115_lock:
                wait = self._api115_last + 1.1 - time.time()
                if wait > 0:
                    time.sleep(wait)
                try:
                    res = requests.request(method, f"{PROAPI}{path}", params=params, data=data,
                                           headers={"Authorization": f"Bearer {token}"}, timeout=20)
                except Exception as e:
                    return None, f"115 接口 {path} 请求出错:{str(e)}"
                finally:
                    self._api115_last = time.time()
            try:
                body = res.json()
            except Exception:
                return None, f"115 接口 {path} HTTP {res.status_code}"
            if body.get("state"):
                return body, ""
            code = str(body.get("code") or "")
            if attempt == 0 and (res.status_code == 401 or code.startswith("401401")):
                self.__nudge_openlist()
                continue
            return None, f"115 接口 {path} 返回 {code}:{body.get('message')}"
        return None, f"115 接口 {path} 鉴权失败"

    def __scan115(self, wanted: Set[str], max_pages: int) -> Tuple[Dict[str, dict], bool, str]:
        """
        在 115 离线任务列表里找这些 info_hash。
        返回 (找到的任务, 结论是否完整, 错误):完整 = 全找到了或翻完了全部页,此时没找到的就是真不在列表里
        """
        found: Dict[str, dict] = {}
        page = 1
        while True:
            body, err = self.__api115("/open/offline/get_task_list", params={"page": page})
            if body is None and "请求出错" in err:
                # 跨境链路偶发建连/握手卡住,同一页再试一次,免得全表扫描被一次超时打断
                body, err = self.__api115("/open/offline/get_task_list", params={"page": page})
            if body is None:
                return found, False, err
            data = body.get("data") or {}
            for task in data.get("tasks") or []:
                info_hash = str(task.get("info_hash") or "").lower()
                if info_hash in wanted:
                    found[info_hash] = task
            if wanted.issubset(found) or page >= int(data.get("page_count") or 1):
                return found, True, ""
            if page >= max_pages:
                return found, False, ""
            page += 1

    def __del115(self, btih: str) -> Tuple[bool, str]:
        """删除 115 离线任务记录(del_source_file=0:已下载的文件保留)"""
        body, err = self.__api115("/open/offline/del_task", "POST",
                                  data={"info_hash": btih, "del_source_file": 0})
        return body is not None, err

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
        删除任务模块:取消并删除 OpenList 离线任务,同时删除 115 侧未完成的离线任务(已落盘文件保留),
        并停止本插件跟踪。用户主动删除不回退订阅(与普通下载器一致)。非本插件任务返回 None 放行。
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
                logger.info(f"115离线:OpenList 中未找到 {title} 的任务记录")
            # OpenList 记录早已结束(如撞 10008)时取消不会删到 115,未完成的 115 任务直接删
            found, _, err = self.__scan115({btih}, QUICK_PAGES)
            task115 = found.get(btih)
            if task115 is not None and task115.get("status") != TASK_DONE:
                ok, err = self.__del115(btih)
                if ok:
                    logger.info(f"115离线:已删除 115 离线任务 {title}")
                else:
                    ok_all = False
                    logger.warn(f"115离线:删除 115 离线任务 {title} 失败:{err}")
            elif err:
                logger.warn(f"115离线:查询 115 离线任务 {title} 失败:{err}")
            self.__untrack(btih)
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
        # 先取走提交上下文:不认领也要清掉,免得串到同线程的下一次下载
        captured = self.__pop_capture(episodes)
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
            # 带上种子自带的 tracker:裸磁力只能靠 115 自己的 DHT/tracker 找 peer
            magnet += "".join(f"&tr={quote(tracker, safe='')}" for tracker in self.__torrent_trackers(torrent))

        btih = self.__parse_btih(magnet)
        # 磁力 hash 归一化为 hex(动漫花园等站点给的是 base32),否则 OpenList 任务名里是 base32,
        # 轮询用 hex 永远匹配不上 -> 任务永远"进行中"(混沌武士实测)
        magnet = self.__normalize_magnet(magnet, btih)
        title = title_hint or self.__parse_dn(magnet) or btih

        # 已在跟踪中的任务不重复提交
        with self._tasks_lock:
            exists = btih in self._tasks
            if exists and captured and not self._tasks[btih].get("ctx"):
                self._tasks[btih]["ctx"] = captured
                self.save_data("offline_tasks", self._tasks)
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
                    "ctx": captured,
                }
                self.save_data("offline_tasks", self._tasks)
            logger.info(f"115离线:已提交离线任务 {title} ({btih}) -> {self._target_path}")
        else:
            logger.info(f"115离线:任务已在跟踪中,不重复提交 {btih}")

        return self._downloader_name, btih, None, ""

    def list_torrents(self, status=None, hashs: Union[list, str] = None,
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
        if self.__query_excludes(status):
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
                left_time=info.get("left"),
                save_path=info.get("path"),
                tags=self._downloader_name,
            ))
        return result

    @staticmethod
    def __query_excludes(status) -> bool:
        """
        云端任务对 MP 永远是"下载中":可转移/已完成/已暂停的查询一律为空(兼容 V3 的 TorrentQueryStatus 与旧枚举)
        """
        text = str(getattr(status, "value", status) or "").strip().lower()
        return text in {"可转移", "transfer", "completed", "complete", "seeding", "完成", "已完成",
                        "paused", "pause", "暂停", "已暂停"}

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

    @staticmethod
    def __torrent_trackers(torrent) -> List[str]:
        """
        种子自带的 tracker(announce + announce-list,去重保序,最多 MAX_TRACKERS 个)
        """
        urls = []
        try:
            struct = getattr(torrent, "_struct", None) or {}
            if struct.get("announce"):
                urls.append(struct.get("announce"))
            for tier in getattr(torrent, "announce_urls", None) or []:
                urls.extend(tier if isinstance(tier, (list, tuple)) else [tier])
        except Exception as e:
            logger.debug(f"115离线:读取种子 tracker 出错:{str(e)}")
        result = []
        for url in urls:
            if isinstance(url, bytes):
                url = url.decode("utf-8", "ignore")
            url = str(url or "").strip()
            if url and url not in result and re.match(r"^(udp|https?|wss?)://", url, re.I):
                result.append(url)
        return result[:MAX_TRACKERS]

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
            return unquote_plus(m.group(1))
        except Exception:
            return m.group(1)

    # endregion

    # region 提交上下文

    @eventmanager.register(ChainEventType.ResourceDownload)
    def on_resource_download(self, event: Event = None):
        """
        资源下载事件:真正下载前(与随后的 download 模块调用同一线程)记下提交前的订阅事实和
        下载失败冷却指纹,供 115 任务最终失败时按原样回退。只读,不拦截。
        """
        self._capture.ctx = None
        if not self.get_state() or not event or not event.event_data:
            return
        data = event.event_data
        if getattr(data, "cancel", False):
            return
        requested = getattr(data, "downloader", None)
        if requested and requested != self._downloader_name:
            return
        try:
            self._capture.ctx = self.__capture_submission(
                getattr(data, "context", None), getattr(data, "episodes", None), getattr(data, "origin", None))
        except Exception as e:
            logger.debug(f"115离线:记录提交上下文出错:{str(e)}")

    def __pop_capture(self, episodes) -> Optional[dict]:
        """
        取走 ResourceDownload 事件刚记下的提交上下文:同线程、5 分钟内、集数一致才算同一次提交
        """
        ctx = getattr(self._capture, "ctx", None)
        self._capture.ctx = None
        if not ctx or time.time() - float(ctx.get("ts") or 0) > 300:
            return None
        wanted = self.__int_list(episodes)
        if wanted and ctx.get("episodes") and wanted != ctx["episodes"]:
            return None
        return ctx

    def __capture_submission(self, context, episodes, origin) -> Optional[dict]:
        """
        提交上下文:来源、订阅 ID、集数、资源优先级;订阅来源再带上提交前的订阅事实与冷却台账字段
        """
        torrent = getattr(context, "torrent_info", None) if context else None
        if torrent is None:
            return None
        meta = getattr(context, "meta_info", None)
        source = str(origin or "")
        priority = getattr(torrent, "pri_order", None)
        episodes = self.__int_list(episodes or getattr(meta, "episode_list", None))
        ctx = {
            "ts": time.time(),
            "source": source,
            "sid": self.__source_sid(source),
            "episodes": episodes,
            "pri": priority if isinstance(priority, int) and not isinstance(priority, bool) else None,
        }
        if ctx["sid"]:
            ctx["failure"] = self.__failure_template(context, source, episodes)
            ctx["before"], ctx["episodes"] = self.__subscribe_facts(ctx["sid"], episodes)
        return ctx

    @staticmethod
    def __source_sid(source: str) -> Optional[int]:
        """下载来源 "Subscribe|{...}" 里的订阅 ID"""
        if not source or not str(source).startswith("Subscribe|"):
            return None
        try:
            return int(json.loads(str(source).split("|", 1)[1]).get("id"))
        except Exception:
            return None

    def __actor(self):
        from app.application.subscription.mutation import SubscriptionActor
        return SubscriptionActor(name=self.plugin_name, is_superuser=True)

    def __read_subscription(self, sid: int):
        """读订阅快照(与 MP 订阅链同一套仓储)"""
        from app.startup.composition.subscription import sync_subscription_mutation_scope
        with sync_subscription_mutation_scope() as mutation:
            return mutation.get_accessible(int(sid), self.__actor())

    def __subscribe_facts(self, sid: int, episodes: List[int]) -> Tuple[Optional[dict], List[int]]:
        """
        提交前订阅里这几集的事实:洗版优先级(没有键 = 还没下载过任何版本)、是否已在 note、整体优先级。
        剧集资源没标集数(整季包)时按订阅目标范围记录,与 MP 记下载事实时的整包兜底一致。
        返回 (事实, 实际记录的集数)
        """
        sub = self.__read_subscription(sid)
        if not sub:
            return None, episodes
        from app.application.subscription import priority as rules
        priority = {}
        if sub.type == MediaType.TV.value:
            priority = rules.get_episode_priority(sub)
            episodes = episodes or rules.get_best_version_target_episodes(sub)
        note = set(self.__int_list(sub.note))
        return {
            "episode_priority": {str(e): priority[str(e)] for e in episodes if str(e) in priority},
            "note": [e for e in episodes if e in note] if episodes else sorted(note),
            "current_priority": sub.current_priority,
        }, episodes

    @staticmethod
    def __failure_template(context, source: str, episodes: List[int]) -> Optional[dict]:
        """
        按 MP 自己的规则算该资源的下载失败冷却指纹(订阅选源时据此跳过该资源),连同台账字段一起记下
        """
        try:
            from app.chain.download import DownloadChain
            fingerprint = DownloadChain._build_download_failure_fingerprint(context)
            if not fingerprint:
                return None
            media, meta, torrent = context.media_info, context.meta_info, context.torrent_info
            try:
                from app.schemas.media import resolve_media_identity
                media_source, media_id = resolve_media_identity(media=media)
            except Exception:
                media_source, media_id = None, None
            try:
                from app.domain import episode as episode_rules
                episode_text = episode_rules.format_ranges(list(episodes)) if episodes else None
            except Exception:
                episode_text = None
            mtype = getattr(media, "type", None)
            site = getattr(torrent, "site", None)
            season = getattr(meta, "season", None)
            return {
                "fingerprint": fingerprint,
                "media_type": getattr(mtype, "value", mtype),
                "title": getattr(media, "title", None),
                "year": str(media.year) if getattr(media, "year", None) is not None else None,
                "media_source": getattr(media_source, "value", media_source),
                "media_id": str(media_id) if media_id is not None else None,
                "seasons": str(season) if season is not None else None,
                "episodes": episode_text or DownloadChain._format_failure_episodes(meta),
                "site": site if isinstance(site, int) and not isinstance(site, bool) else None,
                "site_name": getattr(torrent, "site_name", None),
                "torrent_id": DownloadChain._torrent_resource_key(torrent),
                "torrent_name": getattr(torrent, "title", None),
                "torrent_size": getattr(torrent, "size", None),
                "source": source[:1000] if source else None,
            }
        except Exception as e:
            logger.debug(f"115离线:计算下载失败冷却指纹出错:{str(e)}")
            return None

    @eventmanager.register(ChainEventType.SubscribeCompletionCheck)
    def on_subscribe_completion_check(self, event: Event = None):
        """
        订阅即将判定完成:本插件还有该订阅的 115 任务没下完时否决,等任务完成/失败后再说
        (普通订阅在提交最后一集时就会完成,之后下载失败就没有订阅可以回退了)
        """
        if not self.get_state() or not self._fail_rollback or not event or not event.event_data:
            return
        data = event.event_data
        sid = getattr(getattr(data, "subscribe", None), "id", None)
        if not sid:
            return
        pending = []
        with self._tasks_lock:
            for btih, info in self._tasks.items():
                if (info.get("ctx") or {}).get("sid") == sid:
                    info["vetoed"] = True
                    pending.append(info.get("title") or btih)
            if pending:
                self.save_data("offline_tasks", self._tasks)
        if not pending:
            return
        data.cancel = True
        data.source = self.plugin_name
        data.reason = f"115 离线任务尚未完成:{'、'.join(pending[:3])}{' 等' if len(pending) > 3 else ''}"
        logger.info(f"115离线:订阅 {sid} 暂不完成,{data.reason}")

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

    def __history_of(self, btih: str):
        try:
            from app.db.downloadhistory_oper import DownloadHistoryOper
            oper = DownloadHistoryOper()
            try:
                return oper.get_by_hash(btih)
            except AttributeError:
                result = oper.get_by_hashes([btih]) or {}
                return result.get(btih) if isinstance(result, dict) else (result[0] if result else None)
        except Exception as e:
            logger.debug(f"115离线:读取下载历史出错:{str(e)}")
            return None

    def __enrich_tasks(self):
        """
        提交时刻拿不到的信息在轮询时从下载历史补齐(下载历史在模块返回后才落库):
        磁力无 dn 时的标题;没有提交上下文的任务(绕过 ResourceDownload 的下载等)的订阅出处
        """
        now = int(time.time())
        with self._tasks_lock:
            pending = {h: dict(v) for h, v in self._tasks.items()
                       if (v.get("title") or "") == h or not v.get("ctx_checked")}
        for btih, info in pending.items():
            his = self.__history_of(btih)
            if not his:
                if now - int(info.get("submitted") or now) > 600:
                    self.__update(btih, persist=True, ctx_checked=True)
                continue
            fields: Dict[str, Any] = {"ctx_checked": True}
            name = getattr(his, "torrent_name", None) or getattr(his, "title", None)
            if name and (info.get("title") or "") == btih:
                fields["title"] = str(name)
            if not (info.get("ctx") or {}).get("sid"):
                ctx = self.__resolve_ctx(btih, info, his)
                if ctx.get("sid"):
                    fields["ctx"] = ctx
            self.__update(btih, persist=True, **fields)

    def __match_openlist(self, tasks: List[dict], btih: str, info: dict) -> Optional[dict]:
        """OpenList 任务列表里本次提交对应的记录(名称含 btih;同一磁力更早的旧记录按开始时间排除)"""
        forms = self.__btih_forms(btih)
        since = int(info.get("submitted") or 0) - 120
        latest = None
        for task in tasks:
            if not any(f in str(task.get("name", "")).lower() for f in forms):
                continue
            started = self.__parse_time(task.get("start_time"))
            if started and 0 < started < since:
                continue
            latest = task
        return latest

    def __poll_once(self):
        self.__enrich_tasks()
        undone = self.__fetch_tasks("undone")
        done = self.__fetch_tasks("done")
        with self._tasks_lock:
            tracked = {k: dict(v) for k, v in self._tasks.items()}

        need: Dict[str, dict] = {}
        for btih, info in tracked.items():
            if info.get("src") != "115":
                # 进行中:OpenList 正在替我们轮询 115,直接用它的进度
                task = self.__match_openlist(undone, btih, info)
                if task is not None:
                    self.__update(btih, progress=round(float(task.get("progress") or 0.0), 1),
                                  status=str(task.get("status") or ""))
                    continue
                task = self.__match_openlist(done, btih, info)
                if task is not None and task.get("state") == 2:
                    self.__finish_success(btih, info)
                    continue
                if task is not None:
                    # OpenList 任务出错结束(常见是轮询出错后重试 add 撞 10008),不代表 115 侧的结果
                    error = str(task.get("error") or task.get("status") or "")
                    info.update(src="115", ol_error=error)
                    self.__update(btih, persist=True, src="115", ol_error=error)
                    logger.info(f"115离线:OpenList 任务已结束({error}),改按 115 离线列表跟踪 {info.get('title') or btih}")
            # 改由 115 离线列表判定(OpenList 已结束/记录丢失)
            need[btih] = info

        if need:
            self.__check_115(need)
        self.__expire_tasks()

    def __check_115(self, need: Dict[str, dict]):
        wanted = set(need)
        found, complete, err = self.__scan115(wanted, QUICK_PAGES)
        if not err and not complete and time.time() - self._last_deep_scan >= DEEP_SCAN_INTERVAL:
            self._last_deep_scan = time.time()
            found, complete, err = self.__scan115(wanted, MAX_PAGES)
        if err:
            if time.time() - self._last_115_warn >= 600:
                self._last_115_warn = time.time()
                logger.warn(f"115离线:查询 115 离线任务列表失败,下轮重试:{err}")
            return

        now = int(time.time())
        for btih, info in need.items():
            task = found.get(btih)
            if task is not None:
                status = task.get("status")
                if status == TASK_DONE:
                    self.__finish_success(btih, info, task)
                elif status == TASK_FAILED:
                    text = str(task.get("status_text") or "")
                    reason = "115 判定离线下载失败" + (f"({text})" if text and "失败" not in text else "")
                    self.__finish_failure(btih, info, reason=reason, progress=task.get("percentDone"), task115=task)
                else:
                    self.__update(btih, persist=info.get("src") != "115" or bool(info.get("missing")),
                                  src="115", missing=0,
                                  progress=round(float(task.get("percentDone") or 0.0), 2),
                                  size=float(task.get("size") or 0.0),
                                  speed=self.__fmt_size(task.get("rateDownload")),
                                  left=self.__fmt_duration(task.get("left_time")),
                                  peers=task.get("peers"),
                                  status=str(task.get("status_text") or ""))
                continue
            if not complete:
                continue
            # 115 离线列表里确实没有:连续 3 次且提交满 5 分钟才下结论
            missing = int(info.get("missing") or 0) + 1
            self.__update(btih, persist=True, missing=missing)
            if missing < 3 or now - int(info.get("submitted") or now) < 300:
                continue
            error = str(info.get("ol_error") or "")
            if error and not self.__is_duplicate_error(error):
                # OpenList 报的不是"任务已存在"、115 又没有这个任务:提交根本没到 115
                self.__finish_failure(btih, info, reason=f"提交 115 离线失败:{error}")
            else:
                self.__finish_lost(btih, info)

    @staticmethod
    def __is_duplicate_error(error: str) -> bool:
        return "任务已存在" in error or "10008" in error

    def __expire_tasks(self):
        now = int(time.time())
        with self._tasks_lock:
            expired = {h: dict(v) for h, v in self._tasks.items()
                       if now - int(v.get("submitted") or now) > TRACK_DAYS * 86400}
        for btih, info in expired.items():
            title = info.get("title") or btih
            self.__untrack(btih)
            logger.warn(f"115离线:任务 {title} 跟踪超过 {TRACK_DAYS} 天仍未结束,停止跟踪")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Download,
                    title="⚠️ 115离线任务跟踪超时",
                    text=f"{title}\n提交超过 {TRACK_DAYS} 天仍未下载完成(进度 {info.get('progress') or 0}%),"
                         f"已停止跟踪;订阅状态未改动,如需重下请在 115 删除该任务后手动处理"
                )
            self.__research_if_vetoed(info, "跟踪超时后复查订阅是否完成")

    # endregion

    # region 任务结束

    def __update(self, btih: str, persist: bool = False, **fields):
        with self._tasks_lock:
            if btih not in self._tasks:
                return
            self._tasks[btih].update(fields)
            if persist:
                self.save_data("offline_tasks", self._tasks)

    def __untrack(self, btih: str):
        with self._tasks_lock:
            self._tasks.pop(btih, None)
            self.save_data("offline_tasks", self._tasks)

    def __research_if_vetoed(self, info: dict, why: str):
        """该任务否决过订阅完成:结束后让订阅立即复查(否则要等下次定时搜索才会完成)"""
        sid = (info.get("ctx") or {}).get("sid")
        if info.get("vetoed") and sid:
            self.__research(sid, why)

    def __finish_success(self, btih: str, info: dict, task115: dict = None):
        title = info.get("title") or btih
        self.__untrack(btih)
        # 10008 撞上的是 115 早就完成的旧任务:115 不会重新下载,文件可能早已整理走
        stale = task115 is not None and 0 < int(task115.get("add_time") or 0) < int(info.get("submitted") or 0) - 600
        logger.info(f"115离线:任务完成 {title}" + ("(115 早有该资源的完成记录)" if stale else ""))
        if self._notify:
            text = f"{title}\n目录:{info.get('path') or self._target_path}\n后续由目录监控自动整理入库"
            if stale:
                text += "\n注意:115 早有该资源的完成记录,不会重新下载;如文件已不在,请在 115 删除这条离线记录后重新下载"
            self.post_message(mtype=NotificationType.Download, title="✅ 115离线下载完成", text=text)
        self.__research_if_vetoed(info, "115 任务完成后复查订阅是否完成")

    def __finish_lost(self, btih: str, info: dict):
        title = info.get("title") or btih
        self.__untrack(btih)
        logger.warn(f"115离线:115 离线列表里已没有 {title} 的任务(可能在 115 里被删除),停止跟踪")
        if self._notify:
            self.post_message(
                mtype=NotificationType.Download,
                title="⚠️ 115离线任务已不在",
                text=f"{title}\n115 离线列表里已找不到该任务(可能在 115 里被删除),已停止跟踪;订阅状态未改动"
            )
        self.__research_if_vetoed(info, "任务消失后复查订阅是否完成")

    def __finish_failure(self, btih: str, info: dict, reason: str, progress=None, task115: dict = None):
        """
        下载失败:与 MP 处理下载器提交失败一致——资源冷却、订阅回退、可重试、重新搜索,并通知
        """
        title = info.get("title") or btih
        # 先停止跟踪:随后的重新搜索里订阅完成检查不应再被这个任务否决
        self.__untrack(btih)
        try:
            progress_text = f"(进度 {float(progress):.2f}%)" if progress else ""
        except (TypeError, ValueError):
            progress_text = ""
        lines = [title, f"原因:{reason}{progress_text}"]
        if self._fail_rollback:
            lines.extend(self.__handle_failure(btih, info, reason, task115))
        else:
            lines.append("未开启失败回退:订阅仍视为已下载,如需重下请手动处理")
        logger.warn(f"115离线:任务失败 {title}:{reason}{progress_text};{';'.join(lines[2:])}")
        if self._notify:
            self.post_message(mtype=NotificationType.Download, title="❌ 115离线下载失败", text="\n".join(lines))

    def __handle_failure(self, btih: str, info: dict, reason: str, task115: dict = None) -> List[str]:
        actions = []
        if task115 is not None:
            # 115 对同一磁力连失败任务也报 10008,不清掉就永远重下不了
            ok, err = self.__del115(btih)
            actions.append("已清理 115 失败任务,同一资源可重新下载" if ok else f"清理 115 失败任务未成功:{err}")
        ctx = self.__resolve_ctx(btih, info)
        if not ctx.get("sid"):
            actions.append("非订阅下载,无订阅可回退")
            return actions
        if self._fail_cooldown and self.__record_cooldown(ctx, reason):
            actions.append(f"该资源冷却 {self._fail_cooldown} 小时,期间订阅改试其他资源")
        message, sid = self.__rollback_subscription(btih, ctx)
        actions.append(message)
        if sid or info.get("vetoed"):
            self.__research(ctx["sid"], "下载失败后重新搜索订阅")
            actions.append("已触发重新搜索该订阅")
        return actions

    def __resolve_ctx(self, btih: str, info: dict, his=None) -> dict:
        """
        任务的提交上下文;没有时(绕过 ResourceDownload 的下载)从下载历史补订阅出处和集数(无提交前快照)
        """
        ctx = dict(info.get("ctx") or {})
        if ctx.get("sid"):
            return ctx
        his = his or self.__history_of(btih)
        note = getattr(his, "note", None) if his else None
        if isinstance(note, str):
            try:
                note = json.loads(note)
            except Exception:
                note = None
        source = str(note.get("source") or "") if isinstance(note, dict) else ""
        sid = self.__source_sid(source)
        if sid:
            ctx.update(sid=sid, source=source, episodes=self.__parse_episodes(getattr(his, "episodes", None)))
        return ctx

    def __failure_repository(self):
        try:
            from app.chain.download import DownloadChain
            return DownloadChain().download_failure_repository
        except Exception:
            from app.db.adapters.download import TransactionalDownloadFailureRepository
            from app.db.session import SessionFactory
            return TransactionalDownloadFailureRepository(SessionFactory)

    def __record_cooldown(self, ctx: dict, reason: str) -> bool:
        """
        在 MP 的下载失败冷却台账记一笔(与提交失败同一机制):订阅选源时冷却期内跳过该资源、
        改试其他资源,到期后仍可重试;MP 只在订阅来源读取该台账
        """
        template = dict(ctx.get("failure") or {})
        fingerprint = template.pop("fingerprint", None)
        if not fingerprint:
            return False
        try:
            from app.application.download.failures import DownloadFailureWrite
            fields = {k: template.get(k) for k in FAILURE_FIELDS}
            if fields.get("media_source"):
                try:
                    from app.schemas.media import MediaSource
                    fields["media_source"] = MediaSource(fields["media_source"])
                except Exception:
                    pass
            now = time.time()
            self.__failure_repository().record_failure(DownloadFailureWrite(
                fingerprint=fingerprint,
                failed_at=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)),
                next_retry_at=time.strftime("%Y-%m-%d %H:%M:%S",
                                            time.localtime(now + self._fail_cooldown * 3600)),
                downloader=self._downloader_name,
                error_message=f"115离线下载失败:{reason}"[:1000],
                **fields,
            ))
            return True
        except Exception as e:
            logger.error(f"115离线:写入下载失败冷却出错:{str(e)}")
            return False

    def __landed_episodes(self, btih: str) -> Set[int]:
        """本任务已成功整理入库的集(多文件任务部分落盘时这些集不回退;电影记为 1)"""
        try:
            from app.db.transferhistory_oper import TransferHistoryOper
            rows = TransferHistoryOper().list_by_hash(btih) or []
        except Exception as e:
            logger.debug(f"115离线:读取整理记录出错:{str(e)}")
            return set()
        landed = set()
        for row in rows:
            if not getattr(row, "status", False):
                continue
            episodes = self.__parse_episodes(getattr(row, "episodes", None))
            if episodes:
                landed.update(episodes)
            elif getattr(row, "type", None) == MediaType.MOVIE.value:
                landed.add(1)
        return landed

    def __rollback_subscription(self, btih: str, ctx: dict) -> Tuple[str, Optional[int]]:
        """
        把订阅里这几集回退到提交前的状态;返回 (说明, 已回退的订阅 ID)
        """
        sid = ctx.get("sid")
        if not ctx.get("before"):
            return "缺少提交前的订阅快照(该任务未经 MP 正常下载流程提交),订阅未自动回退,如需重下请手动处理", None
        try:
            from app.startup.composition.subscription import sync_subscription_mutation_scope
        except Exception as e:
            return f"当前 MP 版本缺少订阅写接口,订阅未回退:{str(e)}", None
        landed = self.__landed_episodes(btih)
        try:
            with sync_subscription_mutation_scope() as mutation:
                actor = self.__actor()
                sub = mutation.get_accessible(int(sid), actor)
                if not sub:
                    return f"订阅 {sid} 已不存在(已完成或被删除),未回退", None
                payload, desc = self.__rollback_payload(sub, ctx, landed)
                if not payload:
                    return f"订阅《{sub.name}》{desc}", None
                if mutation.update(sub.id, payload, actor, existing=sub, scene="progress") is None:
                    return f"订阅《{sub.name}》回退未生效(订阅记录可能刚被修改)", None
                logger.info(f"115离线:订阅 {sub.id} 回退下载事实:{payload}")
                return f"订阅《{sub.name}》{desc}", sub.id
        except Exception as e:
            logger.error(f"115离线:回退订阅 {sid} 出错:{str(e)}")
            return f"回退订阅 {sid} 出错:{str(e)}", None

    def __rollback_payload(self, sub, ctx: dict, landed: Set[int]) -> Tuple[dict, str]:
        """
        按提交前快照算订阅回退补丁(纯计算);返回 (补丁, 说明),补丁为空时说明里是不回退的原因。
        只回退仍是本次提交留下的事实:之后被别的下载刷新过的集不动
        """
        from app.application.subscription import priority as rules
        before = ctx.get("before") or {}
        pri = ctx.get("pri")
        note = set(self.__int_list(sub.note))
        before_note = set(self.__int_list(before.get("note")))

        if sub.type != MediaType.TV.value:
            if landed:
                return {}, "已有文件入库,无需回退"
            payload, changes = {}, []
            if 1 in note and 1 not in before_note:
                payload["note"] = sorted(note - {1})
                changes.append("回退为未下载")
            old = before.get("current_priority")
            if sub.best_version and pri is not None and sub.current_priority == pri and old != pri:
                payload["current_priority"] = old
                changes.append(f"洗版优先级 {pri}→{old or 0}")
            return (payload, "、".join(changes)) if payload else ({}, "状态已变化,无需回退")

        targets = [e for e in self.__int_list(ctx.get("episodes")) if e not in landed]
        if not targets:
            return {}, "这几集已有文件入库,无需回退"

        if sub.best_version and not rules.is_full_best_version_enabled(sub):
            # 分集洗版:恢复提交前的优先级;提交前没下载过的集从优先级和 note 里去掉
            episode_priority = rules.get_episode_priority(sub)
            previous = before.get("episode_priority") or {}
            changes = []
            for e in targets:
                key = str(e)
                current = episode_priority.get(key)
                if current is None or pri is None or current != pri:
                    continue
                if key in previous:
                    if int(previous[key]) == current:
                        continue
                    episode_priority[key] = int(previous[key])
                    changes.append(f"E{e:02d} 洗版优先级 {current}→{previous[key]}")
                else:
                    episode_priority.pop(key, None)
                    if e not in before_note:
                        note.discard(e)
                    changes.append(f"E{e:02d} 回退为未下载")
            if not changes:
                return {}, "这几集状态已变化,无需回退"
            payload = {"episode_priority": episode_priority, "note": sorted(note)}
            payload.update(rules.prepare_subscribe_progress_fields(
                replace(sub, episode_priority=episode_priority, note=sorted(note))))
            return payload, "、".join(changes)

        removed = [e for e in targets if e in note and e not in before_note]
        if sub.best_version:
            # 全集洗版:整体优先级回退,首次下载的集移出 note
            payload, changes = {}, []
            old = before.get("current_priority")
            if pri is not None and sub.current_priority == pri and old != pri:
                payload["current_priority"] = old
                changes.append(f"全集洗版优先级 {pri}→{old or 0}")
            if removed:
                note.difference_update(removed)
                payload["note"] = sorted(note)
                changes.append(f"{self.__episodes_text(removed)} 回退为未下载")
            if not payload:
                return {}, "状态已变化,无需回退"
            payload.update(rules.prepare_subscribe_progress_fields(
                replace(sub, note=sorted(note), current_priority=payload.get("current_priority", sub.current_priority))))
            return payload, "、".join(changes)

        # 普通订阅:移出 note,缺集数加回来(下次刷新时 MP 会按媒体库重新核对)
        if not removed:
            return {}, "这几集状态已变化,无需回退"
        note.difference_update(removed)
        lack = (sub.lack_episode or 0) + len(removed)
        if sub.total_episode:
            lack = min(lack, sub.total_episode)
        return {"note": sorted(note), "lack_episode": lack}, f"{self.__episodes_text(removed)} 回退为未下载"

    def __research(self, sid, why: str):
        """让 MP 立即重新搜索该订阅(同一订阅 RESEARCH_GAP 秒内只触发一次),走后台线程不阻塞轮询"""
        try:
            sid = int(sid)
        except (TypeError, ValueError):
            return
        now = time.time()
        if now - self._last_research.get(sid, 0) < RESEARCH_GAP:
            return
        self._last_research[sid] = now

        def run():
            try:
                res = requests.post(f"http://127.0.0.1:{settings.PORT}/api/v1/subscribe/search/{sid}",
                                    params={"token": settings.API_TOKEN}, timeout=60)
                try:
                    message = res.json().get("message") or ""
                except Exception:
                    message = ""
                logger.info(f"115离线:{why},已提交订阅 {sid} 搜索:HTTP {res.status_code} {message}")
            except Exception as e:
                logger.warn(f"115离线:触发订阅 {sid} 搜索失败:{str(e)}")

        threading.Thread(target=run, daemon=True).start()

    # endregion

    # region 工具

    @staticmethod
    def __int_list(values) -> List[int]:
        result = set()
        for value in values or []:
            try:
                result.add(int(value))
            except (TypeError, ValueError):
                continue
        return sorted(result)

    @staticmethod
    def __parse_episodes(text) -> List[int]:
        """"E04" / "E01-E03" / "E01,E05" 之类的集数文本 -> [集号]"""
        episodes = set()
        for lo, hi in re.findall(r"E(\d+)(?:\s*-\s*E?(\d+))?", str(text or ""), re.I):
            lo, hi = int(lo), int(hi or lo)
            if hi < lo:
                lo, hi = hi, lo
            if hi - lo <= 2000:
                episodes.update(range(lo, hi + 1))
        return sorted(episodes)

    @staticmethod
    def __episodes_text(episodes: List[int]) -> str:
        episodes = sorted(episodes)
        if len(episodes) <= 4:
            return "、".join(f"E{e:02d}" for e in episodes)
        return f"E{episodes[0]:02d}-E{episodes[-1]:02d} 等 {len(episodes)} 集"

    @staticmethod
    def __parse_time(value) -> Optional[int]:
        """OpenList 的 ISO 时间(纳秒精度)-> 秒级时间戳"""
        if not value:
            return None
        try:
            text = re.sub(r"(\.\d{6})\d+", r"\1", str(value)).replace("Z", "+00:00")
            return int(datetime.fromisoformat(text).timestamp())
        except Exception:
            return None

    @staticmethod
    def __fmt_size(num) -> str:
        try:
            num = float(num or 0)
        except (TypeError, ValueError):
            return ""
        if num <= 0:
            return ""
        for unit in ("B", "K", "M", "G"):
            if num < 1024:
                return f"{num:.0f}{unit}" if unit == "B" else f"{num:.1f}{unit}"
            num /= 1024
        return f"{num:.1f}T"

    @staticmethod
    def __fmt_duration(seconds) -> str:
        try:
            seconds = int(seconds or 0)
        except (TypeError, ValueError):
            return ""
        if seconds <= 0:
            return ""
        hours, rest = divmod(seconds, 3600)
        minutes = rest // 60
        if hours:
            return f"{hours}小时{minutes}分"
        return f"{minutes}分" if minutes else f"{seconds}秒"

    # endregion

    # region 插件框架接口

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [{
            "path": "/tasks",
            "endpoint": self.api_tasks,
            "methods": ["GET"],
            "summary": "跟踪中的 115 离线任务",
        }, {
            "path": "/failure_preview",
            "endpoint": self.failure_preview,
            "methods": ["GET"],
            "summary": "下载失败处理预览(不改任何数据)",
        }]

    def api_tasks(self) -> schemas.Response:
        """
        API:跟踪中的离线任务(不含磁力全文)
        """
        with self._tasks_lock:
            tasks = {h: {k: v for k, v in info.items() if k != "magnet"} for h, info in self._tasks.items()}
        return schemas.Response(success=True, data=tasks)

    def failure_preview(self, hash: str = "", sid: int = 0, episodes: str = "",
                        pri: Optional[int] = None) -> schemas.Response:
        """
        API:下载失败处理预览,不写任何数据。
        - hash:跟踪中的任务此刻失败会怎样回退订阅、是否写冷却台账;
        - sid + episodes(逗号分隔)+ pri:模拟"现在向该订阅提交这几集、优先级 pri 的资源,随后失败":
          先按 MP 的规则推演提交后的订阅事实,再算回退补丁,回退后应与当前订阅一致
        """
        try:
            if hash:
                btih = hash.strip().lower()
                with self._tasks_lock:
                    info = dict(self._tasks.get(btih) or {})
                if not info:
                    return schemas.Response(success=False, message=f"没有跟踪中的任务 {btih}")
                ctx = self.__resolve_ctx(btih, info)
                landed = self.__landed_episodes(btih)
                data = {"ctx": {k: v for k, v in ctx.items() if k != "failure"},
                        "cooldown": bool((ctx.get("failure") or {}).get("fingerprint")),
                        "landed": sorted(landed)}
                if ctx.get("sid") and ctx.get("before"):
                    sub = self.__read_subscription(ctx["sid"])
                    data["rollback"] = self.__rollback_payload(sub, ctx, landed) if sub else "订阅已不存在"
                return schemas.Response(success=True, data=data)
            if not sid:
                return schemas.Response(success=False, message="需要 hash,或 sid + episodes + pri")
            from app.application.subscription import priority as rules
            sub = self.__read_subscription(int(sid))
            if not sub:
                return schemas.Response(success=False, message=f"订阅 {sid} 不存在")
            eps = self.__int_list(re.split(r"[,\s]+", episodes.strip())) if episodes.strip() else []
            pri = int(pri) if pri is not None else None
            before, eps = self.__subscribe_facts(int(sid), eps)
            ctx = {"sid": int(sid), "episodes": eps, "pri": pri, "before": before}
            # 推演 MP 记录下载事实后的订阅(completion.__record_subscribe_download_facts 的规则,
            # 普通剧集订阅随后按媒体库刷新缺集数)
            is_tv = sub.type == MediaType.TV.value
            old_note = set(self.__int_list(sub.note))
            note = old_note | (set(eps) if is_tv else {1})
            episode_priority = rules.get_episode_priority(sub) if is_tv else {}
            for e in eps:
                old = episode_priority.get(str(e))
                if pri is not None and (old is None or pri > old):
                    episode_priority[str(e)] = pri
            lack = sub.lack_episode
            if is_tv and not sub.best_version:
                lack = max(0, (sub.lack_episode or 0) - len(set(eps) - old_note))
            after = replace(sub, note=sorted(note), episode_priority=episode_priority, lack_episode=lack,
                            current_priority=(pri if not is_tv and sub.best_version else sub.current_priority))
            payload, desc = self.__rollback_payload(after, ctx, set())
            restored = {k: getattr(replace(after, **payload), k) for k in payload}
            current = {k: getattr(sub, k) for k in payload}
            return schemas.Response(success=True, data={
                "before": ctx["before"],
                "after_submit": {"note": after.note, "episode_priority": after.episode_priority,
                                 "current_priority": after.current_priority},
                "rollback": payload, "desc": desc,
                "restores_current": restored == current, "current": current,
            })
        except Exception as e:
            logger.error(f"115离线:失败处理预览出错:{str(e)}")
            return schemas.Response(success=False, message=str(e))

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
                                    'component': 'VSwitch',
                                    'props': {'model': 'fail_rollback',
                                              'label': '下载失败回退订阅'}
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
                                    'props': {'model': 'downloader_name', 'label': '下载器名称',
                                              'placeholder': '115离线'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'poll_interval', 'label': '轮询间隔(秒)',
                                              'placeholder': '30'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'fail_cooldown', 'label': '失败资源冷却(小时,0 不冷却)',
                                              'placeholder': '24'}
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
                                                '任务进度以 115 离线列表为准显示在下载管理页,云端任务不参与本地整理。'
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [{
                                    'component': 'VAlert',
                                    'props': {
                                        'type': 'info',
                                        'variant': 'tonal',
                                        'text': '下载失败回退订阅:115 判定下载失败(或提交没到 115)时,与下载器提交失败的处理一致——'
                                                '该资源进入 MP 下载失败冷却(冷却期内订阅改试其他资源),订阅里这几集回退为未下载/原洗版优先级,'
                                                '清理 115 失败任务以便重试,并立即重新搜索该订阅;'
                                                '115 任务没下完之前订阅不会判定完成。'
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
            "fail_rollback": True,
            "fail_cooldown": 24,
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
