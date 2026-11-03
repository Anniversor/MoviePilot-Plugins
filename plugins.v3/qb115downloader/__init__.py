"""MoviePilot V3 虚拟下载器：qB 本地下完 → OpenList 真实上传 → CLM。"""
try:
    from app.sdk.plugin import _PluginBase
except ImportError:
    from app.plugins import _PluginBase
from app.schemas.transfer import DownloaderTorrent
try:
    from app.schemas.download import DOWNLOAD_TASK_ALREADY_EXISTS
except ImportError:  # kept equal to the literal in app/schemas/download.py (checked by tools/check_moviepilot_contract.py)
    DOWNLOAD_TASK_ALREADY_EXISTS = "下载任务已存在"

import copy
import logging
import os
from pathlib import Path
import re
import shutil
import time

from .core import ConfigError, Engine, torrent_metadata, relative_name
from .adapter import QbAdapter
from .cloud import OpenList


class _AtomicEntries:
    """Downloaders list access through MoviePilot's atomic update (SystemConfigOper.update_atomically(key,
    mutation(session, current) -> (result, new_value))): the read and the write happen under MP's config write lock,
    so a concurrent edit of the downloader list by someone else is never overwritten by a stale copy."""

    def __init__(self, oper):
        self._oper = oper

    def get(self, key):
        return self._oper.get(key)

    def update_atomically(self, key, mutation):
        return self._oper.update_atomically(key, lambda _session, current: mutation(current))


class Qb115Downloader(_PluginBase):
    plugin_name = "qB → 115"
    plugin_desc = "本地 qB 下载保种，逐文件真实上传、校验后整批交接 CLM；删除/最终入库确认待人工。"
    plugin_icon = "115.png"
    plugin_version = "0.1.0"
    plugin_author = "Anniversor"
    author_url = "https://github.com/Anniversor"
    plugin_config_prefix = "qb115downloader_"
    plugin_order = 23
    auth_level = 1
    DOWNLOADER_NAME = "qB → 115"
    DOWNLOADER_TYPE = "qb115virtual"

    def __init__(self):
        super().__init__()
        self._enabled = False
        self._config = {}
        self._engine = None
        self._qb = None
        self._blocked = ""
        self._storage_key = None
        self._storage_checked_at = 0.0
        self._config_error = ""
        self._poll_interval = 30
        self._put_timeout_s = None
        self._raw_hosts = ()
        self._brief_cache = {}
        self._brief_at = 0.0

    STORAGE_TTL = 600  # seconds; avoids refresh=True listings of /115 and the monitor dir on every poll
    BRIEF_TTL = 5.0  # seconds; MP page refreshes / list calls share one batched qB status query
    _HOST_PATTERN = re.compile(r"\.?[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*")

    @classmethod
    def _validate_config(cls, config):
        """-> (clean values, [reasons]). Invalid values refuse to enable; nothing is silently clamped."""
        errors, clean = [], {}

        def number(key, default, minimum, label):
            value = config.get(key)
            if value is None or value == "":
                clean[key] = default
                return
            if isinstance(value, str) and re.fullmatch(r"\s*-?\d+\s*", value):
                value = int(value)
            elif isinstance(value, float) and value.is_integer():
                value = int(value)
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                errors.append("%s 必须是不小于 %s 的整数（%s）" % (key, minimum, label))
                clean[key] = default
            else:
                clean[key] = value
        number("put_timeout_seconds", 0, 0, "0=按文件大小自动估算")
        number("low_water_gib", 10, 0, "GiB")
        number("poll_interval", 30, 10, "秒")
        raw = config.get("raw_url_allowed_hosts") or ""
        hosts = tuple(h.lower() for h in re.split(r"[\s,;]+", raw if isinstance(raw, str) else " ".join(map(str, raw))) if h)
        if any(not cls._HOST_PATTERN.fullmatch(h) for h in hosts):
            errors.append("raw_url_allowed_hosts 含非法主机名（只允许 host 或以点开头的后缀，不含协议/路径/端口）")
            hosts = ()
        clean["raw_url_allowed_hosts"] = hosts
        return clean, errors

    @classmethod
    def _readiness(cls, config):
        """Static prerequisites for registering the virtual downloader (no IO). -> [reasons]; empty = ready.

        An entry that MoviePilot lists but that cannot work makes every submit fail and the guard alarm every 10 s."""
        reasons = []
        if not str(config.get("physical_downloader") or "").strip():
            reasons.append("physical_downloader 未填写（已配置的物理 qB 名称）")
        root = str(config.get("local_root") or "")
        sane = root.startswith("/") and root != "/"
        if sane:
            try:
                relative_name(root[1:])
            except ValueError:
                sane = False
        if not sane:
            reasons.append("local_root 必须是非根的绝对路径")
        elif any(root == m or root.startswith(m + "/") for m in ("/115", "/115_rclone", "/vol02")):
            reasons.append("local_root 不能位于云盘挂载 /115、/115_rclone 或 /vol02 之下")
        if not config.get("isolation_confirmed"):  # same truthiness as _runtime()
            reasons.append("isolation_confirmed 未勾选（需人工确认本地/暂存目录不受其他监控与清理）")
        if config.get("credential_source", "U115OfflineDownloader") not in {"U115OfflineDownloader", "CloudLinkMonitor"}:
            reasons.append("credential_source 只允许 U115OfflineDownloader / CloudLinkMonitor")
        for key, default in (("staging_path", "/115/.qb115-staging"), ("target_path", "/115/媒体库监控目录")):
            if not str(config.get(key, default) or "").startswith("/115/"):
                reasons.append("%s 必须位于 /115/ 之下" % key)
        return reasons

    def init_plugin(self, config=None):
        self.stop_service()
        previous = self._engine
        if previous:
            if not previous.worker.acquire(timeout=65):
                self._enabled = False
                self._blocked = ("旧上传仍未停止；本次重载未应用（旧配置保留、不接新任务、不推进上传，隔离巡检仍在运行），"
                                 "上传结束后重新保存配置以重试，不启动并发写入")
                return
            previous.worker.release()
            if not previous.lock.acquire(timeout=65):
                self._enabled = False
                self._blocked = ("旧 qB 提交仍未停止；本次重载未应用（不接新任务、不推进上传，隔离巡检仍在运行），"
                                 "待确认账本后重新保存配置以重试")
                return
            previous.lock.release()
        self._config = dict(config or {})
        clean, errors = self._validate_config(self._config)
        wants = bool(self._config.get("enabled", False))
        missing = self._readiness(self._config) if wants and not errors else []
        self._config_error = (("配置无效，已拒绝启用：" + "；".join(errors)) if errors else
                              ("配置不完整，已拒绝启用（未注册下载器条目）：" + "；".join(missing)) if missing else "")
        self._enabled = wants and not errors and not missing
        self._poll_interval = clean["poll_interval"]
        self._put_timeout_s = clean["put_timeout_seconds"] or None
        self._raw_hosts = clean["raw_url_allowed_hosts"]
        self._blocked = ""
        self._qb = None  # never keep an adapter bound to the previous configuration
        self._storage_key = None
        self._brief_cache, self._brief_at = {}, 0.0
        self._engine = Engine(self.get_data("tasks_v1") or {},
                              lambda data: self.save_data("tasks_v1", data),
                              free=lambda: shutil.disk_usage(self._config.get("local_root") or "/").free,
                              low_water=clean["low_water_gib"] * 1024**3)
        self._name_conflict = False
        try:
            self._sync_entry()
        except Exception:
            if self._enabled:
                self._name_conflict = True
                self._blocked = "下载器注册未确认或名称冲突；拒绝提交，不修改其他下载器"

    def _entry_store(self):
        from app.db.oper.systemconfig import SystemConfigOper
        from app.schemas.types import SystemConfigKey
        return _AtomicEntries(SystemConfigOper()), SystemConfigKey.Downloaders

    def _plan_entries(self, current):
        """The downloader list this plugin wants, derived from ``current``; touches only its own entry."""
        entries = copy.deepcopy(current or [])
        own = next((x for x in entries if x.get("name") == self.DOWNLOADER_NAME), None)
        if own and own.get("type") != self.DOWNLOADER_TYPE:
            raise ValueError("downloader name collision; refusing to retype")
        if self._enabled and not own:
            entries.append(dict(name=self.DOWNLOADER_NAME, type=self.DOWNLOADER_TYPE, enabled=True,
                                default=False, config={}, path_mapping=[]))
        elif not self._enabled:
            entries = [x for x in entries if not (x.get("name") == self.DOWNLOADER_NAME and x.get("type") == self.DOWNLOADER_TYPE)]
        return entries

    def _sync_entry(self):
        store, key = self._entry_store()
        current = store.get(key)
        if self._plan_entries(current) == list(current or []):  # also raises on a name collision
            return
        # re-derived from the value MoviePilot holds under its config write lock, not from our earlier read
        store.update_atomically(key, lambda latest: (None, self._plan_entries(latest)))
        registered = any(x.get("name") == self.DOWNLOADER_NAME and x.get("type") == self.DOWNLOADER_TYPE
                         for x in (store.get(key) or []))
        if registered != self._enabled:
            raise ValueError("downloader registry readback failed")

    def _guard_needed(self):
        """Any ledger entry (tracked or untracked) still needs the isolation sweep, whatever the enabled flag says."""
        return bool(self._engine and self._engine.tasks)

    def get_state(self):
        # MoviePilot registers get_service() jobs only for a plugin whose get_state() is true. A disabled / invalid /
        # reload-blocked plugin with tasks in its ledger must keep the guard, so it stays "running" for that sole purpose
        # (_owns() still answers None to every download/list/stop/start/remove call: nothing is routed to it).
        return bool(self._enabled or self._guard_needed())

    def _owns(self, downloader):
        return self._enabled and downloader == self.DOWNLOADER_NAME

    def get_module(self):
        return {name: getattr(self, name) for name in
                ("download", "list_torrents", "remove_torrents", "stop_torrents", "start_torrents")}

    def download(self, content=None, download_dir=None, cookie=None, episodes=None,
                 category=None, label=None, downloader=None, custom_words=None, **kwargs):
        if not self._owns(downloader):
            return None
        task = None
        tid = None
        qb = None
        engine = self._engine
        try:
            qb, _ = self._runtime()
            self._qb = qb
            if isinstance(content, Path):
                fd = os.open(content, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(fd, "rb") as stream:
                    content = stream.read(16 * 1024**2 + 1)
            tid, name, size = torrent_metadata(content)
            with engine.lock:
                if engine is not self._engine or engine.stopped.is_set() or not self._owns(downloader):
                    raise ValueError("plugin stopped/reloaded during preflight")
                if tid in engine.tasks:
                    existing = engine.tasks[tid]
                    if existing["state"] in {"admitted", "suspended", "untracked"} or existing.get("quarantined"):
                        raise ValueError("submission incomplete; manual reconciliation required")
                    qb.snapshot(tid)
                    # MoviePilot's own contract for "same hash already tracked" (schemas/download.py)
                    return self.DOWNLOADER_NAME, tid, existing.get("layout"), DOWNLOAD_TASK_ALREADY_EXISTS
                qb.preflight(tid)  # duplicate hash in the physical qB is refused BEFORE any ledger write
                task = engine.admit(tid, name, size, self._config["local_root"], physical=qb.name,
                                    save_path=qb.expected_save_path())
                result = qb.submit(content, tid, episodes=episodes, custom_words=custom_words)
                engine._set(task, state="downloading", layout=result[2])
            return self.DOWNLOADER_NAME, tid, result[2], "qB 已提交；云端上传/入库尚未完成"
        except Exception as err:
            if task is not None:
                if getattr(err, "not_added", False):
                    engine.rollback(tid)  # qB confirmed it never added the torrent: free the reservation
                else:
                    engine._suspend(task, "qB 提交未确认；保留预留与源文件，人工核对后处理，不自动重复添加", qb)
            return None, None, None, "安全检查或提交失败；请查看插件状态，原始凭据/错误不记录"

    def _qb_adapter(self):
        """Physical qB only: no OpenList, no filesystem checks. Used by pause/start/guard/status."""
        from app.sdk.plugin import ModuleManager
        from app.modules.qbittorrent import QbittorrentModule
        from app.runtime.settings import get_runtime_setting
        root = self._config.get("local_root") or ""
        if not root.startswith("/") or root == "/":
            raise ValueError("explicit isolated local root required")
        physical = self._config.get("physical_downloader") or ""
        modules = [m for m in ModuleManager().get_running_modules("download") if isinstance(m, QbittorrentModule)]
        if len(modules) != 1:
            raise ValueError("exactly one already-running native QbittorrentModule required")
        return QbAdapter(modules[0], physical, root, lambda: get_runtime_setting("TORRENT_TAG"))

    def _runtime(self):
        if self._name_conflict:
            raise ValueError("virtual downloader registration is blocked")
        from app.application.directory import DirectoryHelper
        root = self._config.get("local_root") or ""
        if not self._config.get("isolation_confirmed") or not root.startswith("/") or root == "/":
            raise ConfigError("需要明确的独立本地目录（绝对路径、非根）并勾选隔离确认")
        relative_name(root[1:])
        # /115, /115_rclone and /vol02 are THIS NAS's cloud mounts / big volume: hardcoded for this environment
        if any(root == p or root.startswith(p + "/") for p in ("/115", "/115_rclone", "/vol02")):
            raise ConfigError("本地下载目录不能位于云盘挂载 /115、/115_rclone 或 /vol02（本机环境硬编码）之下")
        current = Path("/")
        for component in Path(root).parts[1:]:
            current /= component
            if current.is_symlink() or not current.is_dir():
                raise ConfigError("本地下载目录必须已存在，且各级目录都不是符号链接")
        for directory in DirectoryHelper().get_dirs():
            storage = str(getattr(directory.storage, "value", directory.storage) or "local")
            path = str(directory.download_path or "").removeprefix("local:")
            path = path.rstrip("/") or ("/" if path else "")
            if storage == "local" and directory.monitor_type == "monitor" and path:
                if path == "/" or root == path or root.startswith(path + "/") or path.startswith(root + "/"):
                    raise ConfigError("本地下载目录与 MP 的本地目录监控重叠")
        qb = self._qb_adapter()
        qb.guard()
        source = self._config.get("credential_source", "U115OfflineDownloader")
        if source not in {"U115OfflineDownloader", "CloudLinkMonitor"}:
            raise ConfigError("不支持的凭据来源插件（只允许 U115OfflineDownloader / CloudLinkMonitor）")
        credentials = self.get_config(plugin_id=source) or {}
        token = credentials.get("openlist_token") or ""
        origin = credentials.get("openlist_url") or ""
        if not token or not origin:
            raise ConfigError("凭据来源插件里没有可用的 OpenList 地址/令牌")
        try:
            cloud = OpenList(origin, token, put_timeout=self._put_timeout_s, raw_hosts=self._raw_hosts)
        except ValueError:
            raise ConfigError("OpenList 地址被拒绝：必须是不含账号/query/子路径的 origin，且明文 http 只允许回环/内网地址（否则请用 https）") from None
        staging = self._config.get("staging_path", "/115/.qb115-staging")
        monitor = self._config.get("target_path", "/115/媒体库监控目录")
        key = (origin, staging, monitor)
        if self._storage_key != key or time.monotonic() - self._storage_checked_at > self.STORAGE_TTL:
            self._storage_key = None
            cloud.validate_storage(staging, monitor)
            self._storage_key, self._storage_checked_at = key, time.monotonic()
        return qb, cloud

    def poll(self):
        if not self._enabled:
            return
        try:
            qb, cloud = self._runtime()
            self._qb = qb
            self._blocked = ""
            self._engine.guard(qb)
            self._engine.tick(qb, cloud, self._config.get("staging_path", "/115/.qb115-staging"),
                              self._config.get("target_path", "/115/媒体库监控目录"))
        except ConfigError as err:  # fixed, credential/URL-free messages
            self._blocked = "运行前检查失败：" + str(err)
        except Exception:
            self._blocked = "运行前检查失败：检查物理 qB、独立目录/标签、凭据来源及 OpenList 配置"

    def guard(self):
        """Isolation sweep: needs only the physical qB, never OpenList. A failing sweep is shown, never swallowed."""
        engine = self._engine
        if not engine:
            return
        if not engine.tasks:  # nothing to protect: neither an adapter nor an alarm
            engine.guard_failed = False
            return
        try:
            engine.guard(self._qb_adapter())
        except Exception:
            self._engine.guard_failed = True
            logging.getLogger("qb115downloader").warning("qb115 event=guard_failed")

    def _status_qb(self):
        if self._qb is not None:
            return self._qb
        try:
            return self._qb_adapter()
        except Exception:
            return None

    def _status_note(self):
        failed = bool(self._engine and self._engine.guard_failed)
        guard_only = (not self._enabled) and self._guard_needed()
        return " | ".join(x for x in (self._config_error, self._blocked,
                                      "ℹ 插件未启用（已禁用/配置无效/重载被阻塞）：仅隔离巡检在运行（保护性暂停、摘全局标签），不推进上传、不接新任务" if guard_only else "",
                                      "⚠ 隔离巡检失败：读不到物理 qB 的任务/标签，隔离保护暂不可用（日志事件码 guard_failed）" if failed else "") if x)

    def _brief(self, hashes):
        """Batched qB progress/state for ``hashes`` (cached BRIEF_TTL s); never per-task file listings."""
        if not hashes:
            return {}
        fresh = time.monotonic() - self._brief_at < self.BRIEF_TTL
        if fresh and all(h in self._brief_cache for h in hashes):
            return {h: self._brief_cache[h] for h in hashes}
        qb = self._status_qb()
        if qb is None:
            return {}
        try:
            result = qb.brief(hashes)
        except Exception:
            return {}
        if not fresh:
            self._brief_cache = {}
        self._brief_cache.update(result)
        self._brief_at = time.monotonic()
        return result

    def tasks_status(self, hashs=None):
        note = self._status_note()
        if not self._engine:
            return {"enabled": self._enabled, "blocked": note, "tasks": []}
        ids = None if hashs is None else ({hashs} if isinstance(hashs, str) else set(hashs))
        with self._engine.lock:
            tasks = copy.deepcopy([t for t in self._engine.tasks.values() if ids is None or t["hash"] in ids])
        brief = self._brief([t["hash"] for t in tasks if t["state"] != "untracked"])
        rows = []
        for task in tasks:
            info = brief.get(task["hash"])
            progress, qb_state = (info["progress"] * 100, info["state"]) if info else (None, "unknown")
            quarantined = bool(task.get("quarantined")) or bool(info and not info["isolated"])
            rows.append(dict(hash=task["hash"], name=task["name"], state=task["state"], paused=task["paused"],
                             quarantined=quarantined, root=task.get("root"), physical_downloader=task.get("physical_downloader"),
                             reserved=task["reserved"], selected_size=task.get("selected_size"),
                             qb_progress=progress, qb_state=qb_state, error=task["error"],
                             target=task.get("target"), next_retry=task["next_retry"],
                             library_confirmed=False, cleanup="待人工（本版永不删除）"))
        return {"enabled": self._enabled, "blocked": note, "tasks": rows}

    # Aliases mirror modules/_base/downloader.py::_normalize_query_status; TorrentStatus values are Chinese.
    _TRANSFER = {"transfer", "可转移"}
    _DOWNLOADING = {"downloading", "下载中"}
    _COMPLETED = {"completed", "complete", "seeding", "完成", "已完成"}
    _PAUSED = {"paused", "pause", "暂停", "已暂停"}

    @classmethod
    def _query_status(cls, status):
        text = str(getattr(status, "value", status) or "").strip().lower()
        for name, values in (("transfer", cls._TRANSFER), ("downloading", cls._DOWNLOADING),
                             ("completed", cls._COMPLETED), ("paused", cls._PAUSED)):
            if text in values:
                return name
        return "all"

    @staticmethod
    def _mp_state(task):
        """MoviePilot's three states. Only waiting_library counts as completed: qB 100% or uploaded-but-
        not-handed-off must not appear as finished (nor as 'downloading' once the batch is handed off)."""
        if task["paused"] or task.get("quarantined") or task["state"] == "suspended":
            return "paused"
        return "completed" if task["state"] == "waiting_library" else "downloading"

    def list_torrents(self, status=None, hashs=None, downloader=None, include_all_tags=False, **kwargs):
        # Dispatcher merges list results (ORDERED_LIST_MERGE); returning None for foreign/unspecified
        # downloader names keeps every other downloader untouched. A global (downloader=None) listing
        # therefore never shows these tasks; the downloading page works when MP passes our exact name.
        if not self._owns(downloader):
            return None
        query = self._query_status(status)
        if query == "transfer":
            return []
        ids = {hashs} if isinstance(hashs, str) else set(hashs or [])
        result = []
        for task in self.tasks_status(hashs=ids or None)["tasks"]:
            if task["state"] == "untracked":
                continue
            state = self._mp_state(task)
            if not ids and query != "all" and state != query:  # explicit hashs win over status, as natively
                continue
            progress = 100.0 if state == "completed" else min(task["qb_progress"] or 0, 99.9)
            content = "%s/%s" % ((task["root"] or "").rstrip("/"), task["name"])
            result.append(DownloaderTorrent(downloader=self.DOWNLOADER_NAME, hash=task["hash"],
                                             title=task["name"], name=task["name"], progress=progress, state=state,
                                             size=task["selected_size"] or task["reserved"], tags="mp-qb115",
                                             save_path=task["root"], content_path=content, path=Path(content)))
        return result

    def _control(self, hashs, operation):
        ids = [hashs] if isinstance(hashs, str) else list(hashs or [])
        try:
            qb = self._qb_adapter()  # fresh, physical qB only: pause/start must not depend on OpenList
        except Exception:
            return False
        results = [operation(tid, qb) for tid in ids]  # no short-circuit: act on every requested hash
        return bool(results) and all(results)

    def remove_torrents(self, hashs, delete_file=True, downloader=None, **kwargs):
        if not self._owns(downloader):
            return None
        if delete_file is not False:
            return False
        return self._control(hashs, lambda tid, qb: self._engine.remove(tid, qb, delete_file=False))

    def stop_torrents(self, hashs, downloader=None, **kwargs):
        if not self._owns(downloader):
            return None
        return self._control(hashs, lambda tid, qb: self._engine.control(tid, "pause", qb))

    def start_torrents(self, hashs, downloader=None, **kwargs):
        if not self._owns(downloader):
            return None
        return self._control(hashs, lambda tid, qb: self._engine.control(tid, "start", qb))

    def stop_service(self):
        self._enabled = False
        if self._engine:
            self._engine.stopped.set()

    @staticmethod
    def get_command():
        return []

    def get_service(self):
        guard = dict(id="Qb115DownloaderGuard", name="qB → 115 隔离巡检（只读 qB，不依赖 OpenList）", trigger="interval",
                     func=self.guard, kwargs=dict(seconds=10, max_instances=1, coalesce=True, misfire_grace_time=30))
        if not self._enabled:  # disabled / invalid config / blocked reload: protect, never advance uploads
            return [guard] if self._guard_needed() else []
        return [dict(id="Qb115DownloaderPoll", name="qB → 115 状态推进", trigger="interval", func=self.poll,
                     kwargs=dict(seconds=self._poll_interval,
                                 max_instances=1, coalesce=True, misfire_grace_time=60)), guard]

    def get_api(self):
        return [dict(path="/tasks", endpoint=self.tasks_status, methods=["GET"], auth="apikey", summary="任务状态（不含凭据）"),
                dict(path="/control", endpoint=self.api_control, methods=["POST"], auth="apikey", summary="pause/start/retry/release/abandon/unsuspend；不提供删除")]

    def api_control(self, hash: str, action: str, confirm: str = ""):
        if not self._enabled or action not in {"pause", "start", "retry", "release", "abandon", "unsuspend"}:
            return {"success": False, "message": "仅支持启用状态下 pause/start/retry/release/abandon/unsuspend；清理待人工"}
        if action == "unsuspend" and confirm != hash:
            return {"success": False, "message": "unsuspend 需要显式 confirm=<完整 hash>；仅在人工核对 qB 任务/文件清单后使用"}
        try:
            if action == "abandon":
                operation = lambda tid, qb: self._engine.abandon(tid, qb)
            elif action == "unsuspend":
                operation = lambda tid, qb: self._engine.unsuspend(tid, qb, confirm=True)
            else:
                operation = lambda tid, qb: self._engine.control(tid, action, qb)
            success = self._control(hash, operation)
        except Exception:
            success = False
        return {"success": success, "message": "已处理" if success else "未确认；已交接/未知状态禁止重传，请人工核对"}

    def get_form(self):
        fields = [
            ("enabled", "启用插件（默认关闭）", "VSwitch"),
            ("physical_downloader", "已配置物理 qB 下载器名称（不使用默认实例）", "VTextField"),
            ("local_root", "MP 可读的独立本地目录（按物理 qB 路径映射）", "VTextField"),
            ("staging_path", "OpenList 隔离暂存目录（必须预建，不受 CLM 监控）", "VTextField"),
            ("target_path", "OpenList CLM 监控目录（必须预建）", "VTextField"),
            ("credential_source", "凭据来源插件 ID：U115OfflineDownloader / CloudLinkMonitor", "VTextField"),
            ("low_water_gib", "最低本地空闲空间 GiB（另加全任务大小预留）", "VTextField"),
            ("poll_interval", "轮询秒数（最少 10）", "VTextField"),
            ("put_timeout_seconds", "单文件上传读超时秒数（0=按文件大小自动估算，至少 600 秒）", "VTextField"),
            ("raw_url_allowed_hosts", "额外允许的 115 CDN 主机（逗号分隔；.example.com=含子域；默认仅 OpenList 同主机与 115.com/115cdn.com/115cdn.net）", "VTextField"),
            ("isolation_confirmed", "我已确认本地/暂存目录不受其他监控、自动清理；保种目录不自动整理", "VSwitch"),
        ]
        controls = [dict(component="VCol", props={"cols": 12}, content=[
            dict(component=component, props={"model": model, "label": label})]) for model, label, component in fields]
        warning = "只处理明确选择 qB → 115 的任务，不改订阅/站点/默认下载器。真实上传后仍保种；最终入库确认、源文件/暂存清理待人工。本版永不删除。仅 v1 种子，磁力/未知大小拒绝。"
        return [dict(component="VAlert", props={"type": "warning", "variant": "tonal"}, text=warning),
                dict(component="VForm", content=[dict(component="VRow", content=controls)])], dict(
                    enabled=False, physical_downloader="", local_root="", staging_path="/115/.qb115-staging",
                    target_path="/115/媒体库监控目录", credential_source="U115OfflineDownloader",
                    low_water_gib=10, poll_interval=30, put_timeout_seconds=0, raw_url_allowed_hosts="",
                    isolation_confirmed=False)

    def get_page(self):
        labels = dict(admitted="提交待确认", downloading="下载中", uploading="上传中", verifying="待校验",
                      commit_pending="交接结果待确认", handed_off="已交接", waiting_library="等待入库（本地保种中）",
                      suspended="挂起待人工", failed="失败/退避中", untracked="已停跟踪（保留文件）")
        status = self.tasks_status()
        rows = []
        for task in status["tasks"]:
            text = (f'{"🔴 隔离失效（已暂停并保持） | " if task["quarantined"] else ""}{task["name"]} | {task["hash"]} | {labels.get(task["state"], task["state"])}'
                    f' | qB {task["qb_progress"] if task["qb_progress"] is not None else "未知"}%'
                    f' | {"已暂停" if task["paused"] else task["qb_state"]} | {task["error"]} | 清理待人工')
            rows.append(dict(component="div", props={"class": "pa-2 text-break"}, text=text))
        return [dict(component="VAlert", props={"type": "warning", "variant": "tonal"},
                     text="真实 qB 进度不代表云端可整理。上传→校验→整批交接→等待入库；本地继续保种。最终路径/入库确认与清理待人工，永不自动删除。"),
                dict(component="div", text=status["blocked"]), *rows]
