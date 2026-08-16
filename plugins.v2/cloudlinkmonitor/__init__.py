import datetime
import json
import re
import shutil
import socket
import threading
import traceback
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
from app.chain.storage import StorageChain
from app.chain.tmdb import TmdbChain
from app.chain.transfer import TransferChain
from app.core.config import settings
from app.core.context import MediaInfo
from app.core.event import eventmanager, Event
from app.core.metainfo import MetaInfoPath
from app.db.downloadhistory_oper import DownloadHistoryOper
from app.db.transferhistory_oper import TransferHistoryOper
from app.helper.directory import DirectoryHelper
from app.log import logger
from app.modules.filemanager import FileManagerModule
from app.plugins import _PluginBase
from app.schemas import NotificationType, TransferInfo, TransferDirectoryConf
from app.schemas.types import EventType, MediaType, SystemConfigKey
from app.utils.string import StringUtils
from app.utils.system import SystemUtils

lock = threading.Lock()


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
    plugin_version = "2.9.1"
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

        # 重置失败重试计数与快速探测指纹
        self._retry_counts = {}
        self._fast_fp = None
        self._busy = threading.Lock()

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
                for file_path in media_files + sub_files:
                    self.__handle_file(event_path=str(file_path), mon_path=mon_path, from_reconcile=True)
            if self._junk_clean:
                try:
                    self.__clean_junk_dirs()
                except Exception as e:
                    logger.error(f"清理垃圾目录失败：{str(e)}")
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

                # 元数据
                file_meta = MetaInfoPath(file_path)
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
                # 识别媒体信息（走chain层完整识别链：原生识别失败时自动回退AI辅助识别，
                # 辅助识别以原始文件名请求LLM并回填季集信息，解决复杂命名解析失败问题）
                mediainfo: MediaInfo = self.mediaChain.recognize_by_meta(file_meta)
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
                if not settings.SCRAP_FOLLOW_TMDB:
                    transfer_history = self.transferhis.get_by_type_tmdbid(tmdbid=mediainfo.tmdb_id,
                                                                           mtype=mediainfo.type.value)
                    if transfer_history:
                        mediainfo.title = transfer_history.title
                logger.info(f"{file_path.name} 识别为：{mediainfo.type.value} {mediainfo.title_year}")

                # 获取集数据
                if mediainfo.type == MediaType.TV:
                    episodes_info = self.tmdbchain.tmdb_episodes(tmdbid=mediainfo.tmdb_id,
                                                                 season=1 if file_meta.begin_season is None else file_meta.begin_season)
                else:
                    episodes_info = None

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

                # 刮削（字幕文件无需刮削）
                if self._scrape and not is_subtitle:
                    self.mediaChain.scrape_metadata(fileitem=transferinfo.target_diritem,
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
                    # 通知Strm助手生成（媒体文件生成strm，字幕文件由助手复制到strm目录）
                    self.eventmanager.send_event(EventType.PluginAction, {
                        'file_path': str(transferinfo.target_item.path),
                        'action': 'cloudstrm_file'
                    })

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
        }]

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
            "fast_interval": 0
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
