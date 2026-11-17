"""Safe, standalone qB → 115 state machine (no MoviePilot imports)."""


import copy
import hashlib
import logging
import os
import re
import stat
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

MEDIA_SUFFIXES = {".mkv", ".mp4", ".m4v", ".avi", ".ts", ".m2ts", ".mov", ".wmv",
                  ".srt", ".ass", ".ssa", ".sup", ".sub", ".idx", ".vtt", ".flac", ".mp3"}


def relative_name(name):
    if not isinstance(name, str) or not name or "\\" in name or any(ord(c) < 32 for c in name):
        raise ValueError("unsafe path")
    parts = name.split("/")
    if any(p in {"", ".", ".."} for p in parts):
        raise ValueError("unsafe path")
    return parts


@contextmanager
def open_source(root, name):
    """Open by dirfd/O_NOFOLLOW: no symlinks, including ancestor directories."""
    parts = relative_name(name)
    if PurePosixPath(name).suffix.lower() not in MEDIA_SUFFIXES:
        raise ValueError("non-media file refused")
    root = Path(root)
    if not root.is_absolute():
        raise ValueError("absolute local root required")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in list(root.parts[1:]) + parts[:-1]:
            relative_name(part)
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        filefd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        with os.fdopen(filefd, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ValueError("regular file required")
            yield stream
    finally:
        os.close(fd)


def file_signature(s):
    """ino/size/mtime_ns only.

    st_dev and st_ctime_ns are deliberately excluded: a new hard link or chmod bumps ctime and a
    btrfs subvolume can report another st_dev without the content changing, which used to suspend
    already-uploaded tasks. Trade-off: a same-size rewrite that restores mtime_ns and keeps the
    inode is no longer detected through metadata alone; content changes still change size/mtime/ino.
    """
    return [s.st_ino, s.st_size, s.st_mtime_ns]


def check_media_name(name):
    relative_name(name)
    if PurePosixPath(name).suffix.lower() not in MEDIA_SUFFIXES:
        raise ValueError("non-media file refused")


LOG = logging.getLogger("qb115downloader")


def log_event(event, task, **codes):
    """Structured line: fixed event code + hash/state only. Never URLs, tokens or exception text."""
    LOG.info("qb115 event=%s hash=%s state=%s paused=%s quarantined=%s%s", event, task.get("hash"),
             task.get("state"), task.get("paused"), bool(task.get("quarantined")),
             "".join(" %s=%s" % (k, v) for k, v in sorted(codes.items())
                     if isinstance(v, (int, bool)) or (isinstance(v, str) and re.fullmatch(r"[a-z_]{1,24}", v))))


class ConfigError(ValueError):
    """A configuration problem with a FIXED, user-readable message (never a URL, host, token or response text)."""


QUARANTINE_MESSAGE = ("🔴 隔离失效：任务带有全局整理标签或专用标签丢失，已暂停并保持，不再上传/交接；"
                      "按 README 人工核对（补回 mp-qb115 标签、确认无全局标签）后再用 release 解除")
UNTAG_NOTES = {"removed": "；已摘除全局整理标签（可逆，未删除任何任务/文件）",
               "failed": "；⚠ 摘除全局标签失败，请立即在 qB 人工摘除该任务的全局标签",
               "none": ""}
PUT_QUIET_SECONDS = 300
SUSPEND_NOTES = {"ok": "；已请求暂停 qB（最佳努力，请在 qB 核对）",
                 "failed": "；⚠ 暂停 qB 失败，该任务可能仍在 qB 下载/保种，请人工在 qB 暂停",
                 "none": ""}
ABANDON_MESSAGE = ("已人工放弃跟踪（abandon）：保留本地文件、云端文件和 qB 任务，预留不释放，不再上传/交接；"
                   "qB 里是否仍在做种请自行处理")
UNTRACKED_GUARD_MESSAGE = "已停跟踪任务被补上全局整理标签：已暂停"
STICKY_STATES = {"untracked", "suspended", "external_missing"}  # only an explicit, named transition may leave or enter these


def norm_path(path):
    """qB may or may not report a trailing slash; compare and store paths without it."""
    text = str(path or "")
    return text.rstrip("/") or ("/" if text else "")


class Engine:
    _process_worker = threading.Lock()

    def __init__(self, tasks, save, free, low_water=10 * 1024**3, clock=None):
        if not isinstance(low_water, int) or low_water < 0:
            raise ValueError("nonnegative integer low water required")
        self.tasks = copy.deepcopy(tasks)
        for task in self.tasks.values():
            for item in task.get("manifest") or []:
                signature = item.get("signature")
                if isinstance(signature, list) and len(signature) == 5:  # legacy dev/ino/size/mtime/ctime
                    item["signature"] = [signature[1], signature[2], signature[3]]
        self.save = save
        self.free = free
        self.low_water = low_water
        self.lock = threading.RLock()
        self.worker = self._process_worker
        self.stopped = threading.Event()
        self.clock = clock or (lambda: time.time())  # wall clock: next_retry/quiet_until survive restarts
        self.guard_failed = False

    # -- persistence: every ledger read-copy and every write happens under self.lock ---------------

    def flush(self):
        with self.lock:
            self.save(copy.deepcopy(self.tasks))

    def _set(self, task, _force=None, **fields):
        """Persist ``fields``. ``untracked``/``suspended`` are sticky: a state write from an in-flight worker
        (``_process``/``_backoff``/``_settle_after_move`` run outside the lock) can never undo a tombstone or a
        safety suspension. Only a named authority may cross: ``untrack`` (remove/abandon: any -> untracked),
        ``suspend`` (-> suspended, never out of untracked), ``unsuspend`` (suspended -> new state).
        A denied transition drops ``state`` and its ``error`` text; the other fields are still written."""
        with self.lock:
            if "state" in fields and fields["state"] != task.get("state"):
                current = task.get("state")
                allowed = (_force == "untrack" or (_force == "suspend" and current != "untracked")
                           or (_force == "unsuspend" and current == "suspended")
                           or (_force == "external_missing" and current != "untracked")
                           or current not in STICKY_STATES)
                if not allowed:
                    fields.pop("state")
                    fields.pop("error", None)
            changed = "state" in fields and fields["state"] != task.get("state")
            task.update(fields)
            self.flush()
            if changed:
                log_event("state", task)

    def _suspend(self, task, message, qb=None):
        """Suspend the task. When ``qb`` is the instance the task is pinned to, also ask qB to pause the hash
        (best effort, the outcome is written into the message); a different instance is never touched."""
        with self.lock:
            if task.get("state") == "untracked":
                return
        note = "none"
        if qb is not None and getattr(qb, "name", None) == task.get("physical_downloader"):
            note = "ok" if self._pause(qb, task["hash"]) else "failed"
        self._set(task, state="suspended", error=message + SUSPEND_NOTES[note], _force="suspend")

    @staticmethod
    def outstanding(task):
        """Space still to be consumed. Once the selected files are complete on disk (manifest frozen)
        free() already excludes them, so counting the reservation again would double-count forever."""
        return 0 if task.get("manifest") or task.get("reservation_released") else task["reserved"]

    def admit(self, tid, name, size, root, physical="", save_path="", layout=""):
        with self.lock:
            if not re.fullmatch(r"[a-f0-9]{40}", tid) or len(relative_name(name)) != 1:
                raise ValueError("invalid torrent identity")
            if tid in self.tasks:
                return self.tasks[tid]
            if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
                raise ValueError("known positive torrent size required; magnets unsupported")
            def overlaps(left, right):
                left, right = PurePosixPath(left), PurePosixPath(right)
                return left == right or left in right.parents or right in left.parents
            for existing in self.tasks.values():
                # Include legacy/tombstoned content, not just a bare name. A legacy torrent could itself
                # be named like a full hash, so nested new namespaces must also be refused.
                if overlaps(PurePosixPath(root) / name, PurePosixPath(existing["root"]) / existing["name"]):
                    raise ValueError("local batch path already reserved")
                if (physical and physical == existing.get("physical_downloader") and save_path
                        and existing.get("save_path") and overlaps(PurePosixPath(save_path) / name,
                                                                   PurePosixPath(existing["save_path"]) / existing["name"])):
                    raise ValueError("physical batch path already reserved")
            reserved = sum(self.outstanding(t) for t in self.tasks.values())
            if self.free() - self.low_water - reserved < size:
                raise ValueError("insufficient unreserved local space")
            # save_path = the path EXPECTED from the configuration (qB path mapping, trailing slash trimmed). Every
            # snapshot proves qB's real save_path equals it (adapter round-trip) before anything is uploaded.
            task = dict(hash=tid, name=name, reserved=size, root=str(root), state="admitted",
                        physical_downloader=physical, save_path=norm_path(save_path), layout=layout,
                        paused=False, attempts=0, next_retry=0, manifest=[], error="")
            self.tasks[tid] = task
            self.flush()
            return task

    def capture_billing(self, tid, site, factor, resource):
        """Freeze pre-submission facts. Acceptance must independently bind the exact hash/resource/site."""
        with self.lock:
            task = self.tasks.get(tid)
            if not task or task.get("billing") or not isinstance(site, int) or isinstance(site, bool) or site <= 0:
                return False
            if not isinstance(factor, (int, float)) or isinstance(factor, bool) or not 0 <= factor < float("inf"):
                factor = None
            self._set(task, billing=dict(site=site, downloadvolumefactor=factor, resource=resource,
                                        accepted=False, source="submission"))
            return True

    def confirm_billing(self, tid, site, resource):
        with self.lock:
            task = self.tasks.get(tid)
            billing = task.get("billing") if task else None
            if not billing or billing["site"] != site or billing["resource"] != resource:
                return False
            billing = dict(billing, accepted=True)
            self._set(task, billing=billing)
            return True

    @staticmethod
    def cleanup_policy(task, enabled):
        billing = task.get("billing") or {}
        if not enabled:
            return "disabled"
        if not billing.get("accepted") or not billing.get("site"):
            return "billing_unknown"
        factor = billing.get("downloadvolumefactor")
        if type(factor) not in {int, float} or not 0 <= factor < float("inf"):
            return "billing_unknown"
        if factor != 0:
            return "keep_seeding"
        if task.get("state") != "waiting_library" or not task.get("commit_intent"):
            return "not_handed_off"
        if task.get("paused") or task.get("quarantined"):
            return "held"
        return ""

    @staticmethod
    def _cleanup_failure(err):
        # Whitelist only fixed internal messages. Never surface provider exception text/URLs.
        reasons = {
            "cleanup ledger directory overlap": "ledger_content_overlap",
            "cleanup qB content overlap": "qb_content_overlap",
            "cleanup hash readded": "qb_generation_changed",
            "cleanup qB generation mismatch": "qb_generation_changed",
            "cleanup qB identity/files/tags changed": "qb_files_or_isolation_changed",
            "cleanup source manifest mismatch": "qb_selection_changed",
            "cleanup local fingerprint mismatch": "local_fingerprint_changed",
            "fresh full cloud sha1 required": "cloud_sha1_unavailable_or_mismatch",
            "final cloud mapping unavailable": "final_mapping_missing",
            "cleanup instance binding mismatch": "instance_binding_changed",
        }
        if getattr(err, "transient", False):
            return "cloud_unavailable"
        if getattr(err, "missing", False):
            return "cloud_mapping_missing"
        if isinstance(err, FileNotFoundError):
            return "local_source_missing"
        return reasons.get(str(err), "verification_failed")

    def cleanup_preview(self, tid, qb, cloud, enabled=False, resolve_final=None):
        """Read-only, including full local SHA1 and fresh cloud verify. No ledger or external writes."""
        with self.lock:
            task = copy.deepcopy(self.tasks.get(tid))
        if not task:
            return dict(hash=tid, eligible=False, reason="unknown_task", files=[])
        try:
            files = [dict(path=str(Path(task["root"]) / i["source"]), size=i["size"])
                     for i in self._cleanup_manifest(task)]
        except Exception:
            return dict(hash=tid, eligible=False, reason="manifest_invalid_or_missing", files=[])
        reason = ("already_cleaned" if task.get("cleanup_state") == "done" else
                  "external_missing" if task["state"] == "external_missing" else self.cleanup_policy(task, enabled))
        if not reason:
            try:
                self._cleanup_checks(task, qb, cloud, resolve_final)
            except Exception as err:
                reason = self._cleanup_failure(err)
        return dict(hash=tid, eligible=not bool(reason), reason=reason or "verified_free", files=files)

    @staticmethod
    def _hash_source(task, item):
        with open_source(task["root"], item["source"]) as stream:
            before = os.fstat(stream.fileno())
            digest = hashlib.sha1()
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
            if (file_signature(before) != item["signature"] or file_signature(os.fstat(stream.fileno())) != item["signature"]
                    or digest.hexdigest() != item["sha1"] or before.st_size != item["size"]):
                raise ValueError("cleanup local fingerprint mismatch")

    @staticmethod
    def _paths_overlap(left, right):
        left, right = PurePosixPath(left), PurePosixPath(right)
        return left == right or left in right.parents or right in left.parents

    @staticmethod
    def _cleanup_manifest(task):
        root = task.get("root") or ""
        if (not root.startswith("/") or root == "/"
                or any(root == p or root.startswith(p + "/") for p in ("/115", "/115_rclone", "/quark_rclone", "/vol02"))):
            raise ValueError("cleanup invalid root")
        relative_name(root[1:])
        relative_name(task["target"][1:])
        if not task["target"].startswith("/115/"):
            raise ValueError("cleanup invalid target")
        manifest = task.get("manifest") or []
        sources, remotes = set(), set()
        for item in manifest:
            parts = relative_name(item["source"])
            relative_name(item["remote"])
            if parts[0] != task["name"] or item["source"] in sources or item["remote"] in remotes:
                raise ValueError("cleanup invalid manifest")
            if (not re.fullmatch(r"[a-f0-9]{40}", item["sha1"]) or len(item["signature"]) != 3
                    or item["signature"][1] != item["size"] or item["size"] <= 0):
                raise ValueError("cleanup invalid fingerprint")
            sources.add(item["source"])
            remotes.add(item["remote"])
        if not manifest:
            raise ValueError("cleanup empty manifest")
        return manifest

    @staticmethod
    def qb_file_binding(snapshot):
        return sorted([[f["name"], f["size"], f["priority"]] for f in snapshot["files"]])

    def _cleanup_exclusive(self, task, qb):
        inventory = qb.cleanup_inventory()  # finish network IO before taking the latest ledger copy
        with self.lock:
            others = copy.deepcopy([t for h, t in self.tasks.items() if h != task["hash"]])
        own = str(PurePosixPath(task["root"]) / task["name"])
        for other in others:
            if not str(other.get("root") or "").startswith("/") or len(relative_name(other.get("name"))) != 1:
                raise ValueError("cleanup ledger directory overlap")
            relative_name(other["root"][1:])
            if (not other.get("root") or not other.get("name")
                    or self._paths_overlap(str(PurePosixPath(task["root"]) / task["name"]),
                                           str(PurePosixPath(other["root"]) / other["name"]))):
                raise ValueError("cleanup ledger directory overlap")
        for other in inventory:  # ALL qB tasks, never just our tag or hashes
            if other.get("hash") == task["hash"]:
                if other.get("added_on") != task.get("qb_identity"):
                    raise ValueError("cleanup hash readded")
                continue
            path = other.get("save_path")
            content = other.get("content_path") or path  # unknown content => conservatively own save_path
            if path and content:
                relative_name(path[1:])
                relative_name(content[1:])
            if (not path or not path.startswith("/") or not content.startswith("/")
                    or PurePosixPath(path) == PurePosixPath(own) or PurePosixPath(own) in PurePosixPath(path).parents
                    or self._paths_overlap(own, content)):
                raise ValueError("cleanup qB content overlap")

    @staticmethod
    def _verify_cloud(task, item, cloud, resolve_final=None):
        """Deletion requires a fresh complete SHA1, not merely OpenList's readability/head check."""
        path = task["target"] + "/" + item["remote"]
        try:
            result = cloud.verify(path, item)
        except Exception as err:
            # Only a definitive missing object may use the exact source-history association.
            if not getattr(err, "missing", False) or resolve_final is None:
                raise
            path = resolve_final(task, item)
            if not isinstance(path, str) or not path.startswith("/115/"):
                raise ValueError("final cloud mapping unavailable")
            relative_name(path[1:])
            result = cloud.verify(path, item)
        if not isinstance(result, dict) or str(result.get("sha1") or "").lower() != item["sha1"]:
            raise ValueError("fresh full cloud sha1 required")
        return path

    @classmethod
    def _cheap_qb_binding(cls, task, qb):
        """Fresh destructive-operation gate; no local hashing or cloud IO. Also used inside the RPC adapter."""
        if cls._binding_error(task, qb):
            raise ValueError("cleanup missing manifest/binding")
        snapshot = qb.snapshot(task["hash"])
        identity = task.get("qb_identity")
        if (cls._binding_error(task, qb, snapshot) or type(identity) is not int or identity <= 0
                or snapshot.get("added_on") != identity):
            raise ValueError("cleanup qB generation mismatch")
        tags = sorted({t.strip() for t in str(snapshot.get("tags") or "").split(",") if t.strip()})
        if (snapshot.get("hash") != task["hash"] or snapshot.get("name") != task["name"]
                or snapshot.get("physical") != task["physical_downloader"]
                or cls.qb_file_binding(snapshot) != task.get("qb_files")
                or "mp-qb115" not in tags or ("cleanup_tags" in task and tags != task["cleanup_tags"])
                or qb.isolation_report([task["hash"]])):
            raise ValueError("cleanup qB identity/files/tags changed")
        selected = selected_files(snapshot["files"])
        if selected is None or sorted((i["name"], i["size"]) for i in selected) != sorted((i["source"], i["size"]) for i in task["manifest"]):
            raise ValueError("cleanup source manifest mismatch")
        return tags

    def _cleanup_checks(self, task, qb, cloud, resolve_final=None):
        manifest = self._cleanup_manifest(task)
        self._cleanup_exclusive(task, qb)
        tags = self._cheap_qb_binding(task, qb)
        for item in manifest:
            self._hash_source(task, item)
            self._verify_cloud(task, item, cloud, resolve_final)
        return tags

    @classmethod
    def _cleanup_local(cls, task, item):
        entry = (task.get("cleanup_files") or {}).get(item["source"])
        if not entry:
            cls._hash_source(task, item)
            return
        if not re.fullmatch(r"\.qb115-cleanup-[a-f0-9]{32}", entry.get("slot", "")):
            raise ValueError("invalid cleanup journal slot")
        parts = relative_name(item["source"])
        isolated = "/".join(parts[:-1] + [entry["slot"], parts[-1]])
        if os.path.lexists(os.path.join(task["root"], isolated)):
            cls._hash_source(task, dict(item, source=isolated))
        elif os.path.lexists(os.path.join(task["root"], item["source"])):
            cls._hash_source(task, item)
        elif entry.get("phase") not in {"unlink_intent", "unlinked"}:
            raise ValueError("cleanup file missing without unlink intent")

    def cleanup_free(self, tid, qb, cloud, enabled=False, resolve_final=None):
        if self.stopped.is_set() or not self.worker.acquire(blocking=False):
            return False
        try:
            task = self.tasks.get(tid)
            if not task or self.cleanup_policy(task, enabled):
                return False
            if task.get("cleanup_state") == "done":
                return True
            if self._binding_error(task, qb):
                raise ValueError("cleanup instance binding mismatch")
            recovering = task.get("cleanup_state") in {"remove_intent", "qb_absent"}
            if recovering:
                # Never reissue a delete RPC after a crash. Even a seemingly identical present hash is uncertain.
                if not qb.is_absent(tid):
                    self._set(task, cleanup_reason="remove_uncertain_or_readded")
                    return False
                self._cleanup_manifest(task)
                self._cleanup_exclusive(task, qb)
                for item in task["manifest"]:
                    self._cleanup_local(task, item)
                    self._verify_cloud(task, item, cloud, resolve_final)
            else:
                expected = copy.deepcopy(task)  # never let a late snapshot redefine what is safe to remove
                expected["cleanup_tags"] = self._cleanup_checks(expected, qb, cloud, resolve_final)
                self._cheap_qb_binding(expected, qb)
                self._cleanup_exclusive(task, qb)  # local/cloud SHA1 may have taken hours
                if self._held(task):
                    return False
                self._set(task, cleanup_state="remove_intent", cleanup_reason="verified_free")
                if not qb.remove_only(tid, expected) or not qb.is_absent(tid):
                    self._set(task, cleanup_reason="remove_unconfirmed")
                    return False
            self._set(task, cleanup_state="qb_absent", library_confirmed=True, library_reason="sha1_verified")
            for item in task["manifest"]:
                # Only frozen selected manifest files; isolate atomically before verifying the inode again.
                self._unlink_source(task, item, qb)
            self._set(task, cleanup_state="done", cleanup_reason="verified_free")
            return True
        except Exception as err:
            if self.tasks.get(tid):
                if self.tasks[tid].get("cleanup_reason") != "isolated_file_changed":
                    self._set(self.tasks[tid], cleanup_reason=self._cleanup_failure(err))
            return False
        finally:
            self.worker.release()

    @staticmethod
    @contextmanager
    def _source_parent(task, item):
        parts = list(Path(task["root"]).parts[1:]) + relative_name(item["source"])
        fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
        try:
            for part in parts[:-1]:
                relative_name(part)
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            yield fd, parts[-1]
        finally:
            os.close(fd)

    def _unlink_source(self, task, item, qb):
        """WAL -> private 0700 slot -> atomic rename -> rehash -> WAL -> single dirfd unlink.

        An attacker can replace the public source name between our checks and rename. That object is moved,
        NOT deleted: post-rename identity+SHA1 must match. Nothing ever recursively removes the private slot.
        """
        journal = copy.deepcopy(task.get("cleanup_files") or {})
        entry = journal.get(item["source"])
        with self._source_parent(task, item) as (parent, name):
            if entry is None:
                entry = dict(slot=".qb115-cleanup-" + uuid.uuid4().hex, phase="planned")
                journal[item["source"]] = entry
                self._set(task, cleanup_files=journal)
                os.mkdir(entry["slot"], mode=0o700, dir_fd=parent)
                os.fsync(parent)
                s = os.stat(entry["slot"], dir_fd=parent, follow_symlinks=False)
                entry["directory"] = [s.st_dev, s.st_ino]
                self._set(task, cleanup_files=journal)
            if not re.fullmatch(r"\.qb115-cleanup-[a-f0-9]{32}", entry["slot"]):
                raise ValueError("invalid cleanup journal slot")
            slot = os.open(entry["slot"], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            try:
                s = os.fstat(slot)
                if ([s.st_dev, s.st_ino] != entry.get("directory") or stat.S_IMODE(s.st_mode) != 0o700 or s.st_uid != os.getuid()):
                    raise ValueError("cleanup isolation directory changed")
                def exists(fd, part):
                    try:
                        os.stat(part, dir_fd=fd, follow_symlinks=False)
                        return True
                    except FileNotFoundError:
                        return False
                source_exists, isolated_exists = exists(parent, name), exists(slot, name)
                if entry["phase"] == "unlinked":
                    if source_exists or isolated_exists:
                        raise ValueError("cleanup deleted source reappeared")
                    return
                if source_exists and isolated_exists:
                    raise ValueError("cleanup source replaced after isolation")
                if not source_exists and not isolated_exists:
                    if entry["phase"] != "unlink_intent":
                        raise ValueError("cleanup isolation outcome uncertain")
                else:
                    if self._held(task) or not qb.is_absent(task["hash"]):
                        raise ValueError("cleanup held or hash readded")
                    self._cleanup_exclusive(task, qb)
                    if source_exists:
                        os.rename(name, name, src_dir_fd=parent, dst_dir_fd=slot)
                        os.fsync(parent)
                        os.fsync(slot)
                    filefd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=slot)
                    try:
                        with os.fdopen(filefd, "rb") as stream:
                            before = os.fstat(stream.fileno())
                            digest = hashlib.sha1()
                            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                                digest.update(chunk)
                            if (not stat.S_ISREG(before.st_mode) or file_signature(before) != item["signature"]
                                    or file_signature(os.fstat(stream.fileno())) != item["signature"] or digest.hexdigest() != item["sha1"]):
                                self._set(task, cleanup_reason="isolated_file_changed")
                                raise ValueError("isolated file changed")
                    except Exception:
                        self._set(task, cleanup_reason="isolated_file_changed")
                        raise
                    entry["phase"] = "unlink_intent"
                    self._set(task, cleanup_files=journal)
                    if self._held(task) or not qb.is_absent(task["hash"]):
                        raise ValueError("cleanup held or hash readded")
                    self._cleanup_exclusive(task, qb)  # rehash and rename can expose a new owner
                    os.unlink(name, dir_fd=slot)
                    os.fsync(slot)
                entry["phase"] = "unlinked"
                self._set(task, cleanup_files=journal)
            finally:
                os.close(slot)

    @staticmethod
    def _source_absent(task):
        root = task.get("root") or ""
        if not root.startswith("/") or root == "/" or any(root == p or root.startswith(p + "/") for p in ("/115", "/115_rclone", "/quark_rclone", "/vol02")):
            raise ValueError("unsafe local root")
        relative_name(root[1:])
        if len(relative_name(task["name"])) != 1:
            raise ValueError("invalid content namespace")
        try:
            with Engine._source_parent(task, dict(source=task["name"])) as (parent, name):
                os.stat(name, dir_fd=parent, follow_symlinks=False)
                return False  # includes empty directories, symlinks, partial and unselected data
        except FileNotFoundError:
            return True

    def reconcile(self, qb, cloud, resolve_final=None):
        """Prove external absence; never count someone else's deletion as plugin cleanup success."""
        if self.stopped.is_set() or not self.worker.acquire(blocking=False):
            return
        try:
            with self.lock:
                tasks = list(self.tasks.values())
            for task in tasks:
                if self.stopped.is_set():
                    break
                if task.get("cleanup_state") or task["state"] == "untracked":
                    continue  # the cleanup WAL alone owns in-flight local deletion recovery
                try:
                    if self._binding_error(task, qb):
                        raise ValueError("instance mismatch")
                    absent = qb.is_absent(task["hash"])
                except Exception:
                    self._set(task, reconcile_reason="qb_query_failed")
                    continue
                if absent:
                    try:
                        missing = self._source_absent(task)
                    except Exception:
                        self._set(task, reconcile_reason="local_absence_unconfirmed")
                        continue
                    if missing:
                        self._set(task, state="external_missing", reservation_released=True,
                                  reconcile_reason="external_missing", _force="external_missing",
                                  error="qB 与本地来源已被外部移除；非插件清理成功，不重下")
                    else:
                        self._set(task, reconcile_reason="qb_absent_source_present_or_partial")
                else:
                    self._set(task, reconcile_reason="qb_present")
                if task.get("commit_intent") and task.get("manifest"):
                    try:
                        self._cleanup_manifest(task)
                        paths = [self._verify_cloud(task, item, cloud, resolve_final) for item in task["manifest"]]
                        self._set(task, library_confirmed=True, verified_cloud_paths=paths, library_reason="sha1_verified")
                        if (task["state"] == "suspended" and task.get("recovery_waiting_cloud")
                                and not task["paused"] and not task.get("quarantined") and not absent):
                            self._cleanup_checks(task, qb, cloud, resolve_final)
                            self._set(task, state="waiting_library", recovery_waiting_cloud=False,
                                      error="交接恢复已按冻结本地/qB归属与云端SHA1核实；未重传", _force="unsuspend")
                    except Exception:
                        self._set(task, library_confirmed=False, library_reason="cloud_verification_pending")
        finally:
            self.worker.release()

    def rollback(self, tid):
        """Undo an admission whose qB add is confirmed to have NOT happened; nothing else is touched."""
        with self.lock:
            task = self.tasks.get(tid)
            if not task or task["state"] != "admitted" or task["manifest"]:
                return False
            del self.tasks[tid]
            self.flush()
            return True

    def remove(self, tid, qb, delete_file=True):
        if delete_file is not False:
            return False
        with self.lock:
            if tid not in self.tasks:
                return False
            task = self.tasks[tid]
            if task["state"] == "external_missing" or task.get("cleanup_state"):
                return False
            self._set(task, paused=True, state="untracked", _force="untrack")
            return self._pause(qb, tid)

    def abandon(self, tid, qb):
        """Manual, deletion-free "stop tracking": any tracked task -> ``untracked``.

        Local files, cloud files/stage and the qB task are kept, the reservation is NOT released, nothing is uploaded or
        handed off any more. qB is asked to pause only through the instance the task is pinned to (best effort). True when
        the tombstone was written; False for an unknown or already untracked hash."""
        with self.lock:
            task = self.tasks.get(tid)
            if not task or task["state"] in {"untracked", "external_missing"} or task.get("cleanup_state"):
                return False
            self._set(task, paused=True, state="untracked", error=ABANDON_MESSAGE, _force="untrack")
        if getattr(qb, "name", None) == task.get("physical_downloader"):
            self._pause(qb, tid)
        return True

    def unsuspend(self, tid, qb, confirm=False):
        """Explicit, checked release of a ``suspended`` task - the minimal safe version.

        Refused unless ``confirm is True`` AND all of: state suspended, not quarantined, a frozen manifest, no handoff
        ever started (``commit_intent``), the live qB task is isolated and still bound exactly as at admission (instance,
        save_path, layout), its selected files equal the frozen manifest (name+size) and every local source still has
        the frozen inode/size/mtime. On success the task goes back to ``downloading`` and STAYS PAUSED: qB is never
        resumed here, an explicit ``start`` is still required, and the next tick re-hashes against the frozen manifest."""
        if confirm is not True:
            return False
        with self.lock:
            task = copy.deepcopy(self.tasks.get(tid))
        if (not task or task["state"] != "suspended" or task.get("quarantined") or task.get("commit_intent")
                or not task.get("manifest")):
            return False
        try:
            if self._binding_error(task, qb):
                return False
            snapshot = qb.snapshot(tid)
            if self._binding_error(task, qb, snapshot):
                return False
            current = selected_files(snapshot["files"])
            manifest = task["manifest"]
            if current is None or sorted((i["name"], i["size"]) for i in current) != sorted(
                    (i["source"], i["size"]) for i in manifest):
                return False
            for item in manifest:
                with open_source(task["root"], item["source"]) as stream:
                    if file_signature(os.fstat(stream.fileno())) != item["signature"]:
                        return False
        except Exception:
            return False
        with self.lock:
            live = self.tasks.get(tid)
            if (not live or live["state"] != "suspended" or live.get("quarantined") or live.get("commit_intent")):
                return False
            self._set(live, state="downloading", paused=True, _force="unsuspend",
                      error="已人工核对后解除挂起（unsuspend）；保持暂停，需显式 start；下次推进会重新对冻结清单做 SHA1 核对")
            return True

    @staticmethod
    def _pause(qb, tid):
        try:
            return bool(qb.pause(tid))
        except Exception:
            return False

    @staticmethod
    def _binding_error(task, qb, snapshot=None):
        """The ledger pins physical downloader + actual qB save_path (+ layout); never follow config drift."""
        if not task.get("physical_downloader") or not task.get("save_path"):
            return "账本缺少物理下载器/qB 保存路径绑定；拒绝接管，人工核对"
        if getattr(qb, "name", None) != task["physical_downloader"]:
            return "当前配置的物理下载器与任务创建时不一致；拒绝用另一个实例接管同 hash，人工核对"
        if snapshot is not None:
            if norm_path(snapshot.get("root")) != norm_path(task.get("root")):
                return "qB 本地落地目录与账本批次不一致；挂起，人工核对"
            if norm_path(snapshot.get("save_path")) != norm_path(task["save_path"]):
                return "qB 实际保存路径与任务创建时不一致；挂起，人工核对"
            if task.get("layout") and snapshot.get("layout") != task["layout"]:
                return "qB 内容布局与任务创建时不一致；挂起，人工核对"
        return None

    def _quarantine(self, task, qb, codes=("global_tag",)):
        """Pause FIRST (directly, no snapshot: it must work exactly when isolation is lost), then undo the cause.

        The only corrective action is the reversible removal of the global TORRENT_TAG from this one task."""
        paused = self._pause(qb, task["hash"])
        untag = "none"
        if "global_tag" in codes:
            try:
                untag = qb.untag_global(task["hash"])
            except Exception:
                untag = "failed"
            if untag not in UNTAG_NOTES:
                untag = "failed"
        with self.lock:
            self._set(task, paused=True, quarantined=True, error=QUARANTINE_MESSAGE + UNTAG_NOTES[untag])
            log_event("quarantine", task, pause_ok=paused, untag=untag)

    def guard(self, qb):
        """Cheap sweep (qB only, no OpenList): pause + hold every task that lost native-tag isolation.

        Every tracked (non-untracked) task is swept - completed ones included, they are exactly what native TRANSFER
        picks up - in batches of 50 inside the adapter. ``untracked`` tasks that still carry our tag and whose source
        is still on disk get a lighter guard (pause + untag a re-appearing global tag, no quarantine flag): a tombstone
        must not turn into a native-transfer target. A failed sweep is never silent: ``guard_failed`` + event.
        An empty ledger is never an alarm."""
        with self.lock:
            snapshot = dict(self.tasks)
        tracked = {h: t for h, t in snapshot.items() if t["state"] != "untracked"}
        untracked = {h: t for h, t in snapshot.items() if t["state"] == "untracked" and self._source_present(t)}
        if not tracked and not untracked:
            self.guard_failed = False
            return []
        try:
            report = qb.isolation_report(list(tracked) + list(untracked))
        except Exception:
            self.guard_failed = True
            LOG.warning("qb115 event=guard_failed")
            return []
        self.guard_failed = False
        acted = []
        for tid, codes in report.items():
            codes = [c for c in codes if c != "absent"]  # a hash gone from qB has nothing left to protect
            if tid in untracked:
                # own tag missing = a human took the task over (README: remove mp-qb115 first to hand it to MP)
                if "global_tag" in codes and "own_tag_missing" not in codes:
                    self._protect_untracked(untracked[tid], qb)
                    acted.append(tid)
                continue
            task = tracked.get(tid)
            if task is None or not codes:
                continue
            if task.get("quarantined") and "global_tag" not in codes:
                continue  # already held; only a (re-)appearing global tag needs another action
            self._quarantine(task, qb, codes)
            acted.append(tid)
        return acted

    @staticmethod
    def _source_present(task):
        try:
            return os.path.lexists(os.path.join(task["root"], task["name"]))
        except Exception:
            return False

    def _protect_untracked(self, task, qb):
        paused = self._pause(qb, task["hash"])
        try:
            untag = qb.untag_global(task["hash"])
        except Exception:
            untag = "failed"
        if untag not in UNTAG_NOTES:
            untag = "failed"
        with self.lock:
            self._set(task, paused=True, error=UNTRACKED_GUARD_MESSAGE + UNTAG_NOTES[untag])
            log_event("untracked_guard", task, pause_ok=paused, untag=untag)

    def control(self, tid, action, qb):
        with self.lock:
            task = self.tasks.get(tid)
            if (not task or task["state"] == "external_missing" or task.get("cleanup_state") == "done"
                    or (task.get("cleanup_state") and action != "pause")):
                return False
            mismatch = self._binding_error(task, qb)
            if mismatch:
                if action == "pause":
                    task["paused"] = True  # stop uploads, but never touch another qB instance
                self._suspend(task, mismatch)
                return False
            if action == "pause":
                self._set(task, paused=True)  # stop uploads even if qB pause RPC fails
                return self._pause(qb, tid)
            if action == "release":
                if not task.get("quarantined"):
                    return False
                try:  # hash present in qB + own tag present + no global tag; anything else keeps the hold
                    if qb.isolation_report([tid]):
                        return False
                except Exception:
                    return False
                self._set(task, quarantined=False, error="")  # stays paused until an explicit start
                return True
            if action in {"start", "retry"}:
                # suspended/admitted: qB selection was never confirmed (or a safety check failed) - resuming qB could
                # download the whole torrent. Only a human clears those, outside this API.
                if task["state"] in {"untracked", "suspended", "admitted"} or task.get("quarantined") or (
                        action == "retry" and task.get("commit_intent")):
                    return False
                try:
                    mismatch = self._binding_error(task, qb, qb.snapshot(tid))
                    if mismatch:
                        self._suspend(task, mismatch, qb)
                        return False
                    started = qb.start(tid)
                except Exception as err:
                    if getattr(err, "isolation_lost", False):
                        self._quarantine(task, qb)
                    return False
                if not started:
                    return False
                # clears an ordinary network backoff only; ``quiet_until`` (PUT outcome unknown) is NOT touched
                self._set(task, paused=False, next_retry=0, error="")
                return True
            return False

    def tick(self, qb, cloud, staging, monitor, now=None):
        if self.stopped.is_set() or not self.worker.acquire(blocking=False):
            return
        try:
            started = self.clock()
            base = started if now is None else now

            def at():
                """'Now', re-read whenever it matters: a PUT may block for hours inside one tick."""
                return base + max(0.0, self.clock() - started)
            with self.lock:
                tasks = list(self.tasks.values())
            for task in tasks:
                if self.stopped.is_set():  # reload/disable: release the worker, never start another task
                    break
                current = at()
                if (task["paused"] or task.get("quarantined") or task["state"] in {"waiting_library", "suspended", "external_missing"}
                        or task["next_retry"] > current or task.get("quiet_until", 0) > current):
                    continue
                if task["state"] == "admitted":
                    self._suspend(task, "qB 添加/选集确认前退出；不自动认领或重提，请人工核对", qb)
                    continue
                try:
                    if task.get("commit_intent") or task["state"] in {"commit_pending", "handed_off"}:
                        self._recover_commit(task, cloud, at, qb)
                    else:
                        self._process(task, qb, cloud, staging, monitor)
                except Exception as err:
                    if getattr(err, "isolation_lost", False):
                        self._quarantine(task, qb)
                        continue
                    refused = getattr(err, "raw_host_refused", False)
                    self._backoff(task, at(), quiet=PUT_QUIET_SECONDS if getattr(err, "put_uncertain", False) else 0,
                                  state="failed",
                                  message=("raw_url 主机不在允许名单；核对 115 CDN 域名后加入 raw_url_allowed_hosts（日志/页面不记录主机或 URL）"
                                           if refused else
                                           "任务失败：检查路径、qB、OpenList 可用性及远端冲突；凭据与原始错误不记录"))
        finally:
            self.worker.release()

    def _backoff(self, task, failed_at, quiet=0, state=None, message=""):
        """``failed_at`` is the moment the failure happened, never the start of the tick.

        The put-uncertain quiet period lives in its OWN field (``quiet_until``): start/retry may clear the ordinary
        exponential backoff (``next_retry``) but must never re-fire a PUT that could still be running."""
        with self.lock:
            attempts = task["attempts"] + 1
            delay = min(3600, 60 * 2 ** min(attempts - 1, 6))
            fields = dict(attempts=attempts, error=message, next_retry=failed_at + max(delay, quiet))
            if quiet:
                fields["quiet_until"] = failed_at + quiet
            if state:
                fields["state"] = state
            self._set(task, **fields)

    # -- three-state commit recovery: target / stage -------------------------------------------------

    def _recover_commit(self, task, cloud, at, qb=None):
        """commit_intent is persisted BEFORE move, so after a crash the move may or may not have happened.

        target present, stage gone  -> moved (maybe consumed since): verify tolerantly, wait for library
        target absent, stage whole  -> move never happened: replay a NON-overwriting move once
        target absent, stage absent -> unknown (CLM took it or lost): suspend, never re-upload
        target present AND stage present -> ambiguous: suspend
        Transient probe failures never suspend and never act; they only back off.
        """
        if self._held(task):
            return
        try:
            target, stage = task["target"], task.get("stage")
            has_target = cloud.stat(target) is not None
            has_stage = bool(stage) and cloud.stat(stage) is not None
            if has_target and not has_stage:
                self._settle_after_move(task, cloud)
            elif has_target and has_stage:
                self._suspend(task, "暂存与目标同时存在，交接状态不明；禁止重传/清理，人工核对", qb)
            elif has_stage:
                cloud.verify_batch(stage, task["manifest"])
                if self._held(task):  # stop/pause/suspend arrived while verifying: no replayed move
                    return
                stage_parent = str(PurePosixPath(stage).parent)
                monitor = str(PurePosixPath(target).parent)
                self._set(task, state="commit_pending")
                cloud.move(stage_parent, monitor, task["name"])
                self._set(task, state="handed_off")
                self._settle_after_move(task, cloud)
            else:
                self._set(task, recovery_waiting_cloud=True)
                self._suspend(task, "已发起交接，暂存与目标都不存在（可能已被 CLM 搬走）；禁止重传，等待精确src关联与完整SHA1核验", qb)
        except Exception as err:
            if getattr(err, "transient", False):
                self._backoff(task, at(), message="交接状态核对暂时失败，稍后重试；未重传、未挂起")
            else:
                self._suspend(task, "交接核对失败（暂存不完整或目标内容不符）；禁止自动重传/交接，人工核对", qb)

    def _settle_after_move(self, task, cloud):
        """A consumed/vanished target is NOT a failure: CLM may take the batch the moment it lands.

        One ``stat`` + one listing per manifest directory (names/sizes); the files were verified one by one in the
        stage before the move, and an in-bucket rename keeps their content."""
        target = task["target"]
        if cloud.stat(target) is None:
            self._set(task, state="waiting_library",
                      error="目标目录已不在监控目录（CLM 可能已取走）；等待入库，需按 SHA1/整理历史人工核对")
            return
        present = cloud.count_present(target, task["manifest"])
        note = "" if present == len(task["manifest"]) else "部分/全部文件已不在监控目录（CLM 可能已取走）；等待入库，需人工核对"
        self._set(task, state="waiting_library", error=note)

    def _process(self, task, qb, cloud, staging, monitor):
        for path in (staging, monitor):
            if not isinstance(path, str) or not path.startswith("/115/"):
                raise ValueError("same /115 storage required")
            relative_name(path[1:])
        if staging == monitor or staging.startswith(monitor + "/") or monitor.startswith(staging + "/"):
            raise ValueError("staging and monitor must be disjoint")
        snapshot = qb.snapshot(task["hash"])
        mismatch = self._binding_error(task, qb, snapshot)
        if mismatch:
            self._suspend(task, mismatch, qb)
            return
        if not snapshot or snapshot["root"] != task["root"]:
            raise ValueError("qB path changed or unavailable")
        ready_states = {"uploading", "stalledup", "forcedup", "pausedup", "stoppedup", "queuedup"}
        files = selected_files(snapshot["files"]) if str(snapshot.get("state") or "").lower() in ready_states else None
        if files is None:
            self._set(task, state="downloading")
            return
        for item in files:  # cheap path/suffix refusal BEFORE hashing multi-GB media
            check_media_name(item["name"])
        manifest = []
        for item in files:
            source_name = item["name"]
            parts = relative_name(source_name)
            remote = "/".join(parts[1:]) if len(parts) > 1 and parts[0] == task["name"] else source_name
            if self._held(task):
                return
            with open_source(task["root"], source_name) as stream:
                before = os.fstat(stream.fileno())
                digest = hashlib.sha1()
                head = stream.read(4096)
                digest.update(head)
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
                    if self._held(task):  # stop/pause/suspend/untrack: a multi-GB hash must not outlive the reload wait
                        return
                after = os.fstat(stream.fileno())
                if before.st_size != item["size"] or file_signature(before) != file_signature(after):
                    raise ValueError("source not stable")
            manifest.append(dict(source=source_name, remote=remote, size=item["size"],
                                 sha1=digest.hexdigest(), head=hashlib.sha1(head).hexdigest(),
                                 signature=file_signature(after)))
        if task["manifest"] and manifest != task["manifest"]:
            self._suspend(task, "已冻结上传清单与当前源文件/选集不一致；不替换清单、不自动重传，人工核对", qb)
            return
        selected_size = sum(f["size"] for f in manifest)
        if selected_size > task["reserved"]:
            raise ValueError("selected size exceeds reservation")
        stage_parent = staging + "/" + task["hash"]
        stage = stage_parent + "/" + task["name"]
        target = monitor + "/" + task["name"]
        if (task.get("stage") and task["stage"] != stage) or (task.get("target") and task["target"] != target):
            self._set(task, manifest=manifest, selected_size=selected_size)
            self._suspend(task, "暂存/目标目录已改变；保留原批次路径，拒绝重传与交接，人工核对", qb)
            return
        self._set(task, manifest=manifest, selected_size=selected_size, state="uploading", stage=stage, target=target)
        made, listings = set(), {}

        def mkdir_once(path):
            if path not in made:
                cloud.mkdir(path)
                made.add(path)

        def listing(directory):
            """One refreshed listing per directory per tick instead of a stat (listing) per file."""
            if directory not in listings:
                listings[directory] = {entry["name"]: entry for entry in cloud.list_dir(directory)}
            return listings[directory]
        mkdir_once(stage_parent)
        mkdir_once(stage)
        verified = set(task.get("verified") or [])
        for item in manifest:
            if self._held(task):
                return
            remote = stage + "/" + item["remote"]
            parent = str(PurePosixPath(remote).parent)
            # Only explicit torrent file ancestors, never walk local/cloud trees.
            current = stage
            for component in PurePosixPath(parent).relative_to(stage).parts:
                current += "/" + component
                mkdir_once(current)
            name = PurePosixPath(remote).name
            entry = listing(parent).get(name)
            if entry is None:
                with open_source(task["root"], item["source"]) as stream:
                    try:
                        cloud.put(remote, stream, item["size"], item["sha1"])
                    except Exception as err:
                        # Timeout/reset: the server may still be finishing this very upload. Never fire a
                        # concurrent second PUT to the same path; a quiet period + stat/verify decides next.
                        if getattr(err, "outcome_unknown", False):
                            err.put_uncertain = True
                        raise
                listing(parent)[name] = dict(name=name, size=item["size"], is_dir=False)
            elif (item["remote"] in verified and not entry.get("is_dir") and entry.get("size") == item["size"]):
                continue  # verified on an earlier attempt; the pre-commit verify_batch lists the stage again
            self._set(task, state="verifying")
            cloud.verify(remote, item)
            verified.add(item["remote"])
            self._set(task, verified=sorted(verified))
        # -- commit gate: all qB / OpenList / disk IO happens OUTSIDE self.lock; the lock only guards ledger reads,
        # ledger writes and the final flag re-check right before commit_intent is persisted.
        if self._held(task):
            return
        current_snapshot = qb.snapshot(task["hash"])  # may raise isolation_lost -> quarantine, no move
        mismatch = self._binding_error(task, qb, current_snapshot)
        if mismatch:
            self._suspend(task, mismatch, qb)
            return
        current = selected_files(current_snapshot["files"])
        expected = sorted((i["source"], i["size"]) for i in manifest)
        if current is None or sorted((i["name"], i["size"]) for i in current) != expected:
            self._suspend(task, "上传期间选中文件清单改变；保持暂存隔离，禁止自动交接/重传", qb)
            return
        for item in manifest:
            with open_source(task["root"], item["source"]) as stream:
                current_stat = os.fstat(stream.fileno())
                if file_signature(current_stat) != item["signature"]:
                    self._suspend(task, "上传后本地源文件已改变；禁止交接、重传与清理，人工核对", qb)
                    return
        cloud.verify_batch(stage, manifest)
        if cloud.stat(task["target"]) is not None:
            raise ValueError("destination batch already exists")
        with self.lock:
            if self._held(task):  # pause/quarantine/stop that arrived while we were verifying
                return
            self._set(task, state="commit_pending", commit_intent=True)  # write-ahead: never blindly replay a move
        cloud.move(stage_parent, monitor, task["name"])  # a pause from here on cannot recall the move (README)
        self._set(task, state="handed_off")
        self._settle_after_move(task, cloud)

    def _held(self, task):
        return bool(task["paused"] or task.get("quarantined") or task.get("state") in STICKY_STATES
                    or self.stopped.is_set())


def torrent_metadata(raw):
    """Bounded v1 bencode parser; only hash/name/size leave this function."""
    if not isinstance(raw, bytes) or len(raw) > 16 * 1024**2:
        raise ValueError("bounded torrent bytes required")
    position, info_raw = 0, None
    def parse(depth=0):
        nonlocal position, info_raw
        if depth > 32 or position >= len(raw):
            raise ValueError("invalid bencode")
        token = raw[position:position + 1]
        position += 1
        if token == b"i":
            end = raw.index(b"e", position)
            value = raw[position:end]
            if not re.fullmatch(rb"0|-?[1-9][0-9]*", value):
                raise ValueError("invalid integer")
            position = end + 1
            return int(value)
        if token in {b"d", b"l"}:
            result = {} if token == b"d" else []
            last = None
            while raw[position:position + 1] != b"e":
                if token == b"l":
                    result.append(parse(depth + 1))
                else:
                    key = parse(depth + 1)
                    if not isinstance(key, bytes) or (last is not None and key <= last):
                        raise ValueError("unordered/duplicate bencode key")
                    last = key
                    start = position
                    result[key] = parse(depth + 1)
                    if depth == 0 and key == b"info":
                        info_raw = raw[start:position]
            position += 1
            return result
        position -= 1
        colon = raw.index(b":", position)
        length_text = raw[position:colon]
        if not re.fullmatch(rb"0|[1-9][0-9]*", length_text):
            raise ValueError("invalid string length")
        length = int(length_text)
        position = colon + 1
        value = raw[position:position + length]
        position += length
        if len(value) != length:
            raise ValueError("truncated bencode")
        return value
    try:
        data = parse()
        info = data[b"info"]
        if position != len(raw) or not info_raw or b"meta version" in info or not info[b"pieces"] or len(info[b"pieces"]) % 20 or info[b"piece length"] <= 0:
            raise ValueError("only v1 torrents with known files are supported")
        name = info.get(b"name.utf-8", info[b"name"]).decode("utf-8")
        if len(relative_name(name)) != 1:
            raise ValueError("invalid torrent name")
        items = info.get(b"files") or [{b"length": info[b"length"], b"path": [name.encode()]}]
        total = 0
        paths = set()
        for item in items:
            components = [x.decode("utf-8") for x in item.get(b"path.utf-8", item[b"path"])]
            for component in components:
                if len(relative_name(component)) != 1:
                    raise ValueError("invalid torrent path")
            path = "/".join(components)
            if not path or path in paths or b"l" in item.get(b"attr", b""):
                raise ValueError("duplicate/symlink torrent path")
            paths.add(path)
            length = item[b"length"]
            if not isinstance(length, int) or length < 0:
                raise ValueError("invalid torrent length")
            total += length
        if total <= 0:
            raise ValueError("unknown torrent length")
        return hashlib.sha1(info_raw).hexdigest(), name, total
    except Exception:
        raise ValueError("invalid/unsupported torrent metadata") from None


def selected_files(files):
    """None means not ready/unknown; priority zero is deliberately unselected."""
    if not files:
        return None
    selected = [f for f in files if int(f.get("priority", -1)) > 0]
    if not selected or any(f.get("progress") != 1 or not isinstance(f.get("size"), int)
                           or f["size"] <= 0 for f in selected):
        return None
    return selected
