"""Direct calls to the already-running physical QbittorrentModule, no dispatcher."""
import re
import time
from pathlib import Path

TAG = "mp-qb115"
BATCH = 50  # hashes per qB query: guard/status must stay O(N/50) requests however long the ledger gets


class IsolationLost(ValueError):
    """Own tag missing / native global tag present. Matched by the engine through ``isolation_lost``."""
    isolation_lost = True


class NotAdded(ValueError):
    """qB add rejected AND the hash is confirmed absent: the caller may roll the reservation back."""
    not_added = True


def _tags(value):
    return {t.strip() for t in str(value or "").split(",") if t.strip()}


def _chunks(items, size=BATCH):
    items = list(items)
    for start in range(0, len(items), size):
        yield items[start:start + size]


class QbAdapter:
    # qB reports a freshly added hash asynchronously: poll briefly before declaring the task missing.
    retries = 5
    retry_interval = 0.4

    def __init__(self, module, physical_name, local_root, global_tag, task_root=None):
        if not physical_name or physical_name == "qB → 115":
            raise ValueError("explicit physical qB required")
        self.module, self.name = module, physical_name
        self.root = str(local_root)
        self.global_tag = global_tag
        self.task_root = task_root

    def _server(self):
        server = self.module.get_instance(self.name)
        if server is None:
            raise ValueError("physical qB unavailable")
        return server

    def local_root(self, tid):
        """Resolve a frozen ledger root; only unseen hashes get a new full-hash namespace.

        Without a ledger resolver this adapter retains its explicit, single-root API.
        Production always supplies the resolver, including after reload.
        """
        if not re.fullmatch(r"[a-f0-9]{40}", tid):
            raise ValueError("invalid torrent hash")
        bound = self.task_root(tid) if self.task_root is not None else self.root
        root = str(Path(self.root) / tid) if bound is None else str(bound)
        if not root.startswith("/") or any(part in {"", ".", ".."} for part in root[1:].split("/")):
            raise ValueError("absolute safe local task root required")
        current = Path("/")
        for part in Path(root).parts[1:]:
            current /= part
            if current.is_symlink() or (current.exists() and not current.is_dir()):
                raise ValueError("local task root must not traverse symlinks or files")
        return root

    def expected_save_path(self, tid=None):
        root = self.root if tid is None else self.local_root(tid)
        return str(self.module.normalize_path(Path(root), self.name))

    def guard(self):
        tags = _tags(self.global_tag())
        if not tags or TAG in tags:
            raise ValueError("native global tag isolation unavailable")
        return self._server()

    def preflight(self, tid):
        """Duplicate hash check; must run BEFORE the ledger reserves space or anything is added."""
        server = self.guard()
        existing, error = server.get_torrents(ids=tid)
        if error or existing:
            # Native duplicate branch adds TORRENT_TAG: never enter it deliberately.
            raise ValueError("qB duplicate or preflight unavailable; not adopted")

    def _global_tags(self):
        """Usable global TORRENT_TAG set, or None (empty / contains our own tag: native isolation cannot hold)."""
        tags = _tags(self.global_tag())
        return None if not tags or TAG in tags else tags

    def isolation_report(self, hashes):
        """{hash: [codes]} for every hash that is not cleanly isolated; queries qB in batches of ``BATCH``.

        codes: ``global_tag`` (native TORRENT_TAG present), ``own_tag_missing``, ``absent`` (hash not in qB),
        ``tag_config_unusable`` (global tag empty/contains ours: native TRANSFER would see everything).
        Raises ValueError when qB cannot be queried (callers must not treat that as 'clean')."""
        server = self._server()
        wanted = list(hashes)
        global_tags = self._global_tags()
        if global_tags is None:
            return {h: ["tag_config_unusable"] for h in wanted}
        report, seen = {}, set()
        for chunk in _chunks(wanted):
            torrents, error = server.get_torrents(ids=chunk)
            if error:
                raise ValueError("qB isolation check unavailable")
            for item in torrents:
                tid = item.get("hash")
                if tid not in chunk:
                    continue
                seen.add(tid)
                tags = _tags(item.get("tags"))
                codes = (["global_tag"] if tags & global_tags else []) + ([] if TAG in tags else ["own_tag_missing"])
                if codes:
                    report[tid] = codes
        for tid in wanted:
            if tid not in seen:
                report[tid] = ["absent"]
        return report

    def check_isolation(self, hashes):
        """Hashes (present in qB) that are NOT isolated from native auto-transfer."""
        report = self.isolation_report(hashes)
        return [h for h in hashes if any(code != "absent" for code in report.get(h, []))]

    def brief(self, hashes):
        """{hash: dict(progress, state, isolated)} in batches; no per-task file listing (status pages are O(N/50))."""
        server = self._server()
        global_tags = self._global_tags()
        result = {}
        for chunk in _chunks(hashes):
            torrents, error = server.get_torrents(ids=chunk)
            if error:
                raise ValueError("qB status unavailable")
            for item in torrents:
                tid = item.get("hash")
                if tid not in chunk:
                    continue
                tags = _tags(item.get("tags"))
                result[tid] = dict(progress=item.get("progress") or 0, state=item.get("state"),
                                   isolated=bool(global_tags is not None and TAG in tags and not tags & global_tags))
        return result

    def untag_global(self, tid):
        """Reversible mitigation: remove ONLY the global TORRENT_TAG from ONE task (never delete, never our own tag).

        Returns ``removed`` / ``none`` (the task carried no global tag) / ``failed`` (anything uncertain)."""
        global_tags = self._global_tags()
        if global_tags is None:
            return "failed"
        try:
            server = self._server()
            torrents, error = server.get_torrents(ids=tid)
            if error or len(torrents) != 1 or torrents[0].get("hash") != tid:
                return "failed"
            present = sorted(_tags(torrents[0].get("tags")) & global_tags)
            if not present:
                return "none"
            # native signature: Qbittorrent.remove_torrents_tag(self, ids, tag) -> bool (qbc.torrents_remove_tags)
            if not server.remove_torrents_tag(ids=tid, tag=present):
                return "failed"
            torrents, error = server.get_torrents(ids=tid)
            if error or any(_tags(t.get("tags")) & global_tags for t in torrents):
                return "failed"
            return "removed"
        except Exception:
            return "failed"

    def snapshot(self, tid):
        server = self.guard()
        torrents, error = server.get_torrents(ids=tid)
        if error or len(torrents) != 1 or torrents[0].get("hash") != tid:
            raise ValueError("qB task unavailable")
        item = torrents[0]
        tags = _tags(item.get("tags"))
        if TAG not in tags or tags & _tags(self.global_tag()):
            raise IsolationLost("qB task isolation lost")
        raw = item.get("save_path") or ""
        local = self.module.normalize_return_path(Path(raw), self.name)
        root = self.local_root(tid)
        expected = self.expected_save_path(tid)
        if local != root or raw.rstrip("/") != str(expected).rstrip("/"):
            raise ValueError("qB actual path does not roundtrip to isolated local root")
        return dict(name=item.get("name"), root=local, save_path=raw, physical=self.name,
                    layout=server.get_content_layout(), files=server.get_files(tid),
                    tags=item.get("tags"), state=item.get("state"), progress=item.get("progress") or 0)

    def pause(self, tid):
        """Direct pause, deliberately WITHOUT snapshot/guard: it has to work exactly when isolation is lost."""
        return self._server().stop_torrents(tid)

    def start(self, tid):
        self.snapshot(tid)
        return self.guard().start_torrents(tid)

    def _confirm_snapshot(self, tid):
        last = None
        for attempt in range(self.retries):
            try:
                return self.snapshot(tid)
            except IsolationLost:
                raise
            except ValueError as err:
                last = err
                if attempt + 1 < self.retries:
                    time.sleep(self.retry_interval)
        raise last

    def submit(self, content, tid, episodes=None, custom_words=None):
        server = self.guard()
        self.preflight(tid)
        success, _ = server.add_torrent(content=content, download_dir=self.expected_save_path(tid),
                                         is_paused=True, tag=[TAG], cookie=None, category=None,
                                         ignore_category_check=False)
        if not success:
            existing, error = server.get_torrents(ids=tid)
            if not error and not existing:
                raise NotAdded("qB add rejected; hash confirmed absent")
            raise ValueError("qB add rejected but state unconfirmed; no duplicate adoption")
        self._confirm_snapshot(tid)  # confirm hash, exclusive tags, actual mapping BEFORE resume
        if episodes:
            selected, _ = self.module._select_download_files(server, tid, episodes, custom_words)
            if not selected:
                raise ValueError("qB file selection not confirmed; task remains paused")
        elif not server.start_torrents(tid):
            raise ValueError("qB start not confirmed")
        return self.name, tid, server.get_content_layout(), "添加下载成功"
