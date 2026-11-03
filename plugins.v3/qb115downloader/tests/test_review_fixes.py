"""Regression tests for the independent review findings A-J (stdlib only, loopback only)."""
import copy
import hashlib
import io
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace

PLUGIN = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN))
import adapter
import core
from test_adapter import Native
from test_cloud import Server, cloud
from test_core import bencode
from test_plugin import load_plugin

TID = "a" * 40


class Lost(ValueError):
    isolation_lost = True


class Qb:
    """Fake adapter implementing the interface Engine/plugin use."""

    def __init__(self, root, files, name="physical"):
        self.root, self.files, self.name = root, files, name
        self.paused, self.started = [], []
        self.state, self.lost = "uploading", set()
        self.layout, self.save_path = "Original", root
        self.snapshot_error = None
        self.absent, self.own_missing = set(), set()
        self.untagged, self.untag_result = [], "removed"
        self.report_calls, self.brief_calls, self.snapshot_calls = [], [], []
        self.report_error = None
        self.order = []

    def expected_save_path(self):
        return self.root

    def snapshot(self, tid):
        self.snapshot_calls.append(tid)
        if self.snapshot_error:
            raise self.snapshot_error
        if tid in self.lost:
            raise Lost("qB task isolation lost")
        return dict(name="seed.partial", root=self.root, save_path=self.save_path, physical=self.name,
                    layout=self.layout, files=self.files, tags="mp-qb115", state=self.state, progress=1)

    def check_isolation(self, hashes):
        return [h for h in hashes if h in self.lost]

    def isolation_report(self, hashes):
        if self.report_error:
            raise self.report_error
        self.report_calls.append(list(hashes))
        report = {}
        for h in hashes:
            codes = [c for c, group in (("absent", self.absent), ("global_tag", self.lost),
                                        ("own_tag_missing", self.own_missing)) if h in group]
            if codes:
                report[h] = codes
        return report

    def untag_global(self, tid):
        self.order.append("untag")
        self.untagged.append(tid)
        if self.untag_result == "removed":
            self.lost.discard(tid)
        return self.untag_result

    def brief(self, hashes):
        self.brief_calls.append(list(hashes))
        return {h: dict(progress=1, state=self.state, isolated=h not in self.lost) for h in hashes}

    def pause(self, tid):
        self.order.append("pause")
        self.paused.append(tid)
        return True

    def start(self, tid):
        self.started.append(tid)
        return True


class Workflow(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.addCleanup(self.server.close)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        (Path(self.root) / "seed.partial").mkdir()
        self.files = []
        for name, data in [("a.mkv", b"actual-video" * 1000), ("a.srt", b"subtitle")]:
            (Path(self.root) / "seed.partial" / name).write_bytes(data)
            self.files.append(dict(name="seed.partial/" + name, size=len(data), progress=1, priority=1))
        self.qb = Qb(self.root, self.files)
        self.api = cloud.OpenList(self.server.url, "secret-token")
        self.persisted = {}
        self.engine = core.Engine({}, self.save, free=lambda: 10**9, low_water=0)
        self.tid = TID
        self.engine.admit(self.tid, "seed.partial", 30000, self.root, physical="physical",
                          save_path=self.root, layout="Original")
        self.engine.tasks[self.tid]["state"] = "downloading"
        self.engine.flush()

    def save(self, tasks):
        self.persisted = copy.deepcopy(tasks)

    def tick(self, now=100, qb=None, api=None):
        self.engine.tick(qb or self.qb, api or self.api, "/115/staging", "/115/watch", now=now)

    @property
    def task(self):
        return self.persisted[self.tid]


class A_PutTimeout(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.addCleanup(self.server.close)

    def test_put_has_its_own_long_read_timeout_while_other_requests_stay_short(self):
        api = cloud.OpenList(self.server.url, "secret-token", timeout=.2, put_timeout=5)
        self.server.put_delay = .6
        data = b"media" * 100
        api.put("/115/staging/a.mkv", io.BytesIO(data), len(data), hashlib.sha1(data).hexdigest())
        self.assertEqual(self.server.objects["/115/staging/a.mkv"], data)
        self.server.list_delay = .6
        with self.assertRaises(cloud.CloudError):
            api.list_dir("/115/staging")  # stat() no longer lists (fs/get only); listings keep the short timeout

    def test_put_timeout_defaults_scale_with_size_and_are_configurable(self):
        api = cloud.OpenList(self.server.url, "secret-token")
        self.assertGreaterEqual(api.put_timeout_for(0), 600)
        self.assertGreaterEqual(api.put_timeout_for(10 * 1024**3), 3 * 3600)
        self.assertEqual(cloud.OpenList(self.server.url, "t", put_timeout=77).put_timeout_for(10**12), 77)

    def test_timeout_is_a_transient_error_not_a_response_leak(self):
        api = cloud.OpenList(self.server.url, "secret-token", timeout=.2, put_timeout=.2)
        self.server.put_delay = .6
        with self.assertRaises(cloud.CloudError) as caught:
            api.put("/115/staging/a.mkv", io.BytesIO(b"x"), 1, hashlib.sha1(b"x").hexdigest())
        self.assertTrue(caught.exception.transient)
        self.assertNotIn("secret-token", str(caught.exception))


class A_EngineAfterPutTimeout(Workflow):
    def test_timeout_never_resends_immediately_and_reuses_remote_after_server_finishes(self):
        slow = cloud.OpenList(self.server.url, "secret-token", timeout=.2, put_timeout=.2)
        self.server.put_delay = .6
        self.tick(api=slow)
        self.assertEqual(len(self.server.puts), 1, "no immediate retransmission of the same path")
        self.assertEqual(self.task["state"], "failed")
        self.assertGreaterEqual(self.task["next_retry"], 100 + 300, "quiet period lets the old request finish")
        self.assertEqual(len(self.server.moves), 0)
        self.server.put_delay = 0
        deadline = time.time() + 3
        while "/115/staging/%s/seed.partial/a.mkv" % self.tid not in self.server.objects and time.time() < deadline:
            time.sleep(.02)
        self.tick(now=2000)
        self.assertEqual(len(self.server.puts), 2, "first file verified/reused via stat, only second file uploaded")
        self.assertEqual(len(self.server.moves), 1)
        self.assertEqual(self.task["state"], "waiting_library")


class B_AdmitOrdering(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.module = load_plugin()
        self.p = self.module.Qb115Downloader()
        self.p.init_plugin(dict(enabled=False, local_root=self.root, low_water_gib=0))
        self.p._enabled = True
        self.p._engine.free = lambda: 10**9
        self.raw = bencode({b"info": {b"name": b"seed.partial", b"piece length": 16384, b"pieces": b"x" * 20,
                                      b"files": [{b"length": 4, b"path": [b"a.mkv"]}]}})
        self.qb = Qb(self.root, [])
        self.qb.preflight = lambda tid: None
        self.submitted = []
        self.p._runtime = lambda: (self.qb, None)

    def test_existing_hash_in_physical_qb_is_rejected_before_any_reservation(self):
        def preflight(tid):
            raise ValueError("duplicate")
        self.qb.preflight = preflight
        self.qb.submit = lambda *a, **k: self.submitted.append(a)
        result = self.p.download(content=self.raw, downloader="qB → 115")
        self.assertIsNone(result[1])
        self.assertEqual(self.p._engine.tasks, {})
        self.assertEqual(self.p.data.get("tasks_v1") or {}, {})
        self.assertEqual(self.submitted, [])

    def test_add_confirmed_absent_rolls_back_ledger_and_reservation(self):
        def submit(*a, **k):
            error = ValueError("add rejected")
            error.not_added = True
            raise error
        self.qb.submit = submit
        result = self.p.download(content=self.raw, downloader="qB → 115")
        self.assertIsNone(result[1])
        self.assertEqual(self.p._engine.tasks, {})
        self.assertEqual(self.p.data.get("tasks_v1") or {}, {})

    def test_ambiguous_failure_keeps_suspended_reservation(self):
        def submit(*a, **k):
            raise ValueError("snapshot unavailable after add")
        self.qb.submit = submit
        self.p.download(content=self.raw, downloader="qB → 115")
        (task,) = self.p._engine.tasks.values()
        self.assertEqual(task["state"], "suspended")
        self.assertGreater(task["reserved"], 0)


class B_AdapterSubmit(unittest.TestCase):
    def native(self):
        native = Native()
        native.stopped = []
        native.stop_torrents = lambda tid: native.stopped.append(tid) or True
        return native

    def test_preflight_rejects_existing_hash_without_adding(self):
        native = self.native()
        native.entries = [dict(hash=TID, tags="MOVIEPILOT")]
        a = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "MOVIEPILOT")
        with self.assertRaises(ValueError):
            a.preflight(TID)
        native.entries = []
        a.preflight(TID)

    def test_add_rejected_and_hash_absent_is_marked_not_added(self):
        native = self.native()
        native.add_torrent = lambda **kw: (False, [])
        a = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "MOVIEPILOT")
        with self.assertRaises(ValueError) as caught:
            a.submit(b"torrent", TID)
        self.assertTrue(getattr(caught.exception, "not_added", False))

    def test_add_rejected_but_hash_present_is_ambiguous(self):
        native = self.native()
        calls = []
        def add(**kw):
            calls.append(kw)
            native.entries = [dict(hash=TID, tags="MOVIEPILOT")]
            return False, []
        native.add_torrent = add
        a = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "MOVIEPILOT")
        with self.assertRaises(ValueError) as caught:
            a.submit(b"torrent", TID)
        self.assertFalse(getattr(caught.exception, "not_added", False))

    def test_snapshot_after_add_is_retried_briefly(self):
        native = self.native()
        real = native.add_torrent
        state = dict(calls=0)
        def add(**kw):
            real(**kw)
            ready, native.entries = native.entries, []
            native.pending = ready
            return True, [TID]
        def get(ids=None):
            if getattr(native, "pending", None) is not None and ids == TID:
                state["calls"] += 1
                if state["calls"] > 3:  # 1 preflight (empty) + 2 misses before the task becomes visible
                    native.entries = native.pending
            return copy.deepcopy(native.entries), False
        native.add_torrent, native.get_torrents = add, get
        a = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "MOVIEPILOT")
        a.retry_interval = 0
        self.assertEqual(a.submit(b"torrent", TID)[1], TID)


class C_Quarantine(Workflow):
    def test_pause_works_directly_even_when_isolation_is_lost(self):
        native = B_AdapterSubmit().native()
        native.entries = [dict(hash=TID, name="seed.partial", tags="mp-qb115,MOVIEPILOT", save_path="/qb/isolated",
                               progress=1, state="uploading")]
        a = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "MOVIEPILOT")
        with self.assertRaises(ValueError) as caught:
            a.snapshot(TID)
        self.assertTrue(getattr(caught.exception, "isolation_lost", False))
        self.assertTrue(a.pause(TID))
        self.assertEqual(native.stopped, [TID])

    def test_check_isolation_flags_global_tag_missing_own_tag_and_unavailable_global_tag(self):
        native = B_AdapterSubmit().native()
        other = "b" * 40
        third = "c" * 40
        native.entries = [dict(hash=TID, tags="mp-qb115"), dict(hash=other, tags="mp-qb115,MOVIEPILOT"),
                          dict(hash=third, tags="MOVIEPILOT")]
        a = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "MOVIEPILOT")
        self.assertEqual(sorted(a.check_isolation([TID, other, third])), sorted([other, third]))
        broken = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "")
        self.assertEqual(sorted(broken.check_isolation([TID, other])), sorted([TID, other]))
        native.get_torrents = lambda ids=None: ([], True)
        with self.assertRaises(ValueError):
            a.check_isolation([TID])

    def test_guard_pauses_and_holds_a_task_that_gained_the_global_tag_even_after_handoff(self):
        self.engine.tasks[self.tid]["state"] = "waiting_library"
        self.qb.lost.add(self.tid)
        self.engine.guard(self.qb)
        self.assertEqual(self.qb.paused, [self.tid])
        self.assertTrue(self.task["quarantined"])
        self.assertTrue(self.task["paused"])
        self.assertIn("隔离", self.task["error"])
        self.assertFalse(self.engine.control(self.tid, "start", self.qb))
        self.assertEqual(self.qb.started, [])

    def test_quarantined_task_is_never_uploaded_or_handed_off(self):
        self.qb.lost.add(self.tid)
        self.engine.guard(self.qb)
        self.qb.lost.clear()
        self.tick()
        self.assertEqual(self.server.puts, [])
        self.assertEqual(self.server.moves, [])

    def test_isolation_lost_discovered_during_tick_pauses_instead_of_backoff(self):
        self.qb.snapshot_error = Lost("qB task isolation lost")
        self.tick()
        self.assertEqual(self.qb.paused, [self.tid])
        self.assertTrue(self.task["quarantined"])
        self.assertEqual(self.server.puts, [])
        self.assertNotEqual(self.task["state"], "failed")

    def test_isolation_lost_between_upload_and_commit_blocks_the_move(self):
        def lose():
            self.qb.lost.add(self.tid)
        self.server.after_put = lose
        self.tick()
        self.assertEqual(self.server.moves, [])
        self.assertTrue(self.task["quarantined"])
        self.assertIn(self.tid, self.qb.paused)


class C_Release(Workflow):
    """release/refuse paths added together with the quarantine feature (written after the code, green-first)."""

    def test_release_only_after_live_check_is_clean_and_stays_paused_until_start(self):
        self.qb.untag_result = "failed"  # the guard could not remove the global tag: it stays until 'lost.clear()'
        self.qb.lost.add(self.tid)
        self.engine.guard(self.qb)
        self.assertFalse(self.engine.control(self.tid, "release", self.qb))
        self.assertTrue(self.task["quarantined"])
        self.qb.lost.clear()
        self.assertTrue(self.engine.control(self.tid, "release", self.qb))
        self.assertFalse(self.task["quarantined"])
        self.assertTrue(self.task["paused"])
        self.assertEqual(self.qb.started, [])
        self.assertTrue(self.engine.control(self.tid, "start", self.qb))

    def test_start_hitting_lost_isolation_quarantines(self):
        class StartLost(Qb):
            def start(self, tid):
                raise Lost("lost")
        qb = StartLost(self.root, self.files)
        self.assertFalse(self.engine.control(self.tid, "start", qb))
        self.assertTrue(self.task["quarantined"])
        self.assertEqual(qb.paused, [self.tid])

    def test_tracked_resubmission_of_a_quarantined_task_is_refused(self):
        p = load_plugin().Qb115Downloader()
        p.init_plugin(dict(enabled=False, local_root=self.root, low_water_gib=0))
        p._enabled = True
        raw = bencode({b"info": {b"name": b"seed.partial", b"piece length": 16384, b"pieces": b"x" * 20,
                                 b"files": [{b"length": 4, b"path": [b"a.mkv"]}]}})
        tid, name, size = core.torrent_metadata(raw)
        p._engine.free = lambda: 10**9
        p._engine.admit(tid, name, size, self.root, physical="physical", save_path=self.root, layout="Original")
        p._engine.tasks[tid].update(state="waiting_library", quarantined=True)
        p._runtime = lambda: (self.qb, None)
        self.assertIsNone(p.download(content=raw, downloader="qB → 115")[1])


class C_PluginQuarantine(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.p = load_plugin().Qb115Downloader()
        self.p.init_plugin(dict(enabled=False, local_root=self.root, low_water_gib=0))
        self.p._enabled = True
        self.p._engine.free = lambda: 10**9
        self.qb = Qb(self.root, [])
        self.p._engine.admit(TID, "seed.partial", 10, self.root, physical="physical", save_path=self.root,
                             layout="Original")
        self.p._engine.tasks[TID]["state"] = "waiting_library"

        def runtime():
            raise AssertionError("guard must not need OpenList/_runtime")
        self.p._runtime = runtime
        self.p._qb_adapter = lambda: self.qb

    def test_guard_service_runs_without_openlist_and_status_is_red(self):
        import json
        services = {s["id"]: s for s in self.p.get_service()}
        self.assertIn("Qb115DownloaderGuard", services)
        self.assertLessEqual(services["Qb115DownloaderGuard"]["kwargs"]["seconds"], 15)
        self.qb.lost.add(TID)
        self.p.guard()
        self.assertEqual(self.qb.paused, [TID])
        row = self.p.tasks_status()["tasks"][0]
        self.assertTrue(row["quarantined"])
        self.assertIn("隔离失效", json.dumps(self.p.get_page(), ensure_ascii=False))


class D_Binding(Workflow):
    def test_other_physical_downloader_cannot_take_over_the_same_hash(self):
        other = Qb(self.root, self.files, name="other-qb")
        self.tick(qb=other)
        self.assertEqual(self.server.puts, [])
        self.assertEqual(self.task["state"], "suspended")
        self.assertEqual(other.paused, [])

    def test_control_refuses_to_touch_a_different_physical_downloader(self):
        other = Qb(self.root, self.files, name="other-qb")
        self.assertFalse(self.engine.control(self.tid, "pause", other))
        self.assertFalse(self.engine.control(self.tid, "start", other))
        self.assertEqual((other.paused, other.started), ([], []))
        self.assertTrue(self.task["paused"] or self.task["state"] == "suspended")

    def test_actual_qb_save_path_and_layout_changes_suspend(self):
        for attr, value in [("save_path", self.root + "-moved"), ("layout", "NoSubfolder")]:
            with self.subTest(attr=attr):
                qb = Qb(self.root, self.files)
                setattr(qb, attr, value)
                self.engine.tasks[self.tid].update(state="downloading", error="", next_retry=0)
                self.tick(qb=qb)
                self.assertEqual(self.task["state"], "suspended")
                self.assertEqual(self.server.puts, [])

    def test_ledger_without_binding_fields_is_not_trusted(self):
        for key in ("physical_downloader", "save_path"):
            del self.engine.tasks[self.tid][key]
            self.tick()
            self.assertEqual(self.task["state"], "suspended")
            self.assertEqual(self.server.puts, [])
            self.engine.tasks[self.tid].update(state="downloading", error="", physical_downloader="physical",
                                               save_path=self.root)

    def test_plugin_persists_binding_and_reload_drops_stale_adapter(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        qb = Qb(temp.name, [])
        qb.preflight = lambda tid: None
        qb.submit = lambda content, tid, **kw: ("physical", tid, "Original", "ok")
        p = load_plugin().Qb115Downloader()
        p.init_plugin(dict(enabled=False, local_root=temp.name, low_water_gib=0))
        p._enabled = True
        p._engine.free = lambda: 10**9
        p._runtime = lambda: (qb, None)
        raw = bencode({b"info": {b"name": b"seed.partial", b"piece length": 16384, b"pieces": b"x" * 20,
                                 b"files": [{b"length": 4, b"path": [b"a.mkv"]}]}})
        result = p.download(content=raw, downloader="qB → 115")
        task = p.data["tasks_v1"][result[1]]
        self.assertEqual((task["physical_downloader"], task["save_path"], task["layout"]),
                         ("physical", temp.name, "Original"))
        p._qb = object()
        p.init_plugin(dict(enabled=False, local_root=temp.name))
        self.assertIsNone(p._qb)


class E_MoviePilotContract(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.p = load_plugin().Qb115Downloader()
        self.p.init_plugin(dict(enabled=False, local_root=self.root, low_water_gib=0))
        self.p._enabled = True
        self.p._engine.free = lambda: 10**9
        self.qb = Qb(self.root, [])
        self.p._qb = self.qb
        for index, state in enumerate(["downloading", "uploading", "waiting_library", "failed"]):
            tid = chr(ord("a") + index) * 40
            self.p._engine.admit(tid, "seed%s" % index, 100, self.root, physical="physical",
                                 save_path=self.root, layout="Original")
            self.p._engine.tasks[tid]["state"] = state
        self.hashes = {state: chr(ord("a") + i) * 40 for i, state in
                       enumerate(["downloading", "uploading", "waiting_library", "failed"])}

    def listed(self, status, **kw):
        return {t.hash: t for t in self.p.list_torrents(status=status, downloader="qB → 115", **kw)}

    def test_chinese_downloading_enum_excludes_uploaded_waiting_library_tasks(self):
        rows = self.listed(SimpleNamespace(value="下载中"))
        self.assertEqual(set(rows), {self.hashes["downloading"], self.hashes["uploading"], self.hashes["failed"]})
        for row in rows.values():
            self.assertEqual(row.state, "downloading")
            self.assertLess(row.progress, 100)

    def test_chinese_completed_paused_and_transfer_aliases(self):
        self.assertEqual(set(self.listed("已完成")), {self.hashes["waiting_library"]})
        self.assertEqual(self.listed(SimpleNamespace(value="可转移")), {})
        self.p._engine.tasks[self.hashes["downloading"]]["paused"] = True
        self.assertEqual(set(self.listed("暂停")), {self.hashes["downloading"]})
        self.assertNotIn(self.hashes["downloading"], self.listed("downloading"))
        self.assertEqual(len(self.listed(None)), 4)

    def test_hashs_take_priority_over_status_like_native_module(self):
        rows = self.listed("downloading", hashs=[self.hashes["waiting_library"]])
        self.assertEqual(set(rows), {self.hashes["waiting_library"]})

    def test_actual_local_paths_are_reported_for_download_history(self):
        row = self.listed(None)[self.hashes["waiting_library"]]
        self.assertEqual(row.save_path, self.root)
        self.assertEqual(row.content_path, self.root + "/seed2")
        self.assertEqual(row.path, Path(self.root) / "seed2")

    def test_already_tracked_returns_moviepilot_already_exists_message(self):
        raw = bencode({b"info": {b"name": b"seed.partial", b"piece length": 16384, b"pieces": b"x" * 20,
                                 b"files": [{b"length": 4, b"path": [b"a.mkv"]}]}})
        tid, name, size = core.torrent_metadata(raw)
        self.p._engine.admit(tid, name, size, self.root, physical="physical", save_path=self.root, layout="Original")
        self.p._engine.tasks[tid].update(state="downloading", layout="Original")
        self.p._runtime = lambda: (self.qb, None)
        result = self.p.download(content=raw, downloader="qB → 115")
        self.assertEqual(result[:2], ("qB → 115", tid))
        self.assertEqual(result[3], "下载任务已存在")


class F_ControlWithoutOpenList(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.p = load_plugin().Qb115Downloader()
        self.p.init_plugin(dict(enabled=False, local_root=self.root, low_water_gib=0))
        self.p._enabled = True
        self.p._engine.free = lambda: 10**9
        self.qb = Qb(self.root, [])
        self.p._engine.admit(TID, "seed.partial", 10, self.root, physical="physical", save_path=self.root,
                             layout="Original")
        self.p._engine.tasks[TID]["state"] = "waiting_library"

        def runtime():
            raise ValueError("OpenList unreachable")
        self.p._runtime = runtime
        self.p._qb_adapter = lambda: self.qb
        self.assertIsNone(self.p._qb)

    def test_api_control_pause_and_start_do_not_depend_on_openlist(self):
        self.assertTrue(self.p.api_control(hash=TID, action="pause")["success"])
        self.assertEqual(self.qb.paused, [TID])
        self.assertTrue(self.p.api_control(hash=TID, action="start")["success"])
        self.assertEqual(self.qb.started, [TID])

    def test_moviepilot_stop_start_after_reload_create_adapter_on_demand(self):
        self.assertTrue(self.p.stop_torrents(TID, downloader="qB → 115"))
        self.assertTrue(self.p.start_torrents([TID], downloader="qB → 115"))
        self.assertEqual((self.qb.paused, self.qb.started), ([TID], [TID]))
        self.assertTrue(self.p.remove_torrents(TID, delete_file=False, downloader="qB → 115"))


class G_Locking(Workflow):
    def test_flush_snapshot_is_taken_under_engine_lock(self):
        entered, release = threading.Event(), threading.Event()

        class Slow:
            def __deepcopy__(self, memo):
                entered.set()
                release.wait(3)
                return Slow()

        self.engine.tasks[self.tid]["probe"] = Slow()
        errors = []

        def flusher():
            try:
                self.engine.flush()
            except Exception as err:  # RuntimeError: dictionary changed size during iteration
                errors.append(err)

        flush_thread = threading.Thread(target=flusher, daemon=True)
        flush_thread.start()
        self.assertTrue(entered.wait(2))
        new = "b" * 40
        admit_thread = threading.Thread(
            target=lambda: self.engine.admit(new, "other", 10, self.root, physical="physical",
                                             save_path=self.root, layout="Original"), daemon=True)
        admit_thread.start()
        admit_thread.join(.2)
        self.assertNotIn(new, self.engine.tasks, "admit must not write while a snapshot is being copied")
        release.set()
        flush_thread.join(3)
        admit_thread.join(3)
        self.assertEqual(errors, [])
        self.assertIn(new, self.engine.tasks)

    def test_long_upload_io_does_not_hold_the_engine_lock(self):
        seen = []

        def probe():
            def other_thread():
                got = self.engine.lock.acquire(timeout=1)
                seen.append(got)
                if got:
                    self.engine.lock.release()
            thread = threading.Thread(target=other_thread)
            thread.start()
            thread.join(2)
        self.server.after_put = probe  # runs inside the server while the worker waits for the PUT response
        self.tick()
        self.assertTrue(seen and all(seen), "status/control must stay responsive during a long PUT")

    def test_tick_only_mutates_tasks_and_saves_under_the_engine_lock(self):
        violations = []
        engine = self.engine

        class Trap(dict):
            def __setitem__(self, key, value):
                if not engine.lock._is_owned():
                    violations.append(key)
                super().__setitem__(key, value)

            def update(self, *a, **k):
                if not engine.lock._is_owned():
                    violations.append("update")
                super().update(*a, **k)

        engine.tasks[self.tid] = Trap(engine.tasks[self.tid])
        original = engine.save

        def save(tasks):
            if not engine.lock._is_owned():
                violations.append("save")
            original(tasks)

        engine.save = save
        self.tick()
        self.assertEqual(self.task["state"], "waiting_library")
        self.assertEqual(violations, [])


class H_FileSignature(Workflow):
    def test_signature_ignores_device_and_ctime_but_detects_content_identity(self):
        def stat(**kw):
            base = dict(st_dev=1, st_ino=2, st_size=3, st_mtime_ns=4, st_ctime_ns=5)
            base.update(kw)
            return SimpleNamespace(**base)
        self.assertEqual(core.file_signature(stat()), core.file_signature(stat(st_dev=9, st_ctime_ns=99)))
        for change in (dict(st_ino=7), dict(st_size=8), dict(st_mtime_ns=9)):
            self.assertNotEqual(core.file_signature(stat()), core.file_signature(stat(**change)))

    def test_hardlink_created_during_upload_does_not_make_task_look_modified(self):
        source = Path(self.root) / "seed.partial/a.mkv"
        link = Path(self.root) / "second-hardlink.mkv"
        self.server.after_put = lambda: link.exists() or os.link(source, link)
        self.tick()
        self.assertEqual(self.task["state"], "waiting_library")
        self.assertEqual(len(self.server.moves), 1)

    def test_legacy_five_field_signatures_are_migrated_on_load(self):
        self.tick()
        legacy = copy.deepcopy(self.persisted)
        expected = {}
        for item in legacy[self.tid]["manifest"]:
            st = os.stat(Path(self.root) / item["source"])
            item["signature"] = [st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns]
            expected[item["source"]] = [st.st_ino, st.st_size, st.st_mtime_ns]
        restored = core.Engine(legacy, self.save, free=lambda: 10**9, low_water=0)
        got = {i["source"]: i["signature"] for i in restored.tasks[self.tid]["manifest"]}
        self.assertEqual(got, expected)


class I_CommitRecovery(Workflow):
    def crash_before_move(self):
        def boom(*a):
            raise SystemExit("died after commit_intent was persisted, before move")
        self.api.move = boom
        with self.assertRaises(SystemExit):
            self.tick()
        self.assertTrue(self.task["commit_intent"])
        self.assertEqual(len(self.server.moves), 0)
        self.engine = core.Engine(self.persisted, self.save, free=lambda: 10**9, low_water=0)
        self.api = cloud.OpenList(self.server.url, "secret-token")

    def test_intent_persisted_but_not_moved_replays_a_non_overwriting_move_once(self):
        self.crash_before_move()
        self.tick(now=100)
        self.assertEqual(len(self.server.puts), 2, "no re-upload")
        self.assertEqual(len(self.server.moves), 1)
        self.assertFalse(self.server.moves[0]["overwrite"])
        self.assertEqual(self.task["state"], "waiting_library")
        self.tick(now=100)
        self.assertEqual(len(self.server.moves), 1)

    def test_incomplete_stage_is_not_moved_on_recovery(self):
        self.crash_before_move()
        del self.server.objects["/115/staging/%s/seed.partial/a.srt" % self.tid]
        self.tick(now=100)
        self.assertEqual(self.server.moves, [])
        self.assertEqual(self.task["state"], "suspended")

    def test_stage_and_target_both_absent_suspends(self):
        self.crash_before_move()
        for key in list(self.server.objects):
            if "seed.partial" in key:
                del self.server.objects[key]
        self.tick(now=100)
        self.assertEqual(self.task["state"], "suspended")
        self.assertEqual(self.server.moves, [])
        self.assertEqual(len(self.server.puts), 2)

    def test_transient_probe_failure_never_suspends_or_moves(self):
        self.crash_before_move()
        def broken(path):
            error = cloud.CloudError("OpenList transport/response failure")
            error.transient = True
            raise error
        self.api.stat = broken
        self.tick(now=100)
        self.assertEqual(self.server.moves, [])
        self.assertNotEqual(self.task["state"], "suspended")
        self.assertGreater(self.task["next_retry"], 100)

    def test_clm_consuming_target_right_after_move_is_waiting_not_failed(self):
        real = self.api.move

        def move_then_consume(*args):
            real(*args)
            for key in list(self.server.objects):
                if key.startswith("/115/watch/seed.partial"):
                    del self.server.objects[key]
        self.api.move = move_then_consume
        self.tick()
        self.assertEqual(len(self.server.moves), 1)
        self.assertEqual(self.task["state"], "waiting_library")
        self.tick(now=1000)
        self.assertEqual(len(self.server.puts), 2)


class J_Extras(Workflow):
    def test_reservation_not_counted_twice_after_selected_files_are_on_disk(self):
        engine = core.Engine({}, self.save, free=lambda: 100, low_water=10)
        engine.admit("a" * 40, "first", 60, self.root, physical="physical", save_path=self.root, layout="")
        engine.tasks["a" * 40].update(state="waiting_library", manifest=[dict(size=60)], selected_size=60)
        # free() already excludes the 60 bytes occupied by the finished files
        engine.free = lambda: 40
        engine.admit("b" * 40, "second", 30, self.root, physical="physical", save_path=self.root, layout="")
        with self.assertRaises(ValueError):
            engine.admit("c" * 40, "third", 30, self.root, physical="physical", save_path=self.root, layout="")

    def test_non_media_selection_is_rejected_before_hashing_any_file(self):
        (Path(self.root) / "seed.partial/readme.nfo").write_bytes(b"nfo")
        self.files.append(dict(name="seed.partial/readme.nfo", size=3, progress=1, priority=1))
        opened = []
        original = core.open_source

        def spy(root, name):
            opened.append(name)
            return original(root, name)
        core.open_source = spy
        self.addCleanup(setattr, core, "open_source", original)
        self.tick()
        self.assertEqual(opened, [])
        self.assertEqual(self.server.puts, [])
        self.assertEqual(self.task["state"], "failed")

    def test_structured_log_has_hash_and_state_but_no_secret_or_url(self):
        with self.assertLogs("qb115downloader", "INFO") as logs:
            self.tick()
        text = "\n".join(logs.output)
        self.assertIn(self.tid, text)
        self.assertIn("waiting_library", text)
        for secret in ("secret-token", "http://", "PRIVATE-PASSKEY", "raw_url"):
            self.assertNotIn(secret, text)


class J_StorageValidationCache(unittest.TestCase):
    def test_runtime_does_not_relist_cloud_roots_on_every_poll(self):
        from types import ModuleType
        from unittest.mock import patch
        module = load_plugin()
        p = module.Qb115Downloader()
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        p.init_plugin(dict(enabled=False, local_root=temp.name, physical_downloader="physical", isolation_confirmed=True,
                           put_timeout_seconds=900))
        p.configs["U115OfflineDownloader"] = dict(openlist_url="http://127.0.0.1:5244", openlist_token="secret-token")
        native = Native()
        modules = {}
        for name in ["app", "app.sdk", "app.sdk.plugin", "app.modules", "app.modules.qbittorrent",
                     "app.application", "app.application.directory", "app.runtime", "app.runtime.settings"]:
            modules[name] = ModuleType(name)
        modules["app.sdk.plugin"].ModuleManager = lambda: SimpleNamespace(get_running_modules=lambda method: [native])
        modules["app.modules.qbittorrent"].QbittorrentModule = Native
        modules["app.application.directory"].DirectoryHelper = lambda: SimpleNamespace(get_dirs=lambda: [])
        modules["app.runtime.settings"].get_runtime_setting = lambda key: "MOVIEPILOT"
        with patch.dict(sys.modules, modules), patch.object(module.OpenList, "validate_storage", return_value=1) as validation:
            _, api = p._runtime()
            self.assertEqual(api.put_timeout, 900)
            p._runtime()
            self.assertEqual(validation.call_count, 1)
            p._config["target_path"] = "/115/other-monitor"
            p._runtime()
            self.assertEqual(validation.call_count, 2)
            p._storage_checked_at -= 10**6
            p._runtime()
            self.assertEqual(validation.call_count, 3)


if __name__ == "__main__":
    unittest.main()
