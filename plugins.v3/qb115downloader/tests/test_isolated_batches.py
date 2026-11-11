"""Same torrent root, different full hashes: local namespace and legacy regression tests."""
import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import core

A = "a" * 40
B = "b" * 40
NAME = "seed.partial"


class AdapterNamespaceTests(unittest.TestCase):
    def test_unsafe_hash_namespace_is_refused_before_qb_add(self):
        import tempfile
        from test_adapter import Native
        import adapter
        with tempfile.TemporaryDirectory() as root:
            outside = Path(root) / "outside"
            outside.mkdir()
            (Path(root) / B).symlink_to(outside, target_is_directory=True)
            for bound in [None, "", "relative", root + "/../escape"]:
                native = Native()
                qb = adapter.QbAdapter(native, "physical", root, lambda: "MOVIEPILOT",
                                       task_root=lambda tid, value=bound: value)
                with self.subTest(root=bound), self.assertRaises(ValueError):
                    qb.submit(b"torrent", B)
                self.assertEqual(native.calls, [])

    def test_hash_roots_and_legacy_binding_roundtrip_without_rewriting_old_paths(self):
        from test_adapter import Native
        import adapter
        native = Native()
        ledger = {A: dict(root="/mp/isolated")}
        qb = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "MOVIEPILOT",
                               task_root=lambda tid: ledger.get(tid, {}).get("root"))
        self.assertEqual(qb.local_root(A), "/mp/isolated")
        self.assertEqual(qb.local_root(B), "/mp/isolated/" + B)
        self.assertEqual(qb.expected_save_path(B), "/qb/isolated/" + B)
        def add(**kw):
            native.calls.append(kw)
            native.entries = [dict(hash=B, name=NAME, tags="mp-qb115", save_path=kw["download_dir"])]
            return True, [B]
        native.add_torrent = add
        qb.submit(b"torrent", B, episodes={199})
        self.assertEqual(native.calls[0]["download_dir"], "/qb/isolated/" + B)
        self.assertTrue(native.calls[0]["is_paused"])
        self.assertEqual(native.selected, {199})
        self.assertEqual(qb.snapshot(B)["root"], "/mp/isolated/" + B)
        native.entries = [dict(hash=A, name=NAME, tags="mp-qb115", save_path="/qb/isolated")]
        self.assertEqual(qb.snapshot(A)["root"], "/mp/isolated")
        for wrong in ["/qb/isolated/" + B, "/qb/isolated/" + A]:
            native.entries[0]["save_path"] = wrong
            with self.subTest(path=wrong), self.assertRaises(ValueError):
                qb.snapshot(A)
        native.entries = [dict(hash=B, name=NAME, tags="mp-qb115", save_path="/qb/isolated")]
        with self.assertRaises(ValueError):
            qb.start(B)


class PluginNamespaceTests(unittest.TestCase):
    def test_real_adapter_plugin_submits_same_name_partial_torrents_after_reload(self):
        import tempfile
        from types import ModuleType, SimpleNamespace
        from unittest.mock import patch
        from test_adapter import Native
        from test_plugin import load_plugin
        from test_core import bencode
        module = load_plugin()
        native = Native()
        entries = {}
        adds = []
        def torrents(ids=None):
            wanted = [ids] if isinstance(ids, str) else ids
            return [copy.deepcopy(v) for k, v in entries.items() if wanted is None or k in wanted], False
        def add(**kw):
            tid, name, _ = core.torrent_metadata(kw["content"])
            adds.append(kw)
            entries[tid] = dict(hash=tid, name=name, tags="mp-qb115", save_path=kw["download_dir"],
                                state="pausedDL", progress=0)
            return True, [tid]
        native.get_torrents = torrents
        native.add_torrent = add
        modules = {}
        for name in ["app.sdk.plugin", "app.modules.qbittorrent", "app.runtime.settings"]:
            modules[name] = ModuleType(name)
        modules["app.sdk.plugin"].ModuleManager = lambda: SimpleNamespace(get_running_modules=lambda method: [native])
        modules["app.modules.qbittorrent"].QbittorrentModule = Native
        modules["app.runtime.settings"].get_runtime_setting = lambda key: "MOVIEPILOT"
        with tempfile.TemporaryDirectory() as root, patch.dict(sys.modules, modules):
            p = module.Qb115Downloader()
            config = dict(local_root=root, physical_downloader="physical", low_water_gib=0)
            p.init_plugin(config)
            p._enabled = True
            p._runtime = lambda: (p._qb_adapter(), None)
            raws = [bencode({b"info": {b"name": NAME.encode(), b"piece length": 16384,
                                      b"pieces": b"x" * 20, b"files": [
                                          {b"length": 4, b"path": [("E%d.mkv" % episode).encode()]}]}})
                    for episode in (198, 199)]
            hashes = []
            for episode, raw in zip((198, 199), raws):
                tid = core.torrent_metadata(raw)[0]
                result = p.download(content=raw, downloader="qB → 115", episodes={episode})
                self.assertEqual(result[1], tid, result)
                hashes.append(tid)
                self.assertEqual(p._engine.tasks[tid]["root"], root + "/" + tid)
                self.assertEqual(p._engine.tasks[tid]["save_path"], root + "/" + tid)
                p.init_plugin(config)  # persistence/adapter reconstruction between submissions
                p._enabled = True
            self.assertEqual([a["download_dir"] for a in adds], [root + "/" + h for h in hashes])
            self.assertTrue(all(a["is_paused"] and a["tag"] == ["mp-qb115"] for a in adds))
            self.assertEqual(native.selected, {199})
            listed = {item.hash: item for item in p.list_torrents(downloader="qB → 115")}
            for tid in hashes:
                self.assertEqual(listed[tid].save_path, root + "/" + tid)
                self.assertEqual(listed[tid].content_path, root + "/" + tid + "/" + NAME)
                self.assertEqual(str(listed[tid].path), listed[tid].content_path)
            before = copy.deepcopy(p.data)
            result = p.download(content=raws[1], downloader="qB → 115", episodes={199})
            self.assertEqual(result[3], "下载任务已存在")
            self.assertEqual(len(adds), 2)
            self.assertEqual(p.data, before)
            self.assertEqual(p._engine.guard(p._qb_adapter()), [])
            native.get_instance = lambda name: native  # another instance happens to contain the same hash/path
            p._config["physical_downloader"] = "other"
            mismatch = p.download(content=raws[1], downloader="qB → 115")
            self.assertIsNone(mismatch[1], "duplicate success must prove the frozen physical binding too")
            self.assertEqual(len(adds), 2)
            p._config["physical_downloader"] = "physical"
            # A legacy ledger entry keeps its old qB save path, with no move/set-location calls.
            old = p._engine.tasks[hashes[0]]
            old.update(root=root, save_path=root)
            entries[hashes[0]]["save_path"] = root
            p._engine.flush()
            p.init_plugin(config)
            qb = p._qb_adapter()
            self.assertEqual(qb.snapshot(hashes[0])["root"], root)
            self.assertEqual(qb.snapshot(hashes[1])["root"], root + "/" + hashes[1])


class WorkflowNamespaceTests(unittest.TestCase):
    """Characterize existing safety gates with mixed legacy/new roots, real adapter and HTTP."""
    def setUp(self):
        import tempfile
        from test_adapter import Native
        from test_cloud import Server, cloud
        import adapter
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.server = Server()
        self.addCleanup(self.server.close)
        self.cloud = cloud.OpenList(self.server.url, "secret-token")
        self.engine = core.Engine({}, lambda tasks: None, free=lambda: 100000, low_water=0)
        self.native = Native()
        self.entries, self.files, self.contents = {}, {}, {A: b"episode198", B: b"episode199"}
        self.started, self.paused = [], []
        for tid in (A, B):
            root = self.root if tid == A else self.root / tid  # existing legacy A + new B
            (root / NAME).mkdir(parents=True)
            (root / NAME / "a.mkv").write_bytes(self.contents[tid])
            self.files[tid] = [dict(name=NAME + "/a.mkv", size=len(self.contents[tid]), progress=1, priority=1),
                               dict(name=NAME + "/missing.mkv", size=9999, progress=0, priority=0)]
            self.entries[tid] = dict(hash=tid, name=NAME, tags="mp-qb115", save_path=str(root),
                                     state="uploading", progress=1)
            task = self.engine.admit(tid, NAME, 10000, str(root), physical="physical",
                                     save_path=str(root), layout="Original")
            task["state"] = "downloading"
        self.native.get_files = lambda tid: copy.deepcopy(self.files[tid])
        def torrents(ids=None):
            wanted = [ids] if isinstance(ids, str) else ids
            return [copy.deepcopy(v) for k, v in self.entries.items() if wanted is None or k in wanted], False
        self.native.get_torrents = torrents
        self.native.start_torrents = lambda tid: self.started.append(tid) or True
        self.native.stop_torrents = lambda tid: self.paused.append(tid) or True
        def untag(ids, tag):
            tags = set(self.entries[ids]["tags"].split(",")) - set(tag)
            self.entries[ids]["tags"] = ",".join(sorted(tags))
            return True
        self.native.remove_torrents_tag = untag
        self.qb = adapter.QbAdapter(self.native, "physical", str(self.root), lambda: "MOVIEPILOT",
                                    task_root=lambda tid: self.engine.tasks.get(tid, {}).get("root"))
        self.engine.free = lambda: 100000

    def tick(self, now=100):
        self.engine.tick(self.qb, self.cloud, "/115/staging", "/115/watch", now=now)

    def test_legacy_and_new_sources_hash_independently_and_handoff_never_overwrites_same_cloud_name(self):
        import hashlib
        self.tick()
        first, second = self.engine.tasks[A], self.engine.tasks[B]
        self.assertEqual(first["state"], "waiting_library")
        self.assertEqual(second["state"], "failed")  # same cloud name still occupied; no merge/overwrite
        self.assertNotIn("commit_intent", second)
        self.assertEqual(self.server.objects["/115/watch/" + NAME + "/a.mkv"], self.contents[A])
        self.assertEqual(self.server.objects["/115/staging/" + B + "/" + NAME + "/a.mkv"], self.contents[B])
        for tid in (A, B):
            item = self.engine.tasks[tid]["manifest"][0]
            self.assertEqual(item["source"], NAME + "/a.mkv")
            self.assertEqual(item["sha1"], hashlib.sha1(self.contents[tid]).hexdigest())
        self.assertEqual(len(self.server.moves), 1)
        frozen = copy.deepcopy(second["manifest"])
        # Simulate CLM consuming the first batch on the in-memory HTTP fixture only.
        for key in list(self.server.objects):
            if key == "/115/watch/" + NAME or key.startswith("/115/watch/" + NAME + "/"):
                del self.server.objects[key]
        ledger = copy.deepcopy(self.engine.tasks)
        self.engine = core.Engine(ledger, lambda tasks: None, free=lambda: 100000, low_water=0)
        self.tick(now=200)
        self.assertEqual(self.engine.tasks[B]["state"], "waiting_library")
        self.assertEqual(self.engine.tasks[B]["manifest"], frozen)
        self.assertEqual(len(self.server.puts), 2, "restart reuses verified stage, never reuploads first batch")
        self.assertEqual(len(self.server.moves), 2)
        self.assertTrue(all(not move["overwrite"] for move in self.server.moves))
        self.assertEqual(self.server.objects["/115/watch/" + NAME + "/a.mkv"], self.contents[B])
        for tid in (A, B):
            task = self.engine.tasks[tid]
            self.assertEqual((Path(task["root"]) / NAME / "a.mkv").read_bytes(), self.contents[tid])
        self.assertTrue(self.engine.control(B, "start", self.qb))
        self.assertEqual(self.started, [B])
        self.assertTrue(self.engine.abandon(B, self.qb))
        self.assertTrue(self.engine._source_present(self.engine.tasks[B]))
        self.entries[B]["tags"] = "mp-qb115,MOVIEPILOT"
        self.assertEqual(self.engine.guard(self.qb), [B])
        self.assertEqual(self.entries[B]["tags"], "mp-qb115")
        self.assertEqual(self.engine.tasks[B]["state"], "untracked")
        self.assertNotIn(A, self.paused)

    def test_missing_selected_file_metadata_still_blocks_entire_isolated_batch(self):
        self.engine.tasks[A]["paused"] = True
        for field in ("priority", "progress", "size"):
            with self.subTest(field=field):
                original = self.files[B][0].pop(field)
                self.tick()
                self.assertEqual(self.server.puts, [])
                self.assertEqual(self.server.moves, [])
                self.assertEqual(self.engine.tasks[B]["manifest"], [])
                self.files[B][0][field] = original

    def test_unsuspend_uses_new_hash_root_without_resuming_or_touching_legacy_task(self):
        self.engine.tasks[A]["paused"] = True
        self.server.fail_put = 1
        self.tick()
        task = self.engine.tasks[B]
        self.assertTrue(task["manifest"])
        task["state"] = "suspended"
        self.assertTrue(self.engine.unsuspend(B, self.qb, confirm=True))
        self.assertTrue(task["paused"])
        self.assertEqual(self.started, [])
        self.assertEqual(self.engine.tasks[A]["manifest"], [])
        self.entries[B]["save_path"] = str(self.root)
        task["state"] = "suspended"
        self.assertFalse(self.engine.unsuspend(B, self.qb, confirm=True))
        self.assertFalse(self.engine.control(B, "start", self.qb))
        self.assertEqual(self.started, [])


class BindingTests(unittest.TestCase):
    def test_unsuspend_refuses_snapshot_root_different_from_frozen_manifest_root(self):
        from test_core import WorkflowTests
        flow = WorkflowTests()
        flow.setUp()
        self.addCleanup(flow.doCleanups)
        flow.server.fail_put = 2
        flow.tick()
        task = flow.engine.tasks[flow.tid]
        task["state"] = "suspended"
        snapshot = flow.qb.snapshot
        flow.qb.snapshot = lambda tid: {**snapshot(tid), "root": flow.root + "/other-hash"}
        self.assertFalse(flow.engine.unsuspend(flow.tid, flow.qb, confirm=True))
        self.assertEqual(task["state"], "suspended")
        self.assertEqual(flow.qb.started, [])

    def test_start_refuses_save_path_drift_before_resuming(self):
        from test_core import WorkflowTests
        flow = WorkflowTests()
        flow.setUp()
        self.addCleanup(flow.doCleanups)
        snapshot = flow.qb.snapshot
        flow.qb.snapshot = lambda tid: {**snapshot(tid), "save_path": flow.root + "/other-hash"}
        self.assertFalse(flow.engine.control(flow.tid, "start", flow.qb))
        self.assertEqual(flow.qb.started, [])
        self.assertEqual(flow.engine.tasks[flow.tid]["state"], "suspended")


class AdmissionTests(unittest.TestCase):
    def test_overlapping_local_or_physical_content_paths_remain_reserved(self):
        for root, name, save_path in [("/local", NAME, "/qb/new"),
                                      ("/local/" + NAME, "nested", "/qb/new"),
                                      ("/local", "other", "/qb/" + NAME + "/nested"),
                                      ("/elsewhere", NAME, "/qb")]:
            with self.subTest(root=root, name=name, save_path=save_path):
                engine = core.Engine({}, lambda tasks: None, free=lambda: 1000, low_water=0)
                engine.admit(A, NAME, 10, "/local", physical="physical", save_path="/qb")
                with self.assertRaisesRegex(ValueError, "path.*reserved"):
                    engine.admit(B, name, 10, root, physical="physical", save_path=save_path)
                self.assertEqual(len(engine.tasks), 1)

    def test_same_name_different_hashes_survive_restart_with_independent_reservations(self):
        saved = {}
        def save(tasks):
            saved.clear()
            saved.update(copy.deepcopy(tasks))
        engine = core.Engine({}, save, free=lambda: 100, low_water=10)
        first = engine.admit(A, NAME, 40, "/local/" + A, physical="physical", save_path="/qb/" + A)
        second = engine.admit(B, NAME, 30, "/local/" + B, physical="physical", save_path="/qb/" + B)
        restored = core.Engine(saved, save, free=lambda: 100, low_water=10)
        self.assertEqual(restored.tasks[A], first)
        self.assertEqual(restored.tasks[B], second)
        self.assertEqual(sum(restored.outstanding(t) for t in restored.tasks.values()), 70)
        self.assertIs(restored.admit(A, NAME, 40, "/changed"), restored.tasks[A])
        with self.assertRaisesRegex(ValueError, "insufficient"):
            restored.admit("c" * 40, NAME, 21, "/local/" + "c" * 40)
        self.assertEqual(len(restored.tasks), 2)
