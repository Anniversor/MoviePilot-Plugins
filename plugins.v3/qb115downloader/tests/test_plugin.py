import copy
import importlib.util
from pathlib import Path
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

PLUGIN = Path(__file__).resolve().parents[1]


class Base:
    def __init__(self):
        self.data = {}
        self.writes = []
        self.configs = {}

    def get_data(self, key):
        return copy.deepcopy(self.data.get(key))

    def save_data(self, key, value):
        self.data[key] = copy.deepcopy(value)

    def get_config(self, plugin_id=None):
        return self.configs.get(plugin_id, {})

    def update_config(self, value):
        self.writes.append(value)


def load_plugin():
    if not (PLUGIN / "__init__.py").exists():
        return None
    modules = {}
    for name in ["app", "app.sdk", "app.sdk.plugin", "app.schemas", "app.schemas.transfer"]:
        modules[name] = ModuleType(name)
    modules["app.sdk.plugin"]._PluginBase = Base
    modules["app.schemas.transfer"].DownloaderTorrent = lambda **kw: SimpleNamespace(**kw)
    spec = importlib.util.spec_from_file_location("qb115_test", PLUGIN / "__init__.py", submodule_search_locations=[str(PLUGIN)])
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {**modules, "qb115_test": module}):
        spec.loader.exec_module(module)
    return module


class PluginTests(unittest.TestCase):
    def test_disable_during_preflight_does_not_submit_qb_task(self):
        from test_core import bencode
        p = load_plugin().Qb115Downloader()
        p.init_plugin(dict(local_root="/unused", low_water_gib=0))
        p._engine.free = lambda: 10000
        p._enabled = True
        submitted = []
        qb = SimpleNamespace(submit=lambda *a, **kw: submitted.append(a) or ("physical", a[1], "Original", "ok"))
        def runtime():
            p.stop_service()
            return qb, None
        p._runtime = runtime
        raw = bencode({b"info": {b"name": b"a.mkv", b"length": 4, b"pieces": b"x" * 20, b"piece length": 16384}})
        result = p.download(content=raw, downloader="qB → 115")
        self.assertIsNone(result[1])
        self.assertEqual(submitted, [])
        self.assertEqual(p._engine.tasks, {})

    def test_reinit_also_waits_for_inflight_qb_submission(self):
        import threading
        p = load_plugin().Qb115Downloader()
        p.init_plugin({})
        old = p._engine
        old.lock.acquire()
        thread = threading.Thread(target=lambda: p.init_plugin({}), daemon=True)
        try:
            thread.start()
            self.assertTrue(old.stopped.wait(1))
            thread.join(.05)
            self.assertTrue(thread.is_alive(), "reload must wait for qB submission ledger flush")
        finally:
            old.lock.release()
            thread.join(2)
        self.assertIsNot(p._engine, old)

    def test_reinit_waits_for_old_worker_before_reloading_persisted_ledger(self):
        import threading
        p = load_plugin().Qb115Downloader()
        p.init_plugin({})
        old = p._engine
        old.worker.acquire()
        thread = threading.Thread(target=lambda: p.init_plugin({}), daemon=True)
        try:
            thread.start()
            self.assertTrue(old.stopped.wait(1))
            thread.join(.05)
            self.assertTrue(thread.is_alive(), "reload must wait for old writes to finish")
            self.assertIs(p._engine, old)
        finally:
            old.worker.release()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertIsNot(p._engine, old)

    def test_ui_api_and_scheduler_explicitly_expose_manual_cleanup_and_disabled_default(self):
        import json
        module = load_plugin()
        p = module.Qb115Downloader()
        p.init_plugin({})
        self.assertTrue(callable(getattr(p, "get_form", None)))
        form, defaults = p.get_form()
        self.assertFalse(defaults["enabled"])
        self.assertFalse(defaults["isolation_confirmed"])
        self.assertNotIn("openlist_token", defaults)
        self.assertIn("待人工", json.dumps(p.get_page(), ensure_ascii=False))
        apis = p.get_api()
        self.assertEqual({a["path"] for a in apis}, {"/tasks", "/control"})
        self.assertTrue(all(a["auth"] == "apikey" for a in apis))
        self.assertEqual(next(a for a in apis if a["path"] == "/control")["methods"], ["POST"])
        self.assertEqual(p.get_service(), [])
        p._enabled = True
        service = p.get_service()[0]
        self.assertEqual(service["trigger"], "interval")
        self.assertEqual(service["kwargs"]["max_instances"], 1)
        self.assertFalse(p.api_control(hash="a" * 40, action="delete")["success"])
        p.stop_service()
        self.assertTrue(p._engine.stopped.is_set())

    def test_registration_never_retypes_collision_or_changes_defaults(self):
        module = load_plugin()
        p = module.Qb115Downloader()
        self.assertTrue(callable(getattr(p, "_sync_entry", None)))
        entries = [dict(name="physical", type="qbittorrent", enabled=True, default=True, config={"private": "not-read"})]
        original = copy.deepcopy(entries)
        class Store:
            def get(self, key):
                return copy.deepcopy(entries)
            def update_atomically(self, key, mutation):  # round 3: the registry is only written atomically
                result, value = mutation(copy.deepcopy(entries))
                entries[:] = copy.deepcopy(value)
                return result
        p._entry_store = lambda: (Store(), "Downloaders")
        complete = dict(physical_downloader="physical", local_root="/data/qb115", isolation_confirmed=True)
        p.init_plugin({"enabled": True, **complete})
        self.assertEqual(entries[0], original[0])
        self.assertEqual(entries[1]["name"], "qB → 115")
        self.assertEqual(entries[1]["type"], "qb115virtual")
        self.assertFalse(entries[1]["default"])
        p._sync_entry()
        self.assertEqual(len(entries), 2)
        entries[1]["type"] = "qbittorrent"
        with self.assertRaises(ValueError):
            p._sync_entry()
        self.assertEqual(entries[1]["type"], "qbittorrent")
        p.init_plugin({"enabled": False})
        self.assertEqual(len(entries), 2)
        entries[1]["type"] = "qb115virtual"
        p._sync_entry()
        self.assertEqual(entries, original)

    def test_runtime_binds_only_running_qb_and_reuses_credentials_in_memory_with_isolation_guard(self):
        from test_adapter import Native
        module = load_plugin()
        self.assertTrue(callable(getattr(module.Qb115Downloader, "_runtime", None)))
        p = module.Qb115Downloader()
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        p.init_plugin(dict(enabled=False, local_root=temp.name, physical_downloader="physical", isolation_confirmed=True))
        p.configs["U115OfflineDownloader"] = dict(openlist_url="http://127.0.0.1:5244", openlist_token="secret-token")
        native = Native()
        dirs = []
        modules = {}
        for name in ["app", "app.sdk", "app.sdk.plugin", "app.modules", "app.modules.qbittorrent",
                     "app.application", "app.application.directory", "app.runtime", "app.runtime.settings"]:
            modules[name] = ModuleType(name)
        modules["app.sdk.plugin"].ModuleManager = lambda: SimpleNamespace(get_running_modules=lambda method: [native])
        modules["app.modules.qbittorrent"].QbittorrentModule = Native
        modules["app.application.directory"].DirectoryHelper = lambda: SimpleNamespace(get_dirs=lambda: dirs)
        modules["app.runtime.settings"].get_runtime_setting = lambda key: "MOVIEPILOT"
        with patch.dict(sys.modules, modules), patch.object(module.OpenList, "validate_storage", return_value=1) as validation:
            qb, api = p._runtime()
            self.assertTrue(validation.called)
            self.assertIs(qb.module, native)
            self.assertEqual(qb.name, "physical")
            self.assertEqual(api._token, "secret-token")
            self.assertNotIn("secret-token", repr(p.data) + repr(p._config) + repr(p.writes))
            dirs.append(SimpleNamespace(storage="local", download_path=str(Path(temp.name).parent), monitor_type="monitor"))
            with self.assertRaises(ValueError):
                p._runtime()
            dirs.clear()
            dirs.append(SimpleNamespace(storage="local", download_path="/", monitor_type="monitor"))
            with self.assertRaises(ValueError):
                p._runtime()
            dirs.clear()
            p._config["isolation_confirmed"] = False
            with self.assertRaises(ValueError):
                p._runtime()

    def test_plugin_download_persist_poll_and_real_loopback_handoff(self):
        from test_cloud import Server, cloud
        from test_core import bencode, FakeQb
        server = Server()
        self.addCleanup(server.close)
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        (root / "seed.partial").mkdir()
        (root / "seed.partial/a.mkv").write_bytes(b"data")
        qb = FakeQb(str(root), [dict(name="seed.partial/a.mkv", size=4, progress=1, priority=1)])
        submissions = []
        def submit(content, tid, **kw):
            submissions.append((content, tid, kw))
            return "physical", tid, "Original", "ok"
        qb.submit = submit
        module = load_plugin()
        plugin = module.Qb115Downloader()
        plugin.init_plugin(dict(enabled=False, local_root=str(root), staging_path="/115/staging", target_path="/115/watch", low_water_gib=0))
        plugin._enabled = True
        plugin._runtime = lambda: (qb, cloud.OpenList(server.url, "secret-token"))
        plugin._engine.free = lambda: 1000000
        raw = bencode({b"announce": b"https://tracker/PRIVATE-PASSKEY", b"info":
                       {b"name": b"seed.partial", b"piece length": 16384, b"pieces": b"x" * 20,
                        b"files": [{b"length": 4, b"path": [b"a.mkv"]}]}})
        result = plugin.download(content=raw, downloader="qB → 115", episodes={1})
        self.assertEqual(result[0], "qB → 115")
        self.assertEqual(len(submissions), 1)
        self.assertNotIn("PRIVATE-PASSKEY", repr(plugin.data))
        self.assertTrue(callable(getattr(plugin, "poll", None)))
        plugin.poll()
        self.assertEqual(len(server.puts), 1)
        self.assertEqual(len(server.moves), 1)
        tasks = plugin.tasks_status()
        self.assertEqual(tasks["tasks"][0]["state"], "waiting_library")
        self.assertEqual(tasks["tasks"][0]["qb_progress"], 100)
        self.assertFalse(tasks["tasks"][0]["library_confirmed"])
        listed = plugin.list_torrents(downloader="qB → 115")
        self.assertEqual(listed[0].progress, 100)
        self.assertEqual(plugin.list_torrents(status="transfer", hashs=result[1], downloader="qB → 115"), [])
        plugin.download(content=raw, downloader="qB → 115")
        self.assertEqual(len(submissions), 1)

    def test_disabled_default_and_exact_routing_do_not_touch_other_downloaders(self):
        module = load_plugin()
        self.assertIsNotNone(module)
        plugin = module.Qb115Downloader()
        plugin.init_plugin({})
        self.assertFalse(plugin.get_state())
        self.assertEqual(plugin.writes, [])
        self.assertIsNone(plugin.download(content=b"anything", downloader="physical"))
        self.assertIsNone(plugin.download(content=b"anything", downloader=None))
        self.assertIsNone(plugin.list_torrents(downloader=None))
        self.assertIsNone(plugin.remove_torrents("a" * 40, downloader="physical"))
        plugin._enabled = True
        self.assertEqual(plugin.list_torrents(status="transfer", downloader="qB → 115"), [])
        self.assertIsNone(plugin.download(content=b"anything", downloader="qB -> 115"))
        self.assertFalse(plugin.remove_torrents("a" * 40, downloader="qB → 115"))
        self.assertTrue({"download", "list_torrents", "stop_torrents", "start_torrents", "remove_torrents"}.issubset(plugin.get_module()))
