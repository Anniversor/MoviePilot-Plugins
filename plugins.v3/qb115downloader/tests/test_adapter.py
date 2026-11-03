import copy
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
try:
    import adapter
except ImportError:
    adapter = None


class Native:
    def __init__(self):
        self.calls = []
        self.entries = []
        self.server = self

    def get_instance(self, name):
        return self.server if name == "physical" else None

    def normalize_path(self, path, name):
        return str(path).replace("/mp/", "/qb/")

    def normalize_return_path(self, path, name):
        return str(path).replace("/qb/", "/mp/")

    def get_files(self, tid):
        return [dict(name="seed.partial/a.mkv", priority=1, size=4, progress=1)]

    def get_torrents(self, ids=None):
        return copy.deepcopy(self.entries), False

    def add_torrent(self, **kwargs):
        self.calls.append(kwargs)
        self.entries = [dict(hash="a" * 40, name="seed.partial", tags="mp-qb115",
                             save_path="/qb/isolated", progress=0, state="pausedDL")]
        return True, ["a" * 40]

    def get_content_layout(self):
        return "Original"

    def start_torrents(self, tid):
        return True

    def _select_download_files(self, server, tid, episodes, custom_words):
        self.selected = episodes
        return True, "ok"

    def download(self, **kwargs):
        self.calls.append(kwargs)
        return "physical", "a" * 40, "Original", "ok"


class AdapterTests(unittest.TestCase):
    def test_submit_adds_paused_via_instance_so_native_duplicate_branch_cannot_add_global_tag(self):
        native = Native()
        native.download = lambda **kw: self.fail("native download duplicate branch must not run")
        calls = []
        def add(**kw):
            calls.append(kw)
            native.entries = [dict(hash="a" * 40, name="seed.partial", tags="mp-qb115",
                                   save_path="/qb/isolated", progress=0, state="pausedDL")]
            return True, ["a" * 40]
        native.add_torrent = add
        native.get_content_layout = lambda: "Original"
        native.start_torrents = lambda tid: True
        a = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "MOVIEPILOT")
        self.assertEqual(a.submit(b"torrent", "a" * 40)[1], "a" * 40)
        self.assertTrue(calls[0]["is_paused"])
        self.assertEqual(calls[0]["tag"], ["mp-qb115"])
        self.assertFalse(calls[0]["ignore_category_check"])

    def test_snapshot_requires_owned_tag_and_roundtrip_actual_qb_save_path(self):
        self.assertTrue(callable(getattr(adapter.QbAdapter, "snapshot", None)))
        native = Native()
        native.entries = [dict(hash="a" * 40, name="seed.partial", tags="mp-qb115",
                               save_path="/qb/isolated", progress=.75, state="downloading")]
        a = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "MOVIEPILOT")
        snap = a.snapshot("a" * 40)
        self.assertEqual(snap["root"], "/mp/isolated")
        self.assertEqual(snap["progress"], .75)
        for tags, path in [("mp-qb115,MOVIEPILOT", "/qb/isolated"), ("MOVIEPILOT", "/qb/isolated"),
                           ("mp-qb115", "/qb/elsewhere")]:
            native.entries[0].update(tags=tags, save_path=path)
            with self.subTest(tags=tags, path=path), self.assertRaises(ValueError):
                a.snapshot("a" * 40)

    def test_explicit_physical_qb_submit_never_uses_dispatcher_or_global_tag(self):
        self.assertIsNotNone(adapter)
        native = Native()
        a = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "MOVIEPILOT")
        result = a.submit(b"torrent bytes", "a" * 40, episodes={1}, custom_words="word")
        self.assertEqual(result[1], "a" * 40)
        self.assertEqual(native.calls[0]["tag"], ["mp-qb115"])
        self.assertEqual(native.calls[0]["download_dir"], "/qb/isolated")
        self.assertIsNone(native.calls[0]["category"])
        self.assertIsNone(native.calls[0]["cookie"])
        self.assertEqual(native.selected, {1})
        for tag in ["", "mp-qb115", "x,mp-qb115"]:
            a = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: tag)
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                a.submit(b"torrent", "a" * 40)
        native.entries = [dict(hash="a" * 40, tags="MOVIEPILOT")]
        with self.assertRaises(ValueError):
            adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "MOVIEPILOT").submit(b"torrent", "a" * 40)
        self.assertEqual(len(native.calls), 1)
