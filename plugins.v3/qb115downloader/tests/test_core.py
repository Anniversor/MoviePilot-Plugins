"""Stdlib-only tests; run from repository root, no MP/container mutations."""
import copy
import hashlib
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest

PLUGIN = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN))
import core


def bencode(value):
    if isinstance(value, bytes):
        return str(len(value)).encode() + b":" + value
    if isinstance(value, int):
        return b"i" + str(value).encode() + b"e"
    if isinstance(value, list):
        return b"l" + b"".join(bencode(x) for x in value) + b"e"
    return b"d" + b"".join(bencode(k) + bencode(value[k]) for k in sorted(value)) + b"e"


class CoreTests(unittest.TestCase):
    def test_selected_files_ignore_priority_zero_even_when_incomplete(self):
        self.assertTrue(callable(getattr(core, "selected_files", None)))
        files = [dict(name="seed.partial/a.mkv", size=4, progress=1, priority=1),
                 dict(name="seed.partial/b.mkv", size=999, progress=0, priority=0)]
        self.assertEqual(core.selected_files(files), [files[0]])
        files[0]["progress"] = .99
        self.assertIsNone(core.selected_files(files))

    def test_open_source_refuses_escape_symlink_and_non_media(self):
        self.assertTrue(callable(getattr(core, "open_source", None)))
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            (base / "ok.mkv").write_bytes(b"data")
            (base / "link.mkv").symlink_to(base / "ok.mkv")
            for name in ["../ok.mkv", "/etc/passwd", "link.mkv", "meta.torrent", "a/../ok.mkv", "a\\\\b.mkv"]:
                with self.subTest(name=name), self.assertRaises((ValueError, OSError)):
                    with core.open_source(root, name):
                        pass
            with core.open_source(root, "ok.mkv") as source:
                self.assertEqual(source.read(), b"data")

    def test_negative_low_water_is_not_a_way_to_bypass_admission(self):
        with self.assertRaises(ValueError):
            core.Engine({}, lambda tasks: None, free=lambda: 0, low_water=-1)

    def test_admission_persists_reservation_and_blocks_unknown_or_overcommit(self):
        self.assertTrue(callable(getattr(core, "Engine", None)))
        persisted = {}
        def save(value):
            persisted.clear()
            persisted.update(copy.deepcopy(value))
        e = core.Engine({}, save, free=lambda: 100, low_water=10)
        e.admit("a" * 40, "seed.partial", 60, "/local")
        self.assertEqual(persisted["a" * 40]["reserved"], 60)
        restored = core.Engine(persisted, save, free=lambda: 100, low_water=10)
        for size in [None, 0, 31]:
            with self.subTest(size=size), self.assertRaises(ValueError):
                restored.admit("b" * 40, "other", size, "/local")
        self.assertEqual(len(restored.tasks), 1)



class FakeQb:
    name = "physical"

    def __init__(self, root, files):
        self.root, self.files = root, files
        self.paused = []
        self.started = []

    def local_root(self, tid):
        return self.root

    def expected_save_path(self, tid=None):
        return self.root

    def preflight(self, tid):
        return None

    def check_isolation(self, hashes):
        return []

    def isolation_report(self, hashes):
        return {}

    def untag_global(self, tid):
        return "none"

    def brief(self, hashes):
        return {h: dict(progress=1, state="uploading", isolated=True) for h in hashes}

    def snapshot(self, tid):
        return dict(name="seed.partial", root=self.root, save_path=self.root, physical=self.name,
                    layout="Original", files=self.files, tags="mp-qb115", state="uploading", progress=1)

    def pause(self, tid):
        self.paused.append(tid)
        return True

    def start(self, tid):
        self.started.append(tid)
        return True


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        from test_cloud import Server, cloud
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
        self.files.append(dict(name="seed.partial/unselected.mkv", size=10000, progress=0, priority=0))
        self.qb = FakeQb(self.root, self.files)
        self.api = cloud.OpenList(self.server.url, "secret-token")
        self.persisted = {}
        self.engine = core.Engine({}, self.save, free=lambda: 10**9, low_water=0)
        self.tid = "a" * 40
        self.engine.admit(self.tid, "seed.partial", 30000, self.root, physical="physical",
                          save_path=self.root, layout="Original")
        self.engine.tasks[self.tid]["state"] = "downloading"  # qB submit/selection confirmed
        self.engine.flush()

    def save(self, tasks):
        self.persisted = copy.deepcopy(tasks)

    def tick(self):
        self.engine.tick(self.qb, self.api, "/115/staging", "/115/watch", now=100)

    def test_retry_does_not_silently_change_frozen_cloud_roots(self):
        self.server.fail_put = 2
        self.tick()
        self.server.objects["/115/staging2"] = None
        self.engine.tick(self.qb, self.api, "/115/staging2", "/115/watch", now=161)
        self.assertEqual(self.persisted[self.tid]["state"], "suspended")
        self.assertEqual(len(self.server.puts), 2)
        self.assertEqual(len(self.server.moves), 0)

    def test_start_after_handoff_only_resumes_seeding_never_retries_upload(self):
        self.tick()
        self.assertTrue(self.engine.control(self.tid, "pause", self.qb))
        self.assertTrue(self.engine.control(self.tid, "start", self.qb))
        self.assertFalse(self.engine.control(self.tid, "retry", self.qb))
        self.tick()
        self.assertEqual(len(self.server.puts), 2)
        self.assertEqual(len(self.server.moves), 1)
        self.assertEqual(self.persisted[self.tid]["state"], "waiting_library")

    def test_worker_waits_for_submission_lock_before_observing_new_tasks(self):
        import threading
        entered = threading.Event()
        snapshot = self.qb.snapshot
        def observed(tid):
            entered.set()
            return snapshot(tid)
        self.qb.snapshot = observed
        thread = threading.Thread(target=self.tick, daemon=True)
        self.engine.lock.acquire()
        try:
            thread.start()
            self.assertFalse(entered.wait(.05), "worker must not observe admission before submit confirmation")
        finally:
            self.engine.lock.release()
            thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertTrue(entered.is_set())

    def test_retry_freezes_original_manifest_if_selected_files_change(self):
        self.server.fail_put = 2
        self.tick()
        frozen = copy.deepcopy(self.persisted[self.tid]["manifest"])
        self.files[1]["priority"] = 0
        self.engine.tick(self.qb, self.api, "/115/staging", "/115/watch", now=161)
        self.assertEqual(self.persisted[self.tid]["state"], "suspended")
        self.assertEqual(self.persisted[self.tid]["manifest"], frozen)
        self.assertEqual(len(self.server.moves), 0)

    def test_source_changed_after_first_upload_is_suspended_before_handoff(self):
        changed = Path(self.root) / "seed.partial/a.mkv"
        self.server.after_put = lambda: changed.write_bytes(b"tampered" * 1500)
        self.tick()
        self.assertEqual(len(self.server.moves), 0)
        self.assertEqual(self.persisted[self.tid]["state"], "suspended")

    def test_unlisted_remote_stage_file_cannot_be_committed_with_media(self):
        def inject():
            self.server.objects["/115/staging/" + self.tid + "/seed.partial/private.torrent"] = b"PRIVATE-PASSKEY"
        self.server.after_put = inject
        self.tick()
        self.assertEqual(len(self.server.moves), 0)
        self.assertEqual(self.persisted[self.tid]["state"], "failed")

    def test_http_409_put_never_overwrites_or_commits(self):
        self.server.duplicate = True
        self.tick()
        self.assertEqual(len(self.server.moves), 0)
        self.assertEqual(self.persisted[self.tid]["state"], "failed")
        self.assertNotIn("/115/watch/seed.partial", self.server.objects)

    def test_http_200_json_error_never_commits_or_logs_response_secrets(self):
        import contextlib
        import io
        output = io.StringIO()
        self.server.fail_json = True
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            self.tick()
        self.assertNotIn("secret-token", output.getvalue())
        self.assertNotIn("PRIVATE-PASSKEY", output.getvalue())
        self.assertEqual(len(self.server.moves), 0)
        self.assertEqual(self.persisted[self.tid]["state"], "failed")
        self.assertNotIn("secret-token", repr(self.persisted))
        self.assertNotIn("PRIVATE-PASSKEY", repr(self.persisted))

    def test_existing_wrong_stage_file_is_not_overwritten(self):
        stage = "/115/staging/" + self.tid
        self.server.objects[stage] = None
        self.server.objects[stage + "/seed.partial"] = None
        self.server.objects[stage + "/seed.partial/a.mkv"] = b"wrong"
        self.tick()
        self.assertEqual(len(self.server.puts), 0)
        self.assertEqual(len(self.server.moves), 0)
        self.assertEqual(self.persisted[self.tid]["state"], "failed")

    def test_engine_source_traversal_and_symlink_reject_before_any_upload(self):
        for name in ["../a.mkv", "seed.partial/link.mkv"]:
            with self.subTest(name=name):
                self.files[:] = [dict(name=name, size=4, progress=1, priority=1)]
                link = Path(self.root) / "seed.partial/link.mkv"
                if not link.is_symlink():
                    link.symlink_to(Path(self.root) / "seed.partial/a.srt")
                self.engine.tasks[self.tid]["next_retry"] = 0
                self.tick()
                self.assertEqual(len(self.server.puts), 0)
                self.assertEqual(len(self.server.moves), 0)

    def test_qb_checking_or_moving_is_not_ready_even_with_complete_files(self):
        snapshot = self.qb.snapshot
        for state in ["checkingUP", "moving", "error", "missingFiles", "unknown"]:
            self.qb.snapshot = lambda tid, s=state: {**snapshot(tid), "state": s}
            self.tick()
            self.assertEqual(len(self.server.puts), 0)
            self.assertEqual(self.persisted[self.tid]["state"], "downloading")

    def test_unconfirmed_qb_submission_is_not_auto_adopted_after_restart(self):
        self.engine.tasks[self.tid]["state"] = "admitted"
        self.tick()
        self.assertEqual(len(self.server.puts), 0)
        self.assertEqual(self.persisted[self.tid]["state"], "suspended")

    def test_selection_changed_during_upload_suspends_without_commit_or_new_manifest(self):
        self.server.after_put = lambda: self.files[1].update(priority=0)
        self.tick()
        self.assertEqual(len(self.server.moves), 0)
        self.assertEqual(self.persisted[self.tid]["state"], "suspended")
        self.assertEqual(len(self.persisted[self.tid]["manifest"]), 2)
        self.tick()
        self.assertEqual(len(self.server.moves), 0)

    def test_stop_mid_upload_prevents_commit_and_shared_worker_excludes_reloaded_engine(self):
        self.server.after_put = self.engine.stopped.set
        self.tick()
        self.assertEqual(len(self.server.puts), 1)
        self.assertEqual(len(self.server.moves), 0)
        restored = core.Engine(self.persisted, self.save, free=lambda: 10**9, low_water=0)
        self.engine.worker.acquire()
        try:
            restored.tick(self.qb, self.api, "/115/staging", "/115/watch", now=100)
        finally:
            self.engine.worker.release()
        self.assertEqual(len(self.server.puts), 1)
        self.server.after_put = None
        restored.tick(self.qb, self.api, "/115/staging", "/115/watch", now=100)
        self.assertEqual(len(self.server.puts), 2)
        self.assertEqual(len(self.server.moves), 1)

    def test_staging_must_be_outside_monitor_and_on_same_115_storage(self):
        for staging, monitor in [("/115/watch/staging", "/115/watch"), ("/115/staging", "/115/staging/watch"),
                                 ("/other/staging", "/115/watch"), ("/115/staging/../watch", "/115/watch")]:
            with self.subTest(staging=staging):
                self.engine.tasks[self.tid]["next_retry"] = 0
                self.engine.tick(self.qb, self.api, staging, monitor, now=100)
                self.assertEqual(self.server.puts, [])
                self.assertEqual(self.server.moves, [])
                self.assertEqual(self.persisted[self.tid]["state"], "failed")

    def test_delete_defaults_refuse_and_tracking_removal_preserves_sources_and_reservation(self):
        self.assertTrue(callable(getattr(self.engine, "remove", None)))
        self.assertFalse(self.engine.remove(self.tid, self.qb))
        self.assertEqual(self.qb.paused, [])
        self.assertTrue(self.engine.remove(self.tid, self.qb, delete_file=False))
        self.tick()
        self.assertEqual(self.server.puts, [])
        self.assertTrue((Path(self.root) / "seed.partial/a.mkv").exists())
        self.assertEqual(self.persisted[self.tid]["reserved"], 30000)
        self.assertEqual(self.persisted[self.tid]["state"], "untracked")
        self.assertFalse(self.engine.control(self.tid, "retry", self.qb))

    def test_pause_during_put_stops_before_next_file_or_commit_then_resumes(self):
        self.assertTrue(callable(getattr(self.engine, "control", None)))
        self.server.after_put = lambda: self.engine.control(self.tid, "pause", self.qb)
        self.tick()
        self.assertEqual(len(self.server.puts), 1)
        self.assertEqual(len(self.server.moves), 0)
        self.assertTrue(self.persisted[self.tid]["paused"])
        self.assertEqual(self.qb.paused, [self.tid])
        self.server.after_put = None
        self.assertTrue(self.engine.control(self.tid, "start", self.qb))
        self.tick()
        self.assertEqual(len(self.server.puts), 2)
        self.assertEqual(len(self.server.moves), 1)

    def test_restart_after_commit_crash_never_reuploads_if_clm_consumed_batch(self):
        move = self.api.move
        def crash(*args):
            move(*args)
            raise SystemExit("simulated process death after external move")
        self.api.move = crash
        with self.assertRaises(SystemExit):
            self.tick()
        self.assertEqual(self.persisted[self.tid]["state"], "commit_pending")
        for key in list(self.server.objects):
            if key.startswith("/115/watch/seed.partial"):
                del self.server.objects[key]
        self.engine = core.Engine(self.persisted, self.save, free=lambda: 10**9, low_water=0)
        self.api.move = move
        self.tick()
        self.assertEqual(len(self.server.puts), 2)
        self.assertEqual(len(self.server.moves), 1)
        self.assertEqual(self.persisted[self.tid]["state"], "suspended")
        self.tick()
        self.assertEqual(len(self.server.puts), 2)

    def test_multifile_failure_does_not_commit_and_retries_only_unverified_file(self):
        self.server.fail_put = 2
        try:
            self.tick()
        except Exception:
            self.fail("worker must persist failures without leaking transport errors")
        self.assertEqual(self.server.moves, [])
        self.assertEqual(self.persisted[self.tid]["state"], "failed")
        # backoff counts from the failure moment (tick start 100 + the real few ms the tick took), not from tick start
        self.assertAlmostEqual(self.persisted[self.tid]["next_retry"], 160, delta=5)
        self.assertNotIn("secret-token", repr(self.persisted))
        self.assertNotIn("PRIVATE-PASSKEY", repr(self.persisted))
        self.tick()
        self.assertEqual(len(self.server.puts), 2)
        self.engine.tick(self.qb, self.api, "/115/staging", "/115/watch", now=161)
        self.assertEqual(len(self.server.puts), 3)
        self.assertEqual(len(self.server.moves), 1)
        self.assertEqual(self.persisted[self.tid]["state"], "waiting_library")

    def test_complete_selected_files_are_streamed_verified_then_single_folder_commit(self):
        self.assertTrue(callable(getattr(self.engine, "tick", None)))
        self.tick()
        self.assertEqual(len(self.server.puts), 2)
        self.assertEqual(len(self.server.moves), 1)
        self.assertEqual(self.server.moves[0]["names"], ["seed.partial"])
        self.assertFalse(self.server.moves[0]["overwrite"])
        task = self.persisted[self.tid]
        self.assertEqual(task["state"], "waiting_library")
        self.assertEqual(task["selected_size"], sum(f["size"] for f in self.files[:2]))
        self.assertEqual(task["manifest"][0]["remote"], "a.mkv")
        self.assertTrue((Path(self.root) / "seed.partial/a.mkv").exists())
        self.assertIn("/115/watch/seed.partial/a.mkv", self.server.objects)

    def test_torrent_metadata_has_known_total_and_never_persists_tracker_or_passkey(self):
        self.assertTrue(callable(getattr(core, "torrent_metadata", None)))
        info = {b"name": b"seed.partial", b"piece length": 16384, b"pieces": b"x" * 20,
                b"files": [{b"length": 4, b"path": [b"a.mkv"]}, {b"length": 8, b"path": [b"b.mkv"]}]}
        raw = bencode({b"announce": b"https://tracker/PRIVATE-PASSKEY", b"info": info})
        result = core.torrent_metadata(raw)
        self.assertEqual(result, (hashlib.sha1(bencode(info)).hexdigest(), "seed.partial", 12))
        self.assertNotIn("PRIVATE-PASSKEY", repr(result))
        for content in [b"magnet:?xt=urn:btih:abc", b"not torrent", bencode({b"info": {b"name": b"x"}})]:
            with self.subTest(content=content), self.assertRaises(ValueError):
                core.torrent_metadata(content)
        info[b"files"][0][b"path"] = [b"..", b"a.mkv"]
        with self.assertRaises(ValueError):
            core.torrent_metadata(bencode({b"info": info}))


if __name__ == "__main__":
    unittest.main()
