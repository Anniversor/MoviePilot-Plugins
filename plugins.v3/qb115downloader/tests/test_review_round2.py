"""Regression tests for the second independent review (items 1-10). Stdlib only, loopback only.

Every test here was written and run RED against the pre-fix code first (see DEVLOG.md, round 2).
"""
import copy
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

PLUGIN = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN))
import adapter
import core
from test_adapter import Native
from test_cloud import Server, cloud
from test_core import bencode
from test_plugin import load_plugin
from test_review_fixes import Qb, Workflow, TID

STAGING, MONITOR = "/115/staging", "/115/watch"


def lock_free(engine):
    """True if another thread can take the engine lock within a second."""
    got = []

    def other():
        ok = engine.lock.acquire(timeout=1)
        got.append(ok)
        if ok:
            engine.lock.release()
    thread = threading.Thread(target=other)
    thread.start()
    thread.join(3)
    return bool(got and got[0])


class AdvanceableClock:
    """Patches time.time with a clock only the test advances (a hung PUT 'takes' hours instantly)."""

    def setup_clock(self, start=1000.0):
        self.clock = [start]
        patcher = patch.object(core.time, "time", lambda: self.clock[0])
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_tick(self, qb=None, api=None):
        self.engine.tick(qb or self.qb, api or self.api, STAGING, MONITOR)

    def hang_put(self, hours=3):
        puts = []

        def put(path, stream, size, sha1):
            puts.append(path)
            self.clock[0] += hours * 3600
            raise cloud.CloudTransientError("OpenList request timed out")
        self.api.put = put
        return puts


# ---------------------------------------------------------------- 1 + 2: backoff from failure time
class R1_BackoffFromFailureTime(AdvanceableClock, Workflow):
    def setUp(self):
        super().setUp()
        self.setup_clock()

    def test_put_hung_for_hours_then_timeout_quiet_period_starts_at_failure_time(self):
        puts = self.hang_put()
        self.run_tick()
        failed_at = self.clock[0]
        self.assertEqual(failed_at, 1000 + 3 * 3600)
        self.assertEqual(len(puts), 1)
        self.assertGreaterEqual(self.task["next_retry"], failed_at + core.PUT_QUIET_SECONDS)
        self.clock[0] += 30  # the next 30 s poll must not retransmit the same path
        self.run_tick()
        self.assertEqual(len(puts), 1, "no concurrent re-PUT of the path that may still be uploading")

    def test_ordinary_exponential_backoff_also_counts_from_failure_not_tick_start(self):
        self.server.put_delay = 0

        def broken_stat(path):
            self.clock[0] += 7200
            raise cloud.CloudTransientError("OpenList transport/response failure")
        self.api.stat = broken_stat
        self.run_tick()
        self.assertGreaterEqual(self.task["next_retry"], self.clock[0] + 60)

    def test_recover_commit_backoff_counts_from_failure_time(self):
        def boom(*a):
            raise SystemExit("died after commit_intent")
        self.api.move = boom
        with self.assertRaises(SystemExit):
            self.run_tick()
        self.assertTrue(self.task["commit_intent"])
        self.engine = core.Engine(self.persisted, self.save, free=lambda: 10**9, low_water=0)
        self.api = cloud.OpenList(self.server.url, "secret-token")

        def slow_probe(path):
            self.clock[0] += 3 * 3600
            raise cloud.CloudTransientError("OpenList transport/response failure")
        self.api.stat = slow_probe
        self.run_tick()
        self.assertGreaterEqual(self.task["next_retry"], self.clock[0] + 60)
        self.assertNotEqual(self.task["state"], "suspended")


class R2_QuietPeriodSurvivesControl(AdvanceableClock, Workflow):
    def setUp(self):
        super().setUp()
        self.setup_clock()
        self.real_put = self.api.put

    def test_start_and_retry_do_not_skip_the_put_uncertain_quiet_period(self):
        puts = self.hang_put()
        self.run_tick()
        failed_at = self.clock[0]
        self.assertTrue(self.engine.control(self.tid, "start", self.qb))
        self.assertTrue(self.engine.control(self.tid, "retry", self.qb))
        self.clock[0] += 30
        self.run_tick()
        self.assertEqual(len(puts), 1)
        self.assertEqual(self.server.puts, [])
        self.api.put = self.real_put
        self.clock[0] = failed_at + core.PUT_QUIET_SECONDS + 1  # quiet period over: stat first, then upload
        self.run_tick()
        self.assertEqual(len(self.server.puts), 2)
        self.assertEqual(self.task["state"], "waiting_library")

    def test_retry_still_clears_an_ordinary_network_backoff(self):
        self.server.fail_put = 1  # HTTP 503: the request finished, nothing keeps running
        self.run_tick()
        self.assertEqual(self.task["state"], "failed")
        self.assertTrue(self.engine.control(self.tid, "retry", self.qb))
        self.run_tick()
        self.assertEqual(len(self.server.puts), 3, "ordinary backoff is cleared by retry (503 attempt, then 2 files)")


# ---------------------------------------------------------------- 3: save_path comparison
class R3_SavePathTrailingSlash(Workflow):
    def test_qb_reporting_trailing_slash_does_not_suspend(self):
        qb = Qb(self.root, self.files)
        qb.save_path = self.root + "/"
        self.tick(qb=qb)
        self.assertEqual(self.task["state"], "waiting_library")

    def test_ledger_with_trailing_slash_matches_qb_without(self):
        self.engine.tasks[self.tid]["save_path"] = self.root + "/"
        self.tick()
        self.assertEqual(self.task["state"], "waiting_library")

    def test_admit_stores_the_expected_path_without_trailing_slash(self):
        self.engine.admit("b" * 40, "other", 10, self.root, physical="physical",
                          save_path=self.root + "/", layout="Original")
        self.assertEqual(self.engine.tasks["b" * 40]["save_path"], self.root)

    def test_adapter_snapshot_accepts_trailing_slash_from_qb(self):
        native = Native()
        native.entries = [dict(hash=TID, name="seed.partial", tags="mp-qb115", save_path="/qb/isolated/",
                               progress=1, state="uploading")]
        snap = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "MOVIEPILOT").snapshot(TID)
        self.assertEqual(snap["root"], "/mp/isolated")


# ---------------------------------------------------------------- 4: no network IO under the lock
class R4_CommitPhaseHoldsNoLock(Workflow):
    def wrap(self, name, label, seen, match=None, extra=None):
        real = getattr(self.api, name)

        def wrapper(*args, **kw):
            if match is None or match(*args):
                seen.setdefault(label, []).append(lock_free(self.engine))
                if extra:
                    extra()
            return real(*args, **kw)
        setattr(self.api, name, wrapper)

    def test_slow_verify_stat_and_move_leave_the_engine_lock_available(self):
        seen = {}
        self.wrap("verify_batch", "verify_batch", seen)
        self.wrap("stat", "stat_target", seen, match=lambda p: p == MONITOR + "/seed.partial")
        self.wrap("move", "move", seen)
        self.tick()
        self.assertEqual(sorted(seen), ["move", "stat_target", "verify_batch"])
        for label, results in seen.items():
            self.assertTrue(all(results), label + " must not run under engine.lock")
        self.assertEqual(self.task["state"], "waiting_library")

    def test_pause_during_a_slow_move_returns_promptly_and_move_is_not_cancelled(self):
        results = []

        def pause_in_other_thread():
            thread = threading.Thread(target=lambda: results.append(self.engine.control(self.tid, "pause", self.qb)))
            thread.start()
            thread.join(3)
            results.append(thread.is_alive())
        self.wrap("move", "move", {}, extra=pause_in_other_thread)
        self.tick()
        self.assertEqual(results, [True, False], "pause must complete while the move is still in flight")
        self.assertEqual(len(self.server.moves), 1)
        self.assertTrue(self.task["paused"])
        self.assertEqual(self.task["state"], "waiting_library")

    def test_pause_after_verification_but_before_commit_intent_prevents_the_move(self):
        pauses = []
        self.wrap("stat", "stat_target", {}, match=lambda p: p == MONITOR + "/seed.partial",
                  extra=lambda: pauses.append(self.engine.control(self.tid, "pause", self.qb)))
        self.tick()
        self.assertEqual(pauses, [True])
        self.assertEqual(self.server.moves, [])
        self.assertFalse(self.task.get("commit_intent"))
        self.assertTrue(self.task["paused"])

    def test_tasks_status_style_reads_do_not_block_during_commit(self):
        blocked = []

        def probe():
            holder = []
            thread = threading.Thread(target=lambda: holder.append(self.engine.flush()))
            thread.start()
            thread.join(2)
            blocked.append(thread.is_alive())
        self.wrap("move", "move", {}, extra=probe)
        self.tick()
        self.assertEqual(blocked, [False])


# ---------------------------------------------------------------- 5: 115 call budget
class R5_CloudCallBudget(Workflow):
    def setUp(self):
        super().setUp()
        self.files[:] = []
        shown = Path(self.root) / "seed.partial"
        for name in list(shown.iterdir()):
            name.unlink()
        for i in range(24):
            data = (b"episode-%02d-" % i) * 40
            (shown / ("e%02d.mkv" % i)).write_bytes(data)
            self.files.append(dict(name="seed.partial/e%02d.mkv" % i, size=len(data), progress=1, priority=1))

    def test_stat_of_an_existing_object_is_a_single_fs_get_without_listing(self):
        self.server.objects["/115/staging/x.mkv"] = b"x"
        self.assertEqual(self.api.stat("/115/staging/x.mkv")["name"], "x.mkv")
        self.assertEqual(self.server.gets, ["/115/staging/x.mkv"])
        self.assertEqual(self.server.lists, [])

    def test_stat_of_a_missing_object_with_recognised_not_found_needs_no_listing(self):
        self.assertIsNone(self.api.stat("/115/watch/missing"))
        self.assertEqual(self.server.lists, [])

    def test_stat_of_a_missing_object_with_unrecognised_error_confirms_with_one_refresh_listing(self):
        self.server.not_found_message = "some localized failure"
        self.assertIsNone(self.api.stat("/115/watch/missing"))
        self.assertEqual(self.server.lists, [("/115/watch", True, 1)])

    def test_only_the_first_listing_page_forces_a_refresh(self):
        self.server.page_size = 2
        for i in range(5):
            self.server.objects["/115/staging/f%s.mkv" % i] = b"x"
        manifest = [dict(remote="f%s.mkv" % i) for i in range(5)]
        self.api.verify_batch("/115/staging", manifest)
        self.assertEqual([l[1] for l in self.server.lists], [True, False, False])
        self.assertEqual([l[2] for l in self.server.lists], [1, 2, 3])

    def test_a_24_episode_batch_stays_within_a_small_call_budget_and_never_lists_the_monitor_dir(self):
        self.tick()
        self.assertEqual(self.task["state"], "waiting_library")
        self.assertEqual(len(self.server.puts), 24)
        refreshes = [l for l in self.server.lists if l[1]]
        self.assertLessEqual(len(refreshes), 4, refreshes)  # measured: 3 (stage dir, stage verify_batch, target batch dir)
        self.assertEqual([l for l in self.server.lists if l[0] == MONITOR], [], "monitor dir must never be scanned")
        self.assertLessEqual(len(self.server.gets), 24 + 12, len(self.server.gets))  # measured: 30
        self.assertEqual(len(self.server.raw_headers), 24, "one readability probe per file, not per retry/settle")

    def test_retry_after_a_late_failure_does_not_reverify_files_already_verified(self):
        self.server.fail_put = 24
        self.tick(now=100)
        self.assertEqual(self.task["state"], "failed")
        raw_before = len(self.server.raw_headers)
        self.assertEqual(raw_before, 23)
        self.tick(now=1000)
        self.assertEqual(len(self.server.puts), 25)
        self.assertEqual(len(self.server.raw_headers) - raw_before, 1, "only the previously failed file is probed")
        self.assertEqual(self.task["state"], "waiting_library")

    def test_settling_after_the_move_lists_the_batch_dir_instead_of_probing_every_file(self):
        self.tick()
        batch_lists = [l for l in self.server.lists if l[0] == MONITOR + "/seed.partial"]
        self.assertGreaterEqual(len(batch_lists), 1)
        self.assertLessEqual(len(batch_lists), 2)


# ---------------------------------------------------------------- 6: guard / status load
class R6_AdapterBatching(unittest.TestCase):
    def test_isolation_report_asks_qb_in_batches_of_at_most_50(self):
        native = Native()
        calls = []

        def get_torrents(ids=None):
            calls.append(list(ids))
            return [dict(hash=h, tags="mp-qb115") for h in ids], False
        native.get_torrents = get_torrents
        a = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "MOVIEPILOT")
        hashes = ["%040x" % i for i in range(120)]
        self.assertEqual(a.isolation_report(hashes), {})
        self.assertEqual([len(c) for c in calls], [50, 50, 20])

    def test_brief_returns_progress_without_listing_files_and_in_batches(self):
        native = Native()
        calls = []
        native.get_files = lambda tid: self.fail("status must not call get_files per task")
        native.get_torrents = lambda ids=None: (calls.append(list(ids)) or
                                                [dict(hash=h, tags="mp-qb115", progress=.5, state="downloading") for h in ids], False)
        a = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "MOVIEPILOT")
        brief = a.brief(["%040x" % i for i in range(60)])
        self.assertEqual([len(c) for c in calls], [50, 10])
        self.assertEqual(brief["%040x" % 3], dict(progress=.5, state="downloading", isolated=True))

    def test_isolation_report_codes(self):
        native = Native()
        native.entries = [dict(hash=TID, tags="mp-qb115"), dict(hash="b" * 40, tags="mp-qb115,MOVIEPILOT"),
                          dict(hash="c" * 40, tags="MOVIEPILOT")]
        a = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "MOVIEPILOT")
        report = a.isolation_report([TID, "b" * 40, "c" * 40, "d" * 40])
        self.assertEqual(report, {"b" * 40: ["global_tag"], "c" * 40: ["global_tag", "own_tag_missing"],
                                  "d" * 40: ["absent"]})
        broken = adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: "")
        self.assertEqual(set(broken.isolation_report([TID])), {TID})
        native.get_torrents = lambda ids=None: ([], True)
        with self.assertRaises(ValueError):
            a.isolation_report([TID])


class PluginBase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.p = load_plugin().Qb115Downloader()
        self.p.init_plugin(dict(enabled=False, local_root=self.root, low_water_gib=0))
        self.p._enabled = True
        self.p._engine.free = lambda: 10**9
        self.qb = Qb(self.root, [])
        self.p._qb_adapter = lambda: self.qb

        def runtime():
            raise AssertionError("must not need OpenList/_runtime")
        self.p._runtime = runtime

    def admit(self, tid, state="waiting_library"):
        self.p._engine.admit(tid, "seed" + tid[:2], 10, self.root, physical="physical", save_path=self.root,
                             layout="Original")
        self.p._engine.tasks[tid]["state"] = state


class R6_StatusAndGuardInPlugin(PluginBase):
    def test_tasks_status_and_list_use_one_batched_brief_and_never_snapshot_per_task(self):
        hashes = [("%x" % i) * 40 for i in range(1, 6)]
        for tid in hashes:
            self.admit(tid)
        rows = self.p.tasks_status()["tasks"]
        self.assertEqual(len(rows), 5)
        self.assertEqual(self.qb.snapshot_calls, [])
        self.assertEqual(len(self.qb.brief_calls), 1)
        self.p.list_torrents(downloader="qB → 115")
        self.assertEqual(self.qb.snapshot_calls, [])
        self.assertTrue(all(r["qb_progress"] == 100 for r in rows))

    def test_brief_results_are_cached_briefly_so_page_refreshes_do_not_hit_qb_each_time(self):
        self.admit(TID)
        self.p.tasks_status()
        self.p.tasks_status()
        self.p.list_torrents(downloader="qB → 115")
        self.assertEqual(len(self.qb.brief_calls), 1)

    def test_untracked_tasks_are_not_queried_for_status_or_guard(self):
        # round 3: untracked tasks ARE swept by the guard, but only while their source dir still exists; this one has
        # none under the test root, so the original expectation (status/brief never query untracked) still holds.
        self.admit(TID)
        self.admit("b" * 40, state="untracked")
        self.p.guard()
        self.assertEqual(self.qb.report_calls, [[TID]])
        self.p.tasks_status()
        self.assertEqual(self.qb.brief_calls, [[TID]])

    def test_guard_failure_is_logged_with_a_fixed_code_and_shown_on_the_page(self):
        self.admit(TID)
        self.qb.report_error = ValueError("boom http://openlist.example/secret-token")
        with self.assertLogs("qb115downloader", "WARNING") as logs:
            self.p.guard()
        text = "\n".join(logs.output)
        self.assertIn("guard_failed", text)
        self.assertNotIn("secret-token", text)
        self.assertNotIn("http://", text)
        page = json.dumps(self.p.get_page(), ensure_ascii=False)
        self.assertIn("隔离巡检失败", page)
        self.assertNotIn("secret-token", page)
        self.assertIn("隔离巡检失败", self.p.tasks_status()["blocked"])
        self.qb.report_error = None
        self.p.guard()
        self.assertNotIn("隔离巡检失败", json.dumps(self.p.get_page(), ensure_ascii=False))

    def test_unavailable_qb_adapter_for_guard_is_also_visible(self):
        self.admit(TID)

        def broken():
            raise ValueError("physical qB unavailable")
        self.p._qb_adapter = broken
        self.p.guard()
        self.assertIn("隔离巡检失败", json.dumps(self.p.get_page(), ensure_ascii=False))


# ---------------------------------------------------------------- 7: quarantine mitigation
class NativeTags(Native):
    """Same signatures as app/modules/qbittorrent/qbittorrent.py (read-only checked via the contract tool)."""

    def __init__(self):
        super().__init__()
        self.removed = []
        self.remove_ok = True

    def stop_torrents(self, ids):
        return True

    def remove_torrents_tag(self, ids, tag):
        self.removed.append((ids, tag))
        if not self.remove_ok:
            return False
        for entry in self.entries:
            if entry["hash"] == ids:
                entry["tags"] = ",".join(t for t in entry["tags"].split(",") if t not in tag)
        return True

    def delete_torrents(self, *a, **k):
        raise AssertionError("the plugin must never delete a torrent")

    torrents_delete = delete_torrents


class R7_UntagAdapter(unittest.TestCase):
    def make(self, tags="mp-qb115,MOVIEPILOT", global_tag="MOVIEPILOT"):
        native = NativeTags()
        native.entries = [dict(hash=TID, name="seed.partial", tags=tags, save_path="/qb/isolated", progress=1,
                               state="pausedUP")]
        return native, adapter.QbAdapter(native, "physical", "/mp/isolated", lambda: global_tag)

    def test_only_the_global_tag_is_removed_from_only_that_task(self):
        native, a = self.make()
        self.assertEqual(a.untag_global(TID), "removed")
        self.assertEqual(native.removed, [(TID, ["MOVIEPILOT"])])
        self.assertEqual(native.entries[0]["tags"], "mp-qb115")

    def test_no_global_tag_means_no_call(self):
        native, a = self.make(tags="mp-qb115")
        self.assertEqual(a.untag_global(TID), "none")
        self.assertEqual(native.removed, [])

    def test_failed_removal_is_reported_and_unusable_global_tag_never_touches_own_tag(self):
        native, a = self.make()
        native.remove_ok = False
        self.assertEqual(a.untag_global(TID), "failed")
        for global_tag in ("", "mp-qb115", "x,mp-qb115"):
            native, a = self.make(global_tag=global_tag)
            self.assertEqual(a.untag_global(TID), "failed")
            self.assertEqual(native.removed, [])

    def test_unreachable_task_is_a_failure_not_a_success(self):
        native, a = self.make()
        native.get_torrents = lambda ids=None: ([], True)
        self.assertEqual(a.untag_global(TID), "failed")


class R7_GuardQuarantine(Workflow):
    def test_guard_pauses_first_then_untags_only_that_task_and_records_it(self):
        self.qb.lost.add(self.tid)
        with self.assertLogs("qb115downloader", "INFO") as logs:
            self.engine.guard(self.qb)
        self.assertEqual(self.qb.order, ["pause", "untag"])
        self.assertEqual(self.qb.untagged, [self.tid])
        self.assertTrue(self.task["quarantined"] and self.task["paused"])
        self.assertIn("已摘除全局整理标签", self.task["error"])
        self.assertIn("event=quarantine", "\n".join(logs.output))
        self.assertIn("untag=removed", "\n".join(logs.output))

    def test_untag_failure_is_visible_and_retried_on_the_next_sweep(self):
        self.qb.untag_result = "failed"
        self.qb.lost.add(self.tid)
        self.engine.guard(self.qb)
        self.assertTrue(self.task["quarantined"])
        self.assertIn("摘除全局标签失败", self.task["error"])
        self.engine.guard(self.qb)
        self.assertEqual(self.qb.untagged, [self.tid, self.tid])
        self.qb.untag_result = "removed"
        self.engine.guard(self.qb)
        self.assertIn("已摘除全局整理标签", self.task["error"])
        self.engine.guard(self.qb)
        self.assertEqual(len(self.qb.untagged), 3, "clean again: no repeated action")

    def test_quarantined_task_that_regains_the_global_tag_is_untagged_again(self):
        self.qb.lost.add(self.tid)
        self.engine.guard(self.qb)
        self.assertEqual(len(self.qb.untagged), 1)
        self.qb.lost.add(self.tid)  # somebody re-added it
        self.engine.guard(self.qb)
        self.assertEqual(len(self.qb.untagged), 2)

    def test_own_tag_loss_alone_pauses_once_and_does_not_spam_or_untag(self):
        self.qb.own_missing.add(self.tid)
        self.engine.guard(self.qb)
        self.assertTrue(self.task["quarantined"])
        self.assertEqual(self.qb.untagged, [])
        self.engine.guard(self.qb)
        self.assertEqual(self.qb.paused, [self.tid])

    def test_absent_hash_in_qb_is_ignored_by_the_sweep(self):
        self.qb.absent.add(self.tid)
        self.engine.guard(self.qb)
        self.assertFalse(self.engine.tasks[self.tid].get("quarantined"))
        self.assertEqual(self.qb.paused, [])

    def test_release_requires_hash_present_and_own_tag_and_no_global_tag(self):
        self.qb.lost.add(self.tid)
        self.engine.guard(self.qb)
        self.qb.lost.clear()  # the global tag is gone (removed by the guard or by hand) ...
        self.qb.own_missing.add(self.tid)  # ... but the own tag is not there
        self.assertFalse(self.engine.control(self.tid, "release", self.qb), "own tag missing")
        self.qb.own_missing.clear()
        self.qb.absent.add(self.tid)
        self.assertFalse(self.engine.control(self.tid, "release", self.qb), "hash not found in qB")
        self.qb.absent.clear()
        self.qb.lost.add(self.tid)
        self.assertFalse(self.engine.control(self.tid, "release", self.qb), "global tag back")
        self.qb.lost.clear()
        self.assertTrue(self.engine.control(self.tid, "release", self.qb))
        self.assertFalse(self.task["quarantined"])

    def test_plugin_has_no_deletion_code_path(self):
        banned = ("unlink", "rmtree", "os.remove", "torrents_delete", "delete_torrents", "fs/remove", "fs/rename")
        for name in ("core.py", "cloud.py", "adapter.py", "__init__.py"):
            source = (PLUGIN / name).read_text()
            for word in banned:
                self.assertNotIn(word, source, "%s must not contain %s" % (name, word))


# ---------------------------------------------------------------- 8: config validation / origin / raw host
class R8_ConfigValidation(unittest.TestCase):
    def plugin(self, **config):
        entries = []

        class Store:
            def get(self, key):
                return copy.deepcopy(entries)

            def update_atomically(self, key, mutation):  # round 3: the registry is only written atomically
                result, value = mutation(copy.deepcopy(entries))
                entries[:] = copy.deepcopy(value)
                return result
        p = load_plugin().Qb115Downloader()
        p._entry_store = lambda: (Store(), "Downloaders")
        complete = dict(physical_downloader="physical", local_root="/data/qb115", isolation_confirmed=True)
        p.init_plugin(dict(enabled=True, **complete, **config))
        return p, entries

    def test_invalid_values_refuse_to_enable_and_say_why_on_the_page(self):
        for key, value in [("put_timeout_seconds", -1), ("put_timeout_seconds", "abc"), ("put_timeout_seconds", 1.5),
                           ("put_timeout_seconds", True), ("low_water_gib", -1), ("low_water_gib", "ten"),
                           ("poll_interval", 5), ("poll_interval", 0), ("poll_interval", "fast")]:
            with self.subTest(key=key, value=value):
                p, entries = self.plugin(**{key: value})
                self.assertFalse(p.get_state())
                self.assertEqual(p.get_service(), [])
                self.assertEqual(entries, [], "no downloader entry registered for a rejected config")
                page = json.dumps(p.get_page(), ensure_ascii=False)
                self.assertIn(key, page)
                self.assertIn("配置无效", page)

    def test_boundary_values_and_numeric_strings_from_the_form_are_accepted(self):
        p, entries = self.plugin(put_timeout_seconds=0, low_water_gib="0", poll_interval="10")
        self.assertTrue(p.get_state())
        self.assertEqual(len(entries), 1)
        self.assertEqual(p.get_service()[0]["kwargs"]["seconds"], 10)
        self.assertNotIn("配置无效", json.dumps(p.get_page(), ensure_ascii=False))

    def test_poll_interval_is_no_longer_silently_clamped(self):
        p, _ = self.plugin(poll_interval=3)
        self.assertEqual(p.get_service(), [])


class R8_OriginAndRawHost(unittest.TestCase):
    def test_plaintext_http_is_limited_to_loopback_and_private_addresses(self):
        for origin in ("http://127.0.0.1:5244", "http://192.168.1.2:5244", "http://10.0.0.5", "http://172.16.3.4:1",
                       "http://[::1]:5244", "https://example.com", "https://8.8.8.8:5244"):
            with self.subTest(origin=origin):
                cloud.OpenList(origin, "t")
        for origin in ("http://8.8.8.8:5244", "http://example.com:5244", "http://1.2.3.4", "http://172.32.0.1"):
            with self.subTest(origin=origin), self.assertRaises(ValueError) as caught:
                cloud.OpenList(origin, "t", resolver=lambda host: ["93.184.216.34"])
            self.assertNotIn("example.com", str(caught.exception))

    def test_plain_http_hostnames_must_resolve_only_to_private_addresses(self):
        cloud.OpenList("http://openlist:5244", "t", resolver=lambda host: ["172.18.0.2"])
        with self.assertRaises(ValueError):
            cloud.OpenList("http://openlist:5244", "t", resolver=lambda host: ["172.18.0.2", "93.184.216.34"])

        def failing(host):
            raise OSError("no such host")
        with self.assertRaises(ValueError):
            cloud.OpenList("http://openlist:5244", "t", resolver=failing)

    def test_origin_refusal_is_a_config_error_shown_without_the_url(self):
        module = load_plugin()
        p = module.Qb115Downloader()
        p.init_plugin(dict(enabled=False))
        p._enabled = True

        def runtime():
            raise module.ConfigError("OpenList 地址使用明文 http 且不是回环/内网地址，已拒绝；请改用 https 或内网地址")
        p._runtime = runtime
        p.poll()
        self.assertIn("明文 http", p._blocked)
        self.assertNotIn("example.com", p._blocked)

    def test_raw_url_host_allowlist(self):
        allowed = cloud.raw_host_allowed
        self.assertTrue(allowed("127.0.0.1", "127.0.0.1"))
        self.assertTrue(allowed("cdnfhnfile.115cdn.net", "127.0.0.1"))
        self.assertTrue(allowed("fscdn.115.com", "127.0.0.1"))
        self.assertTrue(allowed("a.b.115cdn.com", "127.0.0.1"))
        self.assertTrue(allowed("files.example.org", "127.0.0.1", extra=("files.example.org",)))
        self.assertTrue(allowed("x.cdn.example.org", "127.0.0.1", extra=(".cdn.example.org",)))
        for host in ("evil.example.com", "115.com.evil.com", "fake115cdn.net", "169.254.169.254", "192.168.1.1", "localhost"):
            self.assertFalse(allowed(host, "127.0.0.1"), host)

    def test_refused_raw_host_never_gets_contacted_and_has_a_visible_reason(self):
        server = Server()
        self.addCleanup(server.close)
        server.raw_base = "http://evil.example.com"
        server.objects["/115/staging/a.mkv"] = b"media"
        api = cloud.OpenList(server.url, "secret-token")
        digest = hashlib.sha1(b"media").hexdigest()
        with self.assertRaises(cloud.CloudError) as caught:
            api.verify("/115/staging/a.mkv", dict(size=5, sha1=digest, head=digest))
        self.assertTrue(getattr(caught.exception, "raw_host_refused", False))
        self.assertNotIn("evil.example.com", str(caught.exception))
        self.assertEqual(server.raw_headers, [])

    def test_extra_allowed_host_is_configurable_and_validated(self):
        p = load_plugin().Qb115Downloader()
        complete = dict(physical_downloader="physical", local_root="/data/qb115", isolation_confirmed=True)
        p.init_plugin(dict(enabled=True, raw_url_allowed_hosts="cdn.example.org, .other.example", **complete))
        self.assertTrue(p.get_state())
        self.assertEqual(p._raw_hosts, ("cdn.example.org", ".other.example"))
        p.init_plugin(dict(enabled=True, raw_url_allowed_hosts="https://bad/path", **complete))
        self.assertFalse(p.get_state())
        self.assertIn("raw_url_allowed_hosts", json.dumps(p.get_page(), ensure_ascii=False))


class R8_RefusedRawHostIsVisibleOnTask(Workflow):
    def test_task_error_names_the_reason_without_a_host_or_url(self):
        self.server.raw_base = "http://evil.example.com"
        self.tick()
        self.assertEqual(self.task["state"], "failed")
        self.assertIn("raw_url 主机不在允许名单", self.task["error"])
        self.assertNotIn("evil.example.com", json.dumps(self.persisted))
        self.assertEqual(self.server.moves, [])


# ---------------------------------------------------------------- 9: start must not resume suspended downloads
class R9_StartRefusesSuspended(Workflow):
    def test_start_refuses_suspended_and_admitted_tasks_without_touching_qb(self):
        for state in ("suspended", "admitted"):
            with self.subTest(state=state):
                self.engine.tasks[self.tid].update(state=state, paused=True)
                self.assertFalse(self.engine.control(self.tid, "start", self.qb))
                self.assertEqual(self.qb.started, [])
                self.assertTrue(self.engine.tasks[self.tid]["paused"])

    def test_crashed_admission_cannot_be_started_through_the_plugin_api(self):
        p = load_plugin().Qb115Downloader()
        p.init_plugin(dict(enabled=False, local_root=self.root, low_water_gib=0))
        p._enabled = True
        p._engine.free = lambda: 10**9
        p._engine.admit(TID, "seed.partial", 10, self.root, physical="physical", save_path=self.root, layout="Original")
        p._qb_adapter = lambda: self.qb
        self.assertFalse(p.api_control(hash=TID, action="start")["success"])
        self.assertFalse(p.start_torrents(TID, downloader="qB → 115"))
        self.assertEqual(self.qb.started, [])

    def test_pause_of_suspended_task_still_allowed_and_normal_start_still_works(self):
        self.engine.tasks[self.tid].update(state="suspended")
        self.assertTrue(self.engine.control(self.tid, "pause", self.qb))
        self.engine.tasks[self.tid].update(state="waiting_library", paused=True)
        self.assertTrue(self.engine.control(self.tid, "start", self.qb))


# ---------------------------------------------------------------- 10: README must match the verified facts
class R10_ReadmeFacts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.readme = (PLUGIN / "README.md").read_text()

    def test_overwrite_conflict_is_403_file_exists_not_409(self):
        self.assertIn("file exists", self.readme)
        self.assertIn("403", self.readme)
        self.assertNotIn("会得到 409", self.readme)

    def test_environment_specific_hardcoding_is_declared(self):
        for word in ("/115", "115 Open", "/vol02", "本机环境专用"):
            self.assertIn(word, self.readme)

    def test_quarantine_window_and_manual_steps_are_stated_honestly(self):
        self.assertNotIn("窗口最长约 10 秒", self.readme)
        for word in ("remove_torrents_tag", "仍不能完全消除", "补回 mp-qb115 标签"):
            self.assertIn(word, self.readme)

    def test_ledger_fields_documented_match_the_implementation(self):
        for word in ("quiet_until", "save_path", "verified", "commit_intent", "physical_downloader"):
            self.assertIn(word, self.readme)


if __name__ == "__main__":
    unittest.main()
