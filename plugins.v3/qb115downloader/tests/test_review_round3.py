"""Regression tests for the third independent review (pre-trial defects 1-8). Stdlib only, loopback only.

Every test here was written and run RED against the pre-fix code first (see DEVLOG.md, round 3).
No test deletes anything: tombstones are made by changing ledger fields, not files.
"""
import copy
import hashlib
import json
import os
from contextlib import contextmanager
from pathlib import Path
import sys
import tempfile
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

PLUGIN = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN))
import core
from test_cloud import Server, cloud
from test_plugin import load_plugin
from test_review_fixes import Qb, Workflow, TID
from test_review_round2 import PluginBase

OTHER = "b" * 40
GUARD, POLL = "Qb115DownloaderGuard", "Qb115DownloaderPoll"
COMPLETE = dict(physical_downloader="physical", local_root="/data/qb115", isolation_confirmed=True)


class FakeLock:
    """Stands in for a worker/ledger lock that a long PUT / multi-GB SHA1 still holds."""

    def acquire(self, blocking=True, timeout=-1):
        return timeout == -1  # the reload's timed acquire never succeeds; plain acquire (guard sweep) does

    def release(self):
        pass

    __enter__ = lambda self: self.acquire() and self
    __exit__ = lambda self, *a: None


# ---------------------------------------------------------------- 1: guard survives reload / disable / bad config
class R3_1_GuardSurvives(PluginBase):
    def ids(self):
        return [s["id"] for s in self.p.get_service()]

    def test_blocked_reload_keeps_the_guard_and_the_guard_still_acts(self):
        self.admit(TID, state="uploading")
        self.assertEqual(self.ids(), [POLL, GUARD])
        self.p._engine.worker = FakeLock()  # old worker never releases within the wait
        self.p.init_plugin(dict(enabled=True, **COMPLETE))
        self.assertIn("旧上传仍未停止", self.p._blocked)
        self.assertEqual(self.ids(), [GUARD], "poll must not advance, the isolation guard must stay registered")
        self.assertTrue(self.p.get_state(), "MoviePilot only registers services of a plugin whose get_state() is true")
        self.qb.lost.add(TID)
        self.p.guard()
        self.assertEqual(self.qb.paused, [TID], "guard keeps protecting while the reload is blocked")
        self.assertEqual(self.qb.untagged, [TID])

    def test_ledger_lock_blocked_reload_keeps_the_guard_too(self):
        self.admit(TID, state="uploading")
        self.p._engine.lock = FakeLock()
        self.p.init_plugin(dict(enabled=True, **COMPLETE))
        self.assertIn("旧 qB 提交仍未停止", self.p._blocked)
        self.assertEqual(self.ids(), [GUARD])

    def test_user_disable_keeps_a_guard_only_service_while_the_ledger_has_tasks(self):
        self.admit(TID)
        self.p.init_plugin(dict(enabled=False, **COMPLETE))
        self.assertEqual(self.ids(), [GUARD])
        self.assertTrue(self.p.get_state())
        self.assertIn("仅隔离巡检", self.p.tasks_status()["blocked"])

    def test_invalid_config_keeps_the_guard(self):
        self.admit(TID)
        self.p.init_plugin(dict(enabled=True, poll_interval=1, **COMPLETE))
        self.assertIn("配置无效", self.p._config_error)
        self.assertEqual(self.ids(), [GUARD])

    def test_disabled_guard_only_does_protective_pause_and_untag_never_uploads(self):
        self.admit(TID, state="downloading")
        self.p.init_plugin(dict(enabled=False, **COMPLETE))
        self.qb.lost.add(TID)
        self.p.guard()
        self.assertEqual(self.qb.paused, [TID])
        self.assertEqual(self.qb.untagged, [TID])
        self.p.poll()  # runtime() raises AssertionError if poll tried to advance uploads
        self.assertEqual(self.qb.started, [])

    def test_empty_ledger_has_no_services_and_state_false(self):
        self.p.init_plugin(dict(enabled=False, **COMPLETE))
        self.assertEqual(self.ids(), [])
        self.assertFalse(self.p.get_state())

    def test_enabled_plugin_still_registers_poll_and_guard(self):
        self.admit(TID)
        self.p.init_plugin(dict(enabled=True, **COMPLETE))
        self.assertEqual(self.ids(), [POLL, GUARD])


# ---------------------------------------------------------------- 2: stopped / pause honoured between tasks and while hashing
class R3_2_StopChecks(Workflow):
    def big_file(self, size=4 * 1024 * 1024):
        (Path(self.root) / "seed.partial" / "a.mkv").write_bytes(b"x" * size)
        self.files[0]["size"] = size
        self.engine.tasks[self.tid]["reserved"] = 10 ** 9

    def counting_open(self, on_read):
        reads, real = [], core.open_source

        @contextmanager
        def opener(root, name):
            with real(root, name) as stream:
                class Proxy:
                    def __getattr__(self_, attr):
                        return getattr(stream, attr)

                    def read(self_, size=-1):
                        reads.append(size)
                        on_read(len(reads))
                        return stream.read(size)
                yield Proxy()
        return reads, opener

    def test_stop_during_local_sha1_abandons_the_hash_and_uploads_nothing(self):
        self.big_file()
        reads, opener = self.counting_open(lambda n: self.engine.stopped.set() if n == 2 else None)
        with patch.object(core, "open_source", opener):
            self.tick()
        self.assertLessEqual(len(reads), 3, "a multi-GB hash must not run to the end after stop")
        self.assertEqual(self.server.puts, [])
        self.assertEqual(self.task["state"], "downloading")

    def test_pause_during_local_sha1_abandons_the_hash_too(self):
        self.big_file()
        reads, opener = self.counting_open(
            lambda n: self.engine.tasks[self.tid].update(paused=True) if n == 2 else None)
        with patch.object(core, "open_source", opener):
            self.tick()
        self.assertLessEqual(len(reads), 3)
        self.assertEqual(self.server.puts, [])

    def test_stop_between_tasks_does_not_start_the_next_task(self):
        self.engine.admit(OTHER, "other", 30000, self.root, physical="physical", save_path=self.root,
                          layout="Original")
        self.engine.tasks[OTHER]["state"] = "downloading"
        real = self.qb.snapshot

        def snapshot(tid):
            result = real(tid)
            self.engine.stopped.set()  # reload arrives while the first task is being examined
            return result
        self.qb.snapshot = snapshot
        self.tick()
        self.assertEqual(self.qb.snapshot_calls, [self.tid], "the second task must not even be looked at")
        self.assertEqual(self.server.puts, [])

    def test_recover_commit_never_replays_a_move_after_stop(self):
        def boom(*a):
            raise SystemExit("died after commit_intent")
        self.api.move = boom
        with self.assertRaises(SystemExit):
            self.tick()
        self.assertTrue(self.task["commit_intent"])
        engine = core.Engine(self.persisted, self.save, free=lambda: 10 ** 9, low_water=0)
        api = cloud.OpenList(self.server.url, "secret-token")
        real = api.verify_batch

        def verify_batch(*a):
            result = real(*a)
            engine.stopped.set()
            return result
        api.verify_batch = verify_batch
        engine.tick(self.qb, api, "/115/staging", "/115/watch", now=100)
        self.assertEqual(self.server.moves, [], "stop arrived before the replayed move: nothing may be moved")
        self.assertTrue(self.task["commit_intent"])


# ---------------------------------------------------------------- 3: tombstone / suspend not overwritten by an in-flight _process
class R3_3_StickyStates(Workflow):
    def hook_first_put(self, action):
        real, calls = self.api.put, []

        def put(path, stream, size, sha1):
            calls.append(path)
            if len(calls) == 1:
                action()
            return real(path, stream, size, sha1)
        self.api.put = put

    def test_untrack_during_put_survives_the_inflight_tick(self):
        self.hook_first_put(lambda: self.engine.remove(self.tid, self.qb, delete_file=False))
        self.tick()
        self.assertEqual(self.task["state"], "untracked")
        self.assertTrue(self.task["paused"])
        self.assertEqual(len(self.server.puts), 1, "no next file after the tombstone")
        self.assertEqual(self.server.moves, [])

    def test_control_suspend_during_put_survives_the_inflight_tick(self):
        other = Qb(self.root, self.files, name="other")
        self.hook_first_put(lambda: self.engine.control(self.tid, "pause", other))
        self.tick()
        self.assertEqual(self.task["state"], "suspended")
        self.assertEqual(other.paused, [], "never touch another qB instance")

    def test_suspend_never_resurrects_an_untracked_task(self):
        self.engine.remove(self.tid, self.qb, delete_file=False)
        self.engine._suspend(self.engine.tasks[self.tid], "late suspend", self.qb)
        self.assertEqual(self.task["state"], "untracked")

    def test_failure_backoff_does_not_overwrite_sticky_states_or_their_message(self):
        task = self.engine.tasks[self.tid]
        self.engine._suspend(task, "reason to keep")
        self.engine._backoff(task, 1000.0, state="failed", message="generic failure")
        self.assertEqual(self.task["state"], "suspended")
        self.assertIn("reason to keep", self.task["error"])

    def test_plain_state_writes_cannot_leave_untracked_or_suspended(self):
        task = self.engine.tasks[self.tid]
        self.engine.remove(self.tid, self.qb, delete_file=False)
        for state in ("uploading", "verifying", "failed", "downloading", "waiting_library"):
            self.engine._set(task, state=state)
            self.assertEqual(self.task["state"], "untracked", state)


# ---------------------------------------------------------------- 4: suspend pauses qB, abandon, unsuspend
class R3_4_Suspend(Workflow):
    def frozen_task(self):
        """A task with a frozen manifest and no commit_intent, then suspended (uploads done, verify failed)."""
        self.server.raw_base = "http://evil.example.com"  # verify fails after the PUTs: manifest frozen, no handoff
        self.tick()
        task = self.engine.tasks[self.tid]
        self.assertTrue(task["manifest"] and not task.get("commit_intent"))
        self.server.raw_base = None
        self.engine._suspend(task, "manual reason")
        self.qb.paused.clear()
        return task

    def test_binding_suspend_requests_qb_pause_and_says_so(self):
        self.qb.layout = "Subfolder"  # layout drift on the right qB instance
        self.tick()
        self.assertEqual(self.task["state"], "suspended")
        self.assertEqual(self.qb.paused, [self.tid])
        self.assertIn("已请求暂停 qB", self.task["error"])

    def test_suspend_through_a_different_qb_instance_never_pauses_it(self):
        other = Qb(self.root, self.files, name="other")
        self.engine.control(self.tid, "start", other)
        self.assertEqual(self.task["state"], "suspended")
        self.assertEqual(other.paused + other.started, [])

    def test_failed_pause_is_reported_honestly(self):
        self.qb.pause = lambda tid: False
        self.qb.layout = "Subfolder"
        self.tick()
        self.assertEqual(self.task["state"], "suspended")
        self.assertIn("暂停 qB 失败", self.task["error"])

    def test_admitted_task_suspended_by_tick_pauses_qb(self):
        self.engine.tasks[self.tid]["state"] = "admitted"
        self.tick()
        self.assertEqual(self.task["state"], "suspended")
        self.assertEqual(self.qb.paused, [self.tid])

    def test_abandon_tombstones_a_suspended_task_and_keeps_everything(self):
        task = self.engine.tasks[self.tid]
        self.engine._suspend(task, "stuck")
        self.qb.paused.clear()
        before = (sum(core.Engine.outstanding(t) for t in self.engine.tasks.values()), dict(self.server.objects))
        self.assertTrue(self.engine.abandon(self.tid, self.qb))
        self.assertEqual(self.task["state"], "untracked")
        self.assertTrue(self.task["paused"])
        self.assertEqual(self.qb.paused, [self.tid])
        self.assertIn("abandon", self.task["error"])
        self.assertEqual((sum(core.Engine.outstanding(t) for t in self.engine.tasks.values()),
                          dict(self.server.objects)), before, "reservation and cloud untouched")
        self.assertFalse(self.engine.abandon(self.tid, self.qb), "already untracked")
        self.assertFalse(self.engine.abandon("c" * 40, self.qb), "unknown hash")
        self.assertFalse(self.engine.control(self.tid, "start", self.qb), "tombstone stays")

    def test_abandon_with_a_mismatching_qb_still_tombstones_but_never_touches_it(self):
        other = Qb(self.root, self.files, name="other")
        self.assertTrue(self.engine.abandon(self.tid, other))
        self.assertEqual(self.task["state"], "untracked")
        self.assertEqual(other.paused, [])

    def test_unsuspend_requires_every_check_and_keeps_the_task_paused(self):
        task = self.frozen_task()
        started = list(self.qb.started)
        self.assertFalse(self.engine.unsuspend(self.tid, self.qb, confirm=False))
        self.assertFalse(self.engine.unsuspend(self.tid, self.qb))
        self.assertEqual(self.task["state"], "suspended")
        self.assertTrue(self.engine.unsuspend(self.tid, self.qb, confirm=True))
        self.assertEqual(self.task["state"], "downloading")
        self.assertTrue(self.task["paused"], "an explicit start is still required")
        self.assertEqual(self.qb.started, started, "unsuspend never resumes qB")
        self.assertTrue(task["manifest"], "frozen manifest kept")

    def test_unsuspend_refuses_without_a_frozen_manifest(self):
        self.engine._suspend(self.engine.tasks[self.tid], "selection never confirmed")
        self.assertFalse(self.engine.unsuspend(self.tid, self.qb, confirm=True))
        self.assertEqual(self.task["state"], "suspended")

    def test_unsuspend_refuses_after_a_handoff_was_started(self):
        task = self.frozen_task()
        task["commit_intent"] = True
        self.assertFalse(self.engine.unsuspend(self.tid, self.qb, confirm=True))
        self.assertEqual(self.engine.tasks[self.tid]["state"], "suspended")

    def test_unsuspend_refuses_when_binding_selection_or_source_changed(self):
        self.frozen_task()
        self.qb.layout = "Subfolder"
        self.assertFalse(self.engine.unsuspend(self.tid, self.qb, confirm=True), "layout drift")
        self.qb.layout = "Original"
        other = Qb(self.root, self.files, name="other")
        self.assertFalse(self.engine.unsuspend(self.tid, other, confirm=True), "other physical downloader")
        self.files[1]["priority"] = 0
        self.assertFalse(self.engine.unsuspend(self.tid, self.qb, confirm=True), "selection changed")
        self.files[1]["priority"] = 1
        source = Path(self.root) / "seed.partial" / "a.mkv"
        os.utime(source, ns=(1, 1))
        self.assertFalse(self.engine.unsuspend(self.tid, self.qb, confirm=True), "source signature changed")
        self.qb.lost.add(self.tid)
        self.assertFalse(self.engine.unsuspend(self.tid, self.qb, confirm=True), "isolation lost")
        self.assertEqual(self.task["state"], "suspended")

    def test_unsuspend_refuses_quarantined_task(self):
        task = self.frozen_task()
        task["quarantined"] = True
        self.assertFalse(self.engine.unsuspend(self.tid, self.qb, confirm=True))

    def test_unsuspend_refuses_non_suspended_tasks(self):
        self.assertFalse(self.engine.unsuspend(self.tid, self.qb, confirm=True))
        self.assertFalse(self.engine.unsuspend("c" * 40, self.qb, confirm=True))


class R3_4_SuspendApi(PluginBase):
    def test_api_abandon_and_unsuspend_need_explicit_actions_and_confirm(self):
        self.admit(TID, state="suspended")
        refused = self.p.api_control(hash=TID, action="unsuspend")
        self.assertFalse(refused["success"])
        self.assertIn("confirm", refused["message"])
        wrong = self.p.api_control(hash=TID, action="unsuspend", confirm="yes")
        self.assertFalse(wrong["success"])
        self.assertEqual(self.p._engine.tasks[TID]["state"], "suspended")
        checked = self.p.api_control(hash=TID, action="unsuspend", confirm=TID)
        self.assertFalse(checked["success"], "no frozen manifest: refused by the engine")
        self.assertNotIn("仅支持", checked["message"])
        self.assertTrue(self.p.api_control(hash=TID, action="abandon")["success"])
        self.assertEqual(self.p._engine.tasks[TID]["state"], "untracked")
        self.assertEqual(self.qb.paused, [TID])

    def test_no_delete_action_exists(self):
        for action in ("delete", "remove", "rename", "move"):
            self.assertFalse(self.p.api_control(hash=TID, action=action)["success"])


# ---------------------------------------------------------------- 5: untracked tasks keep a light guard
class R3_5_UntrackedGuard(Workflow):
    def untrack(self):
        self.engine.remove(self.tid, self.qb, delete_file=False)
        self.qb.paused.clear()

    def test_untracked_task_that_regains_the_global_tag_is_paused_and_untagged(self):
        self.untrack()
        self.qb.lost.add(self.tid)
        self.engine.guard(self.qb)
        self.assertEqual(self.qb.order[-2:], ["pause", "untag"])
        self.assertEqual(self.qb.untagged, [self.tid])
        self.assertEqual(self.task["state"], "untracked", "no quarantine flag, tombstone stays")
        self.assertIn("已摘除全局整理标签", self.task["error"])
        self.engine.guard(self.qb)
        self.assertEqual(self.qb.untagged, [self.tid], "clean again: no repeated action")

    def test_untracked_without_our_tag_is_left_alone(self):
        self.untrack()
        self.qb.lost.add(self.tid)
        self.qb.own_missing.add(self.tid)  # the human took the task over (README: remove mp-qb115 first)
        self.engine.guard(self.qb)
        self.assertEqual(self.qb.paused + self.qb.untagged, [])

    def test_untracked_whose_source_is_gone_or_hash_absent_is_left_alone(self):
        self.untrack()
        self.qb.lost.add(self.tid)
        self.engine.tasks[self.tid]["name"] = "no-such-source"
        self.engine.guard(self.qb)
        self.assertEqual(self.qb.paused + self.qb.untagged, [])
        self.engine.tasks[self.tid]["name"] = "seed.partial"
        self.qb.lost.discard(self.tid)  # the real adapter reports a hash that qB does not know as ["absent"] only
        self.qb.absent.add(self.tid)
        self.engine.guard(self.qb)
        self.assertEqual(self.qb.paused + self.qb.untagged, [])

    def test_guard_sweeps_untracked_hashes_with_the_tracked_ones(self):
        (Path(self.root) / "other").mkdir()
        self.engine.admit(OTHER, "other", 10, self.root, physical="physical", save_path=self.root, layout="Original")
        self.engine.tasks[OTHER]["state"] = "untracked"
        self.engine.guard(self.qb)
        self.assertEqual(sorted(self.qb.report_calls[0]), sorted([self.tid, OTHER]))


class R3_5_Plugin(PluginBase):
    def test_a_ledger_of_only_untracked_tasks_keeps_the_guard_registered(self):
        self.admit(TID, state="untracked")
        self.p.init_plugin(dict(enabled=False, **COMPLETE))
        self.assertEqual([s["id"] for s in self.p.get_service()], [GUARD])


# ---------------------------------------------------------------- 6: config precheck, atomic registry write, quiet empty ledger
class AtomicStore:
    """Only the atomic interface exists: a stale read-modify-write (``set``) must fail the test."""

    def __init__(self, entries=None, concurrent=None):
        self.entries = copy.deepcopy(entries or [])
        self.concurrent = concurrent  # an entry another writer adds between our read and our write
        self.atomic_calls = 0

    def get(self, key):
        return copy.deepcopy(self.entries)

    def set(self, key, value):
        raise AssertionError("read-modify-write of the downloader list is forbidden; use update_atomically")

    def update_atomically(self, key, mutation):
        self.atomic_calls += 1
        if self.concurrent:
            self.entries.append(self.concurrent)
            self.concurrent = None
        result, value = mutation(copy.deepcopy(self.entries))
        self.entries[:] = copy.deepcopy(value)
        return result


class R3_6_Registration(unittest.TestCase):
    def plugin(self, store, **config):
        p = load_plugin().Qb115Downloader()
        p._entry_store = lambda: (store, "Downloaders")
        p.init_plugin(config)
        return p

    def test_incomplete_config_does_not_register_and_names_the_missing_fields(self):
        store = AtomicStore()
        p = self.plugin(store, enabled=True)
        self.assertEqual(store.entries, [])
        self.assertFalse(p.get_state())
        page = json.dumps(p.get_page(), ensure_ascii=False)
        self.assertIn("配置不完整", page)
        self.assertIn("physical_downloader", page)
        self.assertIn("local_root", page)
        self.assertIn("isolation_confirmed", page)
        self.assertEqual(p.get_service(), [])

    def test_each_precheck_failure_blocks_registration(self):
        for key, value in [("physical_downloader", ""), ("local_root", ""), ("local_root", "relative/path"),
                           ("local_root", "/"), ("local_root", "/115/qb"), ("local_root", "/vol02/x"),
                           ("isolation_confirmed", False), ("credential_source", "SomethingElse"),
                           ("staging_path", "/elsewhere/stage"), ("target_path", "/elsewhere/watch")]:
            with self.subTest(key=key, value=value):
                store = AtomicStore()
                p = self.plugin(store, enabled=True, **{**COMPLETE, key: value})
                self.assertEqual(store.entries, [])
                self.assertFalse(p.get_state())
                self.assertIn("配置不完整", p._config_error)

    def test_complete_config_registers_through_the_atomic_interface_and_keeps_concurrent_writes(self):
        mine = dict(name="physical", type="qbittorrent", enabled=True, default=True, config={"private": "not-read"})
        racer = dict(name="someone-else", type="transmission", enabled=True, default=False, config={})
        store = AtomicStore([mine], concurrent=racer)
        p = self.plugin(store, enabled=True, **COMPLETE)
        self.assertTrue(p.get_state())
        self.assertEqual(store.atomic_calls, 1)
        self.assertEqual([e["name"] for e in store.entries], ["physical", "someone-else", "qB → 115"])
        self.assertEqual(store.entries[0], mine)
        self.assertEqual(store.entries[1], racer)
        self.assertFalse(store.entries[2]["default"])
        p.init_plugin(dict(enabled=True, **COMPLETE))
        self.assertEqual(store.atomic_calls, 1, "already registered: no write at all")

    def test_disable_removes_only_our_entry_atomically(self):
        store = AtomicStore([dict(name="physical", type="qbittorrent", enabled=True, default=True, config={})])
        p = self.plugin(store, enabled=True, **COMPLETE)
        self.assertEqual(len(store.entries), 2)
        p.init_plugin(dict(enabled=False, **COMPLETE))
        self.assertEqual([e["name"] for e in store.entries], ["physical"])
        calls = store.atomic_calls
        p.init_plugin(dict(enabled=False, **COMPLETE))
        self.assertEqual(store.atomic_calls, calls, "nothing to remove: no write")

    def test_name_collision_is_still_refused_inside_the_atomic_update(self):
        store = AtomicStore([dict(name="qB → 115", type="qbittorrent", enabled=True, default=False, config={})])
        p = self.plugin(store, enabled=True, **COMPLETE)
        self.assertEqual(store.entries[0]["type"], "qbittorrent")
        self.assertTrue(p._name_conflict)

    def test_real_entry_store_uses_systemconfig_update_atomically_with_the_session_signature(self):
        recorded = {}

        class Oper:
            def get(self, key):
                return list(recorded.get("value") or [])

            def update_atomically(self, key, mutation):  # app/db/oper/systemconfig.py: mutation(session, current)
                result, value = mutation(object(), list(recorded.get("value") or []))
                recorded["value"], recorded["key"] = value, key
                return result

            def set(self, *a):
                raise AssertionError("must not use set")
        modules = {}
        for name in ("app", "app.db", "app.db.oper", "app.db.oper.systemconfig", "app.schemas", "app.schemas.types"):
            modules[name] = ModuleType(name)
        modules["app.db.oper.systemconfig"].SystemConfigOper = Oper
        modules["app.schemas.types"].SystemConfigKey = SimpleNamespace(Downloaders="Downloaders")
        p = load_plugin().Qb115Downloader()
        with patch.dict(sys.modules, modules):
            store, key = p._entry_store()
            self.assertEqual(key, "Downloaders")
            self.assertEqual(store.update_atomically(key, lambda current: ("ok", [{"name": "x"}])), "ok")
        self.assertEqual(recorded["value"], [{"name": "x"}])
        self.assertEqual(recorded["key"], "Downloaders")


class R3_6_QuietEmptyLedger(unittest.TestCase):
    def test_empty_ledger_never_raises_the_isolation_alarm(self):
        p = load_plugin().Qb115Downloader()
        p.init_plugin(dict(enabled=False))
        p._enabled = True

        def broken():
            raise ValueError("physical_downloader missing")
        p._qb_adapter = broken
        p.guard()
        self.assertFalse(p._engine.guard_failed)
        self.assertNotIn("隔离巡检失败", json.dumps(p.get_page(), ensure_ascii=False))

    def test_non_empty_ledger_with_unusable_adapter_still_alarms(self):
        p = load_plugin().Qb115Downloader()
        p.init_plugin(dict(enabled=False))
        p._engine.admit(TID, "seed", 10, "/r", physical="physical", save_path="/r", layout="Original")
        p._enabled = True

        def broken():
            raise ValueError("physical qB unavailable")
        p._qb_adapter = broken
        p.guard()
        self.assertTrue(p._engine.guard_failed)


# ---------------------------------------------------------------- 7: same-host raw_url is pinned to the OpenList port
class R3_7_RawPort(unittest.TestCase):
    def test_same_host_requires_the_origin_port_other_hosts_are_unchanged(self):
        allowed = cloud.raw_host_allowed
        self.assertTrue(allowed("127.0.0.1", "127.0.0.1"), "legacy 3-argument call unchanged")
        self.assertTrue(allowed("127.0.0.1", "127.0.0.1", port=5244, origin_port=5244))
        self.assertFalse(allowed("127.0.0.1", "127.0.0.1", port=22, origin_port=5244))
        self.assertFalse(allowed("127.0.0.1", "127.0.0.1", port=80, origin_port=5244))
        self.assertTrue(allowed("fscdn.115.com", "127.0.0.1", port=443, origin_port=5244), "CDN family: any port")
        self.assertTrue(allowed("files.example.org", "127.0.0.1", extra=("files.example.org",), port=8443,
                                origin_port=5244))

    def test_verify_refuses_same_host_other_port_without_contacting_it(self):
        server = Server()
        self.addCleanup(server.close)
        server.raw_base = "http://127.0.0.1:%s" % (server.server_port + 1)
        server.objects["/115/staging/a.mkv"] = b"media"
        api = cloud.OpenList(server.url, "secret-token")
        digest = hashlib.sha1(b"media").hexdigest()
        with self.assertRaises(cloud.CloudError) as caught:
            api.verify("/115/staging/a.mkv", dict(size=5, sha1=digest, head=digest))
        self.assertTrue(getattr(caught.exception, "raw_host_refused", False))
        self.assertEqual(server.raw_headers, [])

    def test_cgnat_http_origin_is_refused_as_documented(self):
        for origin in ("http://100.64.1.1:5244", "http://100.127.255.254"):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                cloud.OpenList(origin, "t", resolver=lambda host: ["100.64.1.1"])
        cloud.OpenList("https://100.64.1.1:5244", "t")

    def test_same_host_same_port_still_verifies(self):
        server = Server()
        self.addCleanup(server.close)
        server.objects["/115/staging/a.mkv"] = b"media"
        api = cloud.OpenList(server.url, "secret-token")
        digest = hashlib.sha1(b"media").hexdigest()
        self.assertTrue(api.verify("/115/staging/a.mkv", dict(size=5, sha1=digest, head=digest))["readable"])

    def test_invalid_port_in_raw_url_is_a_fixed_error(self):
        server = Server()
        self.addCleanup(server.close)
        server.raw_base = "http://127.0.0.1:99999"
        server.objects["/115/staging/a.mkv"] = b"media"
        api = cloud.OpenList(server.url, "secret-token")
        digest = hashlib.sha1(b"media").hexdigest()
        with self.assertRaises(cloud.CloudError):
            api.verify("/115/staging/a.mkv", dict(size=5, sha1=digest, head=digest))
        self.assertEqual(server.raw_headers, [])


# ---------------------------------------------------------------- 8: README states the real behaviour and trial steps
README = (PLUGIN / "README.md").read_text(encoding="utf-8")


class R3_8_Readme(unittest.TestCase):
    def need(self, *needles):
        for needle in needles:
            self.assertIn(needle, README, needle)

    def test_guard_and_poll_semantics_for_disabled_invalid_and_blocked_reload(self):
        self.need("禁用状态", "配置无效", "重载被阻塞", "只做保护性暂停", "不推进上传", "get_state")

    def test_opens_listing_cache_and_download_link_cost(self):
        self.need("目录缓存", "115 下载链接")

    def test_move_precheck_is_not_atomic(self):
        self.need("非原子", "FsMove")

    def test_tombstone_is_sticky_now(self):
        self.need("墓碑", "不会被在途", "suspended")

    def test_torrent_tag_and_layout_consequences(self):
        self.need("TORRENT_TAG", "包括保种", "全局内容布局", "挂起所有任务")

    def test_network_and_raw_url_caveats(self):
        self.need("100.64.0.0/10", "site_url", "web_proxy", "raw_url_allowed_hosts", "OpenList 同主机")

    def test_ledger_location_and_uninstall_leftover(self):
        self.need("plugindata", "tasks_v1", "qb115virtual", "手工删除")

    def test_system_version_dependency_is_stated(self):
        self.need("system_version", "v3.2.3")

    def test_manual_actions_and_untracked_guard_documented(self):
        self.need("abandon", "unsuspend", "confirm=", "untracked", "mp-qb115 标签")

    def test_trial_steps(self):
        self.need("试用步骤", "硬链接", "不要用“移动”", "小型无私密测试种子", "不要保存插件配置")

    def test_atomic_downloader_registry_write_is_documented(self):
        self.need("update_atomically")


if __name__ == "__main__":
    unittest.main()
