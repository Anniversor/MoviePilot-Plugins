import copy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from test_cleanup import CleanupFixture
from test_core import core


class RecoveryEdgeTests(CleanupFixture):
    def test_crash_after_unlink_intent_restarts_without_deleting_reappearing_source(self):
        unlink = core.os.unlink
        def crash(name, **kw):
            unlink(name, **kw)
            raise SystemExit('after unlink')
        with patch.object(core.os, 'unlink', side_effect=crash), self.assertRaises(SystemExit):
            self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True)
        restored = core.Engine(self.saved[-1], lambda x: None, lambda: 100, low_water=0)
        self.assertTrue(restored.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))
        self.assertEqual(len(self.removes), 1)

    def test_unconfirmed_remove_and_readded_hash_never_unlink_source(self):
        self.qb.remove_only = lambda *a: False
        self.assertFalse(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))
        self.assertTrue(self.path.exists())
        self.assertEqual(self.t['cleanup_state'], 'remove_intent')
        self.qb.remove_only = lambda *a: self.fail('WAL must not reissue remove')
        self.assertFalse(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))
        self.assertTrue(self.path.exists())


class PolicyTests(CleanupFixture):
    def test_only_exact_numeric_zero_is_free_unknown_stays_unknown(self):
        for factor, reason in [(0, ''), (1, 'keep_seeding'), (.5, 'keep_seeding'),
                               (None, 'billing_unknown'), ('0', 'billing_unknown'),
                               (False, 'billing_unknown'), (float('nan'), 'billing_unknown')]:
            self.t['billing']['downloadvolumefactor'] = factor
            self.assertEqual(self.e.cleanup_policy(self.t, True), reason)
        self.t.pop('billing')
        self.assertEqual(self.e.cleanup_policy(self.t, True), 'billing_unknown')

    def test_preview_reports_specific_guard_failure_without_raw_exception_text(self):
        self.t['qb_files'] = []
        self.assertEqual(self.preview()['reason'], 'qb_files_or_isolation_changed')
        self.t['qb_files'] = [['seed.partial/a.mkv', 5, 1]]
        self.cloud.verify = lambda *a: {}
        self.assertEqual(self.preview()['reason'], 'cloud_sha1_unavailable_or_mismatch')

    def test_pause_after_remove_intent_stops_local_unlink_and_can_be_held(self):
        remove = self.qb.remove_only
        def pause(tid, identity):
            result = remove(tid, identity)
            self.assertTrue(self.e.control(tid, 'pause', self.qb))
            return result
        self.qb.remove_only = pause
        self.assertFalse(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))
        self.assertTrue(self.path.exists())
        self.assertTrue(self.t['paused'])

    def test_recovery_never_uses_different_physical_instance(self):
        self.t['cleanup_state'] = 'remove_intent'
        self.absent = True
        self.qb.name = 'wrong'
        self.assertFalse(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))
        self.assertTrue(self.path.exists())


class NamespaceTests(CleanupFixture):
    def test_disjoint_legacy_content_does_not_own_entire_shared_root(self):
        self.e.tasks['b'*40] = dict(hash='b'*40, root=str(self.root.parent), name='legacy',
                                    state='external_missing', reservation_released=True)
        self.qb.cleanup_inventory = lambda: [dict(hash='b'*40, save_path=str(self.root.parent),
                                                  content_path=str(self.root.parent / 'legacy'), added_on=99)]
        self.assertTrue(self.preview()['eligible'])
        self.e.tasks['b'*40]['name'] = self.tid
        self.assertFalse(self.preview()['eligible'], 'true parent/child content ownership still blocks')

    def test_cloud_mounted_roots_and_symlinks_are_refused(self):
        for root in ('/115/source', '/115_rclone/source', '/quark_rclone/source', '/vol02/source'):
            self.t['root'] = root
            with self.assertRaises(ValueError):
                self.e._cleanup_manifest(self.t)
            self.assertFalse(self.preview()['eligible'])
        self.t['root'] = str(self.root)
        self.path.unlink()
        self.path.symlink_to(self.root / 'unrelated')
        self.assertFalse(self.preview()['eligible'])
        self.assertEqual(self.removes, [])

    def test_malformed_other_namespace_and_inconsistent_savepath_are_not_ignored(self):
        for other in [dict(hash='b'*40, root=str(self.root.parent), name='../other'),
                      dict(hash='b'*40, root=str(self.root.parent)+'/../x', name='other')]:
            self.e.tasks['b'*40] = other
            self.assertFalse(self.preview()['eligible'])
        self.e.tasks.pop('b'*40)
        self.qb.cleanup_inventory = lambda: [dict(hash='b'*40, save_path=str(self.path.parent),
                                                 content_path='/unrelated/a.mkv', added_on=99)]
        self.assertFalse(self.preview()['eligible'])

    def test_terminal_records_cannot_be_rewritten_by_abandon_or_remove(self):
        self.t.update(state='external_missing')
        self.assertFalse(self.e.abandon(self.tid, self.qb))
        self.assertFalse(self.e.remove(self.tid, self.qb, delete_file=False))
        self.assertEqual(self.t['state'], 'external_missing')

    def test_done_cannot_start_or_retry(self):
        self.assertTrue(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))
        self.assertFalse(self.e.control(self.tid, 'start', self.qb))
        self.assertFalse(self.e.control(self.tid, 'retry', self.qb))


class ReconcileTests(CleanupFixture):
    def test_external_absence_is_terminal_releases_reservation_and_verifies_library(self):
        self.path.unlink()
        self.path.parent.rmdir()
        self.absent = True
        self.e.reconcile(self.qb, self.cloud)
        self.assertEqual(self.t['state'], 'external_missing')
        self.assertNotEqual(self.t.get('cleanup_state'), 'done')
        self.assertTrue(self.t['library_confirmed'])
        self.assertEqual(core.Engine.outstanding(self.t), 0)
        self.assertEqual(self.removes, [])
        for action in ('start', 'retry', 'release'):
            self.assertFalse(self.e.control(self.tid, action, self.qb))
        restored = core.Engine(self.saved[-1], lambda x: None, lambda: 100, low_water=0)
        restored.reconcile(self.qb, self.cloud)
        self.assertEqual(restored.tasks[self.tid]['state'], 'external_missing')

    def test_absence_query_error_or_partial_source_is_never_external_missing(self):
        self.absent = True
        self.e.reconcile(self.qb, self.cloud)
        self.assertNotEqual(self.t['state'], 'external_missing')
        self.assertEqual(self.t['reconcile_reason'], 'qb_absent_source_present_or_partial')
        self.qb.is_absent = lambda tid: (_ for _ in ()).throw(ValueError('network'))
        self.e.reconcile(self.qb, self.cloud)
        self.assertEqual(self.t['reconcile_reason'], 'qb_query_failed')
        self.assertTrue(self.path.exists())
        self.assertEqual(self.removes, [])

    def test_only_identified_commit_recovery_hold_can_resume_after_full_cloud_proof(self):
        self.t.update(state='suspended', recovery_waiting_cloud=True)
        self.e.reconcile(self.qb, self.cloud)
        self.assertTrue(self.t['library_confirmed'])
        self.assertEqual(self.t['state'], 'waiting_library')
        self.t.update(state='suspended', recovery_waiting_cloud=False)
        self.e.reconcile(self.qb, self.cloud)
        self.assertEqual(self.t['state'], 'suspended', 'unrelated safety holds must stay sticky')

    def test_manifestless_missing_task_releases_reservation_but_not_partial_tree(self):
        self.t.update(manifest=[], commit_intent=False, state='downloading')
        self.absent = True
        self.e.reconcile(self.qb, self.cloud)
        self.assertEqual(core.Engine.outstanding(self.t), 5)
        self.path.unlink()
        self.path.parent.rmdir()
        self.e.reconcile(self.qb, self.cloud)
        self.assertEqual(self.t['state'], 'external_missing')
        self.assertEqual(core.Engine.outstanding(self.t), 0)


class StrictCloudTests(CleanupFixture):
    def test_cleanup_requires_fresh_full_cloud_sha1_not_just_readable_head(self):
        for result in (None, {}, {'sha1': ''}, {'sha1': 'f'*40}):
            with self.subTest(result=result):
                self.cloud.verify = lambda path, item: result
                self.assertFalse(self.preview()['eligible'], 'head verification without full SHA1 is insufficient for deletion')
                self.assertFalse(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))
                self.assertTrue(self.path.exists())
                self.assertEqual(self.removes, [])

    def test_clm_consumed_monitor_uses_exact_final_mapping_and_verifies_sha1(self):
        from cloud import CloudMissing
        final = '/115/library/renamed.mkv'
        def verify(path, item):
            self.verifies.append(path)
            if path != final:
                raise CloudMissing('consumed')
            return dict(sha1=item['sha1'])
        self.cloud.verify = verify
        resolver_calls = []
        def resolve(task, item):
            resolver_calls.append((task['hash'], item['source']))
            return final
        self.assertTrue(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True, resolve_final=resolve))
        self.assertEqual(resolver_calls, [(self.tid, 'seed.partial/a.mkv')])
        self.assertIn(final, self.verifies)

    def test_network_failure_does_not_fall_back_to_history(self):
        from cloud import CloudTransientError
        self.cloud.verify = lambda *a: (_ for _ in ()).throw(CloudTransientError('network'))
        self.assertFalse(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True,
                                           resolve_final=lambda *a: self.fail('network failure is not absence')))
        self.assertTrue(self.path.exists())
