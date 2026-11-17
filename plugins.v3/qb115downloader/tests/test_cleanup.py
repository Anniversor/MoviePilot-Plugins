"""Free cleanup tracer bullets; only temporary local files and fake/loopback peers."""
import copy
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from test_core import core, FakeQb


class BillingTests(unittest.TestCase):
    def test_submission_billing_is_immutable_and_requires_exact_acceptance(self):
        e = core.Engine({}, lambda x: None, lambda: 10000, low_water=0)
        t = e.admit('a' * 40, 'a.mkv', 4, '/local', physical='physical', save_path='/local')
        self.assertTrue(callable(getattr(e, 'capture_billing', None)), 'submission billing capture missing')
        e.capture_billing(t['hash'], 3, 0, 'resource-one')
        self.assertFalse(e.confirm_billing(t['hash'], 4, 'resource-one'))
        self.assertFalse(e.confirm_billing(t['hash'], 3, 'resource-two'))
        self.assertTrue(e.confirm_billing(t['hash'], 3, 'resource-one'))
        frozen = copy.deepcopy(t['billing'])
        e.capture_billing(t['hash'], 3, 1, 'resource-one')
        self.assertEqual(t['billing'], frozen, 'later promotion changes must not rewrite original billing')
        restored = core.Engine(e.tasks, lambda x: None, lambda: 10000, low_water=0)
        self.assertEqual(restored.tasks[t['hash']]['billing'], frozen)


class EventBillingTests(unittest.TestCase):
    def test_mp_events_bind_pre_submission_factor_to_accepted_hash_not_title(self):
        from test_plugin import load_plugin
        from test_core import bencode
        p = load_plugin().Qb115Downloader()
        p.init_plugin({})
        p._enabled = True
        p._engine.free = lambda: 10**12
        qb = FakeQb('/local', [])
        qb.submit = lambda content, tid, **kw: ('physical', tid, 'Original', 'ok')
        p._runtime = lambda: (qb, None)
        context = {'torrent_info': {'site': 3, 'enclosure': 'https://site/download?id=199',
                                    'title': 'same-title', 'downloadvolumefactor': 0}}
        from test_submission_scope import submit_via_mp
        raw = bencode({b'info': {b'name': b'a.mkv', b'length': 4, b'pieces': b'x' * 20, b'piece length': 16384}})
        tid = submit_via_mp(p, context, raw)[1]
        context['torrent_info']['downloadvolumefactor'] = 1  # promotion changes after submission capture
        self.assertIsNotNone(tid)
        self.assertFalse(p._engine.tasks[tid]['billing']['accepted'])
        wrong = copy.deepcopy(context)
        wrong['torrent_info']['enclosure'] += '0'
        p.on_download_added(SimpleNamespace(event_data={'downloader': p.DOWNLOADER_NAME, 'hash': tid, 'context': wrong}))
        self.assertFalse(p._engine.tasks[tid]['billing']['accepted'])
        p.on_download_added(SimpleNamespace(event_data={'downloader': p.DOWNLOADER_NAME, 'hash': tid, 'context': context}))
        self.assertEqual(p._engine.tasks[tid]['billing']['downloadvolumefactor'], 0)
        self.assertTrue(p._engine.tasks[tid]['billing']['accepted'])


class CleanupFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / ('a' * 40)
        self.root.mkdir()
        (self.root / 'seed.partial').mkdir()
        self.path = self.root / 'seed.partial' / 'a.mkv'
        self.path.write_bytes(b'video')
        self.tid = 'a' * 40
        self.saved = []
        self.e = core.Engine({}, lambda x: self.saved.append(copy.deepcopy(x)), lambda: 10**9, low_water=0)
        self.t = self.e.admit(self.tid, 'seed.partial', 5, str(self.root), 'physical', str(self.root), 'Original')
        self.t.update(state='waiting_library', commit_intent=True, target='/115/watch/seed.partial',
                      qb_identity=123, qb_files=[['seed.partial/a.mkv', 5, 1]],
                      billing=dict(site=3, downloadvolumefactor=0, accepted=True),
                      manifest=[dict(source='seed.partial/a.mkv', remote='a.mkv', size=5,
                                     sha1=hashlib.sha1(b'video').hexdigest(),
                                     signature=core.file_signature(self.path.stat()))])
        self.qb = FakeQb(str(self.root), [dict(name='seed.partial/a.mkv', size=5, progress=1, priority=1)])
        self.absent = False
        self.removes = []
        snapshot = self.qb.snapshot
        self.qb.snapshot = lambda tid: dict(snapshot(tid), added_on=123, hash=tid)
        self.qb.cleanup_inventory = lambda: ([] if self.absent else [dict(hash=self.tid, save_path=str(self.root), content_path=str(self.root / 'seed.partial'), added_on=123)])
        self.qb.is_absent = lambda tid: self.absent
        def remove(tid, expected):
            self.removes.append((tid, expected['qb_identity']))
            self.absent = True
            return True
        self.qb.remove_only = remove
        self.verifies = []
        self.cloud = SimpleNamespace(verify=lambda path, item: (self.verifies.append(path) or dict(sha1=item['sha1'])))

    def preview(self):
        return self.e.cleanup_preview(self.tid, self.qb, self.cloud, enabled=True)


class CleanupHappyTests(CleanupFixture):
    def test_free_cleanup_verifies_cloud_removes_only_hash_and_unlinks_only_manifest(self):
        self.assertTrue(callable(getattr(self.e, 'cleanup_free', None)), 'cleanup state machine missing')
        extra = self.root / 'seed.partial' / 'unselected.mkv'
        extra.write_bytes(b'piece-boundary')
        self.assertTrue(self.preview()['eligible'])
        self.assertTrue(self.path.exists(), 'preview must not delete')
        self.assertEqual(self.removes, [])
        self.assertTrue(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))
        self.assertEqual(self.removes, [(self.tid, 123)])
        self.assertFalse(self.path.exists())
        self.assertTrue(extra.exists(), 'unverified and unselected files must remain')
        self.assertEqual(self.t['cleanup_state'], 'done')
        self.assertTrue(self.t.get('library_confirmed'), 'successful cleanup must publish its fresh cloud proof even without a prior reconcile')
        phases = [x[self.tid].get('cleanup_state') for x in self.saved]
        self.assertLess(phases.index('remove_intent'), phases.index('qb_absent'))
        self.assertIn('/115/watch/seed.partial/a.mkv', self.verifies)
        self.assertTrue(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))
        self.assertEqual(len(self.removes), 1)


class CleanupGuardTests(CleanupFixture):
    def test_other_qb_save_directory_overlap_blocks_even_unrelated_unlabelled_hash(self):
        self.qb.cleanup_inventory = lambda: [dict(hash='b' * 40, save_path=str(self.root.parent), content_path='', added_on=321)]
        self.assertFalse(self.preview()['eligible'], 'all qB tasks must participate in exclusivity check')
        self.assertFalse(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))
        self.assertEqual(self.removes, [])
        self.assertTrue(self.path.exists())

    def test_legacy_ledger_root_overlap_blocks_even_untracked_other_instance(self):
        self.e.tasks['b' * 40] = dict(hash='b' * 40, root=str(self.root.parent), name=self.tid, state='untracked', physical_downloader='other')
        self.assertFalse(self.preview()['eligible'], 'all legacy ledger reservations must participate')

    def test_complete_qb_file_selection_binding_is_required(self):
        self.qb.files.append(dict(name='seed.partial/other.mkv', size=99, priority=0, progress=0))
        self.assertFalse(self.preview()['eligible'], 'full torrent file list changed')

    def test_malformed_manifest_cannot_redirect_remote_verification(self):
        self.t['manifest'][0]['remote'] = '../other.mkv'
        self.assertFalse(self.preview()['eligible'])
        self.assertEqual(self.verifies, [])


    def test_concurrent_replacement_before_isolation_is_never_unlinked(self):
        rename = os.rename
        calls = []
        def race(src, dst, *args, **kw):
            calls.append((src, dst))
            rename(self.path, self.path.with_suffix('.original'))
            self.path.write_bytes(b'other')
            return rename(src, dst, *args, **kw)
        with patch.object(core.os, 'rename', side_effect=race):
            self.assertFalse(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True), 'replacement must be isolated then retained, never deleted')
        self.assertEqual(len(calls), 1)
        retained = [p for p in self.root.rglob('*') if p.is_file() and p.read_bytes() == b'other']
        self.assertEqual(len(retained), 1, 'replacement data must remain in private isolation slot')
        self.assertEqual(self.t['cleanup_reason'], 'isolated_file_changed')


class CleanupRecoveryTests(CleanupFixture):
    def test_crash_after_atomic_rename_recovers_slot_without_source(self):
        rename = os.rename
        def crash(*args, **kw):
            rename(*args, **kw)
            raise SystemExit('power loss after rename')
        with patch.object(core.os, 'rename', side_effect=crash), self.assertRaises(SystemExit):
            self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True)
        self.assertFalse(self.path.exists())
        restored = core.Engine(self.saved[-1], lambda x: None, lambda: 10**9, low_water=0)
        self.assertTrue(restored.cleanup_free(self.tid, self.qb, self.cloud, enabled=True), 'WAL slot must resume after rename')
        self.assertEqual(restored.tasks[self.tid]['cleanup_state'], 'done')
        self.assertEqual(len(self.removes), 1)
        self.assertEqual([p for p in self.root.rglob('*') if p.is_file()], [])

    def test_crash_after_qb_remove_recovers_without_second_remove(self):
        remove = self.qb.remove_only
        def crash(tid, identity):
            remove(tid, identity)
            raise SystemExit('power loss')
        self.qb.remove_only = crash
        with self.assertRaises(SystemExit):
            self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True)
        self.assertEqual(self.saved[-1][self.tid]['cleanup_state'], 'remove_intent')
        restored = core.Engine(self.saved[-1], lambda x: None, lambda: 10**9, low_water=0)
        self.assertTrue(restored.cleanup_free(self.tid, self.qb, self.cloud, enabled=True), 'absent qB must resume local-only journal')
        self.assertEqual(len(self.removes), 1)
        self.assertFalse(self.path.exists())


class CleanupAdapterTests(unittest.TestCase):
    def test_native_delete_is_exact_hash_no_files_readback_and_generation_checked(self):
        from test_adapter import Native, adapter
        n = Native()
        tid = 'a' * 40
        n.entries = [dict(hash=tid, name='seed.partial', tags='mp-qb115', save_path='/qb/isolated', added_on=123)]
        deletes = []
        def delete(**kw):
            deletes.append(kw)
            n.entries = []
            return True
        n.delete_torrents = delete
        a = adapter.QbAdapter(n, 'physical', '/mp/isolated', lambda: 'MOVIEPILOT')
        self.assertTrue(callable(getattr(a, 'remove_only', None)), 'safe qB removal adapter missing')
        expected = dict(hash=tid, qb_identity=123, physical_downloader='physical', root='/mp/isolated',
                        save_path='/qb/isolated', layout='Original', name='seed.partial',
                        qb_files=[['seed.partial/a.mkv', 4, 1]],
                        manifest=[dict(source='seed.partial/a.mkv', size=4)], cleanup_tags=['mp-qb115'])
        self.assertFalse(a.remove_only(tid, dict(expected, qb_identity=124)))
        self.assertEqual(deletes, [])
        self.assertEqual(a.snapshot(tid)['added_on'], 123)
        self.assertEqual(a.cleanup_inventory()[0]['save_path'], '/mp/isolated')
        self.assertTrue(a.remove_only(tid, expected))
        self.assertEqual(deletes, [dict(ids=tid, delete_file=False)])
        self.assertTrue(a.is_absent(tid))
        n.get_torrents = lambda **kw: ([], True)
        with self.assertRaises(ValueError):
            a.is_absent(tid)


if __name__ == '__main__':
    unittest.main()
