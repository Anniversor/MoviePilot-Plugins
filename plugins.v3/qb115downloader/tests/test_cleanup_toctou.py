"""Observable cleanup boundaries: real local files, Engine and fake qB peers."""
import copy
from unittest.mock import patch

from test_cleanup import CleanupFixture
from test_core import core
from test_adapter import Native, adapter


class NativePeer(Native):
    def __init__(self, fixture):
        super().__init__()
        self.entries = [dict(hash=fixture.tid, name='seed.partial', tags='mp-qb115',
                             save_path=str(fixture.root), content_path=str(fixture.path.parent), added_on=123)]
        self.files = copy.deepcopy(fixture.qb.files)
        self.files.append(dict(name='seed.partial/unselected.mkv', size=99, priority=0, progress=0))
        self.deletes = []

    def normalize_path(self, path, name):
        return str(path)

    def normalize_return_path(self, path, name):
        return str(path)

    def get_files(self, tid):
        return copy.deepcopy(self.files)

    def get_torrents(self, ids=None):
        ids = [ids] if isinstance(ids, str) else ids
        return copy.deepcopy([e for e in self.entries if ids is None or e['hash'] in ids]), False

    def delete_torrents(self, ids, delete_file):
        self.deletes.append(dict(ids=ids, delete_file=delete_file))
        self.entries = [e for e in self.entries if e['hash'] != ids]
        return True


class BindingBoundaryTests(CleanupFixture):
    def setUp(self):
        super().setUp()
        self.native = NativePeer(self)
        self.t['qb_files'] = self.e.qb_file_binding(dict(files=self.native.files))
        self.qb = adapter.QbAdapter(self.native, 'physical', str(self.root), lambda: 'MOVIEPILOT')

    def mutate(self, field):
        if field == 'priority':
            self.native.files[1]['priority'] = 1
        elif field == 'file':
            self.native.files[1]['name'] = 'seed.partial/changed.mkv'
        elif field == 'incomplete':
            self.native.files[0]['progress'] = .5
        elif field == 'layout':
            self.native.get_content_layout = lambda: 'NoSubfolder'
        elif field == 'physical':
            self.qb.name = 'other'
        elif field == 'root':
            self.qb.local_root = lambda tid: str(self.root.parent)
            self.native.entries[0]['save_path'] = str(self.root.parent)
        elif field == 'network':
            self.native.get_files = lambda tid: (_ for _ in ()).throw(ValueError('network'))
        else:
            self.native.entries[0][field] = {'name': 'other', 'hash': 'b' * 40, 'added_on': 124,
                                           'tags': 'mp-qb115,manual', 'save_path': str(self.root.parent)}[field]

    def during_verify(self, field):
        def verify(path, item):
            self.mutate(field)
            return dict(sha1=item['sha1'])
        self.cloud.verify = verify
        self.assertFalse(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))
        self.assertEqual(self.native.deletes, [])
        self.assertTrue(self.path.exists())
        self.assertNotIn('cleanup_state', self.t, 'observable drift must be rejected BEFORE remove WAL')

    def during_remove_snapshot(self, field):
        remove = self.qb.remove_only
        def changed(*args):
            self.mutate(field)
            return remove(*args)
        self.qb.remove_only = changed
        self.assertFalse(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))
        self.assertEqual(self.native.deletes, [])
        self.assertTrue(self.path.exists())
        self.assertEqual(self.t['cleanup_state'], 'remove_intent')
        self.qb.remove_only = lambda *a: self.fail('never replay an uncertain RPC')
        self.assertFalse(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))

    def test_selection_change_during_verify_is_rejected_before_wal(self):
        self.during_verify('priority')

    def test_file_change_during_verify_is_rejected_before_wal(self):
        self.during_verify('file')

    def test_incomplete_selection_during_verify_is_rejected_before_wal(self):
        self.during_verify('incomplete')

    def test_labels_change_during_verify_is_rejected_before_wal(self):
        self.during_verify('tags')

    def test_selection_change_during_adapter_snapshot_is_rejected(self):
        self.during_remove_snapshot('priority')

    def test_file_change_during_adapter_snapshot_is_rejected(self):
        self.during_remove_snapshot('file')

    def test_incomplete_selection_during_adapter_snapshot_is_rejected(self):
        self.during_remove_snapshot('incomplete')

    def test_labels_change_during_adapter_snapshot_is_rejected(self):
        self.during_remove_snapshot('tags')

    def test_other_binding_drift_during_long_io_is_rejected_before_wal(self):
        for field in ('name', 'layout', 'physical', 'root', 'save_path', 'added_on', 'hash', 'network'):
            with self.subTest(field=field):
                fixture = BindingBoundaryTests()
                fixture.setUp()
                try:
                    fixture.during_verify(field)
                finally:
                    fixture.doCleanups()

    def test_other_binding_drift_at_adapter_snapshot_is_rejected(self):
        for field in ('name', 'layout', 'physical', 'root', 'save_path', 'added_on', 'hash', 'network'):
            with self.subTest(field=field):
                fixture = BindingBoundaryTests()
                fixture.setUp()
                try:
                    fixture.during_remove_snapshot(field)
                finally:
                    fixture.doCleanups()


    def test_selection_change_during_local_hash_is_rejected_before_wal(self):
        hash_source = self.e._hash_source
        def hashed(task, item):
            hash_source(task, item)
            self.mutate('priority')
        self.e._hash_source = hashed
        self.assertFalse(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))
        self.assertEqual(self.native.deletes, [])
        self.assertNotIn('cleanup_state', self.t)
        self.assertTrue(self.path.exists())


class ExclusiveBoundaryTests(CleanupFixture):
    def race(self, peer, boundary):
        inventory = self.qb.cleanup_inventory
        other = dict(hash='b' * 40, root=str(self.root), name=self.t['name'],
                     save_path=str(self.root), content_path=str(self.path.parent), added_on=456)
        def arrive():
            if peer == 'ledger':
                self.e.tasks[other['hash']] = copy.deepcopy(other)
            elif peer == 'qb':
                self.qb.cleanup_inventory = lambda: inventory() + [copy.deepcopy(other)]
            else:
                def unavailable():
                    raise ValueError('network')
                self.qb.cleanup_inventory = unavailable
        if boundary == 'verify':
            def verify(path, item):
                arrive()
                return dict(sha1=item['sha1'])
            self.cloud.verify = verify
        elif boundary == 'hash':
            hash_source = self.e._hash_source
            def hashed(task, item):
                hash_source(task, item)
                arrive()
            self.e._hash_source = hashed
        elif boundary == 'removed':
            remove = self.qb.remove_only
            def removed(*args):
                result = remove(*args)
                arrive()
                return result
            self.qb.remove_only = removed
        rename = core.os.rename
        def renamed(*args, **kwargs):
            result = rename(*args, **kwargs)
            if boundary == 'renamed':
                arrive()
                # Another writer now owns the public path; never restore over it.
                self.path.write_bytes(b'other')
            return result
        with patch.object(core.os, 'rename', side_effect=renamed):
            self.assertFalse(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))
        self.assertNotEqual(self.t.get('cleanup_state'), 'done')
        self.assertEqual(self.t['cleanup_reason'], {'ledger': 'ledger_content_overlap',
                         'qb': 'qb_content_overlap', 'network': 'verification_failed'}[peer])
        if boundary in {'verify', 'hash'}:
            self.assertEqual(self.removes, [])
            self.assertNotIn('cleanup_state', self.t, 'no irreversible remove WAL before fresh checks')
        else:
            self.assertEqual(len(self.removes), 1)
        if boundary == 'renamed':
            self.assertEqual(self.path.read_bytes(), b'other')
            entry = self.t['cleanup_files'][self.t['manifest'][0]['source']]
            self.assertEqual((self.path.parent / entry['slot'] / self.path.name).read_bytes(), b'video')
        else:
            self.assertEqual(self.path.read_bytes(), b'video')
        if peer == 'ledger':
            self.assertEqual(self.e.tasks[other['hash']], other)

    def test_new_ledger_overlap_during_verify(self):
        self.race('ledger', 'verify')

    def test_new_qb_overlap_during_verify(self):
        self.race('qb', 'verify')

    def test_new_ledger_overlap_during_local_hash(self):
        self.race('ledger', 'hash')

    def test_new_qb_overlap_during_local_hash(self):
        self.race('qb', 'hash')

    def test_new_ledger_overlap_after_qb_remove_before_rename(self):
        self.race('ledger', 'removed')

    def test_new_qb_overlap_after_qb_remove_before_rename(self):
        self.race('qb', 'removed')

    def test_new_ledger_overlap_after_rename_before_unlink(self):
        self.race('ledger', 'renamed')

    def test_new_qb_overlap_after_rename_before_unlink(self):
        self.race('qb', 'renamed')

    def test_inventory_failure_during_verify_preserves_source(self):
        self.race('network', 'verify')

    def test_inventory_failure_after_remove_preserves_source(self):
        self.race('network', 'removed')

    def test_inventory_failure_after_rename_preserves_slot(self):
        self.race('network', 'renamed')

    def test_ledger_arrival_during_final_inventory_is_not_hidden_by_old_copy(self):
        inventory = self.qb.cleanup_inventory
        rename = core.os.rename
        def renamed(*args, **kwargs):
            result = rename(*args, **kwargs)
            def queried():
                result = inventory()
                self.e.tasks['b' * 40] = dict(hash='b' * 40, root=str(self.root), name=self.t['name'])
                return result
            self.qb.cleanup_inventory = queried
            return result
        with patch.object(core.os, 'rename', side_effect=renamed):
            self.assertFalse(self.e.cleanup_free(self.tid, self.qb, self.cloud, enabled=True))
        self.assertEqual(self.t['cleanup_reason'], 'ledger_content_overlap')
        entry = self.t['cleanup_files'][self.t['manifest'][0]['source']]
        self.assertEqual((self.path.parent / entry['slot'] / self.path.name).read_bytes(), b'video')
