import copy
import unittest
from test_cleanup import CleanupFixture
from test_core import bencode
from test_plugin import load_plugin


class PluginCleanupFlow(CleanupFixture):
    def plugin(self, enabled=True):
        p = load_plugin().Qb115Downloader()
        p.init_plugin({'auto_cleanup_free': enabled})
        p._enabled = True
        p._engine = self.e
        p._qb = self.qb
        p._runtime = lambda: (self.qb, self.cloud)
        p._resolve_final = lambda task, item: '/115/library/a.mkv'
        return p

    def test_preview_is_read_only_with_absolute_paths_sizes_and_poll_runs_cleanup(self):
        p = self.plugin()
        saved = copy.deepcopy(self.e.tasks)
        row = p.cleanup_preview(self.tid)
        self.assertTrue(row['eligible'])
        self.assertEqual(row['files'], [dict(path=str(self.path), size=5)])
        self.assertEqual(self.e.tasks, saved)
        self.assertEqual(self.removes, [])
        p.poll()
        self.assertEqual(self.t['cleanup_state'], 'done')
        self.assertFalse(self.path.exists())
        status = p.tasks_status()['tasks'][0]
        self.assertTrue(status['library_confirmed'])
        self.assertEqual(status['cleanup'], 'done')
        self.assertEqual(status['qb_state'], 'absent')
        self.assertEqual(p.cleanup_preview(self.tid)['reason'], 'already_cleaned')
        self.assertIn('/cleanup_preview', [r['path'] for r in p.get_api()])

    def test_default_disabled_and_non_bool_config_cannot_enable_deletion(self):
        p = self.plugin(False)
        self.assertFalse(p.get_form()[1]['auto_cleanup_free'])
        p.poll()
        self.assertEqual(self.removes, [])
        for value in ('false', 'true', 1, None):
            _, errors = p._validate_config({'auto_cleanup_free': value})
            self.assertTrue(errors)

    def test_completed_or_externally_missing_hash_can_never_be_resubmitted(self):
        raw = bencode({b'info': {b'name': b'a.mkv', b'length': 4, b'pieces': b'x'*20, b'piece length': 16384}})
        from test_core import core
        tid = core.torrent_metadata(raw)[0]
        p = self.plugin()
        task = self.e.tasks.pop(self.tid)
        task['hash'] = tid
        self.e.tasks[tid] = task
        for state, cleanup in [('external_missing', None), ('waiting_library', 'done')]:
            task.update(state=state, cleanup_state=cleanup)
            self.qb.snapshot = lambda *a: self.fail('terminal task must never query/submit/start qB')
            self.assertIsNone(p.download(content=raw, downloader=p.DOWNLOADER_NAME)[1])


if __name__ == '__main__':
    unittest.main()
