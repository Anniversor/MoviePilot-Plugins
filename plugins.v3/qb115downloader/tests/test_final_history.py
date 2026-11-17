import unittest
from types import SimpleNamespace
from test_core import core


class FinalHistory(unittest.TestCase):
    def test_exact_source_size_and_storage_mapping_without_download_hash(self):
        from history import resolve_history
        task = dict(hash='a'*40, target='/115/watch/seed.partial')
        item = dict(remote='a.mkv', size=5)
        calls = []
        row = SimpleNamespace(src='/115_rclone/watch/seed.partial/a.mkv', src_storage='local',
                              dest='/115_rclone/library/renamed.mkv', dest_storage='local', status=True,
                              src_fileitem={'size': 5}, dest_fileitem={'size': 5}, download_hash=None, downloader=None)
        def lookup(src, storage):
            calls.append((src, storage))
            return row
        self.assertEqual(resolve_history(task, item, lookup, '/115_rclone', '/115'), '/115/library/renamed.mkv')
        self.assertEqual(calls, [(row.src, 'local')])
        for field, value in [('src', row.src+'0'), ('src_storage', '115'), ('status', False),
                             ('dest', '/local/library/a.mkv'), ('src_fileitem', {'size': 6}),
                             ('dest_fileitem', {'size': 6})]:
            old = getattr(row, field)
            setattr(row, field, value)
            with self.subTest(field=field), self.assertRaises(ValueError):
                resolve_history(task, item, lookup, '/115_rclone', '/115')
            setattr(row, field, old)
