"""Cancellation must win after blocking inventory, before local destructive syscalls."""
import threading
from test_cleanup import CleanupFixture

class CancelBoundaryTests(CleanupFixture):
    def check_cancel(self, boundary, action):
        entered, release = threading.Event(), threading.Event()
        inventory = self.qb.cleanup_inventory
        result = {}
        def blocked():
            rows = inventory()
            journal = self.t.get('cleanup_files') or {}
            phases = [entry['phase'] for entry in journal.values()]
            at_boundary = ('unlink_intent' in phases) if boundary == 'unlink' else (bool(phases) and 'unlink_intent' not in phases)
            if at_boundary:
                entered.set()
                if not release.wait(5):
                    raise ValueError('test barrier timed out')
            return rows
        self.qb.cleanup_inventory = blocked
        thread = threading.Thread(target=lambda: result.update(ok=self.e.cleanup_free(self.tid,self.qb,self.cloud,enabled=True)))
        thread.start()
        try:
            self.assertTrue(entered.wait(5), 'destructive boundary was not reached')
            if action == 'pause':
                self.assertTrue(self.e.control(self.tid,'pause',self.qb))
            else:
                self.e.stopped.set()
            release.set()
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertFalse(result['ok'], 'successful cancellation cannot be followed by deletion')
            self.assertNotEqual(self.t.get('cleanup_state'),'done')
            remaining=[p for p in self.root.rglob('*') if p.is_file()]
            self.assertEqual(len(remaining),1)
            self.assertEqual(remaining[0].read_bytes(),b'video')
            if boundary == 'rename': self.assertTrue(self.path.exists(), 'pause must also stop the rename')
        finally:
            release.set();thread.join(5)

    def test_pause_during_final_inventory_preserves_isolated_file(self):
        self.check_cancel('unlink','pause')

    def test_pause_during_rename_inventory_preserves_public_file(self):
        self.check_cancel('rename','pause')

    def test_stop_during_final_inventory_preserves_isolated_file(self):
        self.check_cancel('unlink','stop')

    def test_stop_during_rename_inventory_preserves_public_file(self):
        self.check_cancel('rename','stop')
