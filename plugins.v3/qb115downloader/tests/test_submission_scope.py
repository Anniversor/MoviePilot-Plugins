"""Exercise a synthetic caller with the exact live MP module/method boundary."""
import copy
import unittest
from types import SimpleNamespace
from test_core import bencode, FakeQb
from test_plugin import load_plugin


def submit_via_mp(plugin, context, content):
    scope = {'__name__': 'app.chain.download.submission', 'plugin': plugin,
             'SimpleNamespace': SimpleNamespace}
    exec(compile('def _submit_prepared_download(context, content):\n'
                 '    prepared = SimpleNamespace(torrent_content=content, torrent=context["torrent_info"])\n'
                 '    return plugin.download(content=content, downloader=plugin.DOWNLOADER_NAME)\n',
                 '<mp-contract-caller>', 'exec'), scope)
    return scope['_submit_prepared_download'](context, content)


class SubmissionScope(unittest.TestCase):
    def test_direct_calls_cannot_consume_stale_resource_event_and_live_submission_freezes_factor(self):
        p = load_plugin().Qb115Downloader()
        p.init_plugin({})
        p._enabled = True
        p._engine.free = lambda: 10**12
        qb = FakeQb('/local', [dict(name='a.mkv', size=4, priority=1, progress=0)])
        snap = qb.snapshot
        qb.snapshot = lambda tid: dict(snap(tid), added_on=123)
        qb.submit = lambda content, tid, **kw: ('physical', tid, 'Original', 'ok')
        p._runtime = lambda: (qb, None)
        context = {'torrent_info': {'site': 3, 'enclosure': 'https://site/id=1', 'downloadvolumefactor': 0}}
        raw = bencode({b'info': {b'name': b'a.mkv', b'length': 4, b'pieces': b'x'*20, b'piece length': 16384}})
        if hasattr(p, 'on_resource_download'):
            p.on_resource_download(SimpleNamespace(event_data={'downloader': p.DOWNLOADER_NAME, 'context': context}))
        tid = p.download(content=raw, downloader=p.DOWNLOADER_NAME)[1]
        self.assertFalse(p._engine.tasks[tid].get('billing'), 'an aborted ResourceDownload must not leak into a direct call')
        p._engine.tasks.clear()
        tid = submit_via_mp(p, context, raw)[1]
        self.assertEqual(p._engine.tasks[tid].get('qb_identity'), 123)
        self.assertEqual(p._engine.tasks[tid].get('qb_files'), [['a.mkv', 4, 1]])
        self.assertEqual(p._engine.tasks[tid]['billing']['downloadvolumefactor'], 0)
        self.assertFalse(p._engine.tasks[tid]['billing']['accepted'])
        context['torrent_info']['downloadvolumefactor'] = 1
        wrong = copy.deepcopy(context)
        wrong['torrent_info']['enclosure'] += '0'
        p.on_download_added(SimpleNamespace(event_data={'downloader': p.DOWNLOADER_NAME, 'hash': tid, 'context': wrong}))
        self.assertFalse(p._engine.tasks[tid]['billing']['accepted'])
        p.on_download_added(SimpleNamespace(event_data={'downloader': p.DOWNLOADER_NAME, 'hash': tid, 'context': context}))
        self.assertEqual(p._engine.tasks[tid]['billing']['downloadvolumefactor'], 0)
        self.assertTrue(p._engine.tasks[tid]['billing']['accepted'])
