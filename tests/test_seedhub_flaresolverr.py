import base64
import importlib
import importlib.util
import json
import sys
import threading
import time
import unittest
from collections.abc import Callable
from contextlib import contextmanager
from enum import Enum
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock


PLUGIN = Path(__file__).parents[1] / 'plugins.v3/seedhubindexer/__init__.py'
TARGET = 'https://sidhub.cc/s/test/'


class MediaType(Enum):
    TV = 'TV'
    MOVIE = 'MOVIE'


class ImportIsolation(unittest.TestCase):
    def test_lazy_runtime_dependency_is_shared_with_test_runner(self):
        module = load_plugin()
        self.assertIs(module.requests, importlib.import_module('requests'))
        self.assertIs(module.Timeout, importlib.import_module('urllib3.util').Timeout)


@contextmanager
def injected_modules(stubs: dict[str, ModuleType]):
    # Restore only the stubs; patch.dict would also evict dependencies imported
    # during the test, leaving requests and urllib3 with mismatched classes.
    absent = object()
    previous = {name: sys.modules.get(name, absent) for name in stubs}
    sys.modules.update(stubs)
    try:
        yield
    finally:
        for name, original in previous.items():
            if original is absent:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = cast(ModuleType, original)


def load_plugin():
    stubs = {
        'apscheduler.triggers.cron': {'CronTrigger': object},
        'app.core.config': {'settings': SimpleNamespace(PROXY=None)},
        'app.helper.sites': {'SitesHelper': object},
        'app.log': {'logger': Mock()},
        'app.plugins': {'_PluginBase': object},
        'app.schemas.types': {'MediaType': MediaType},
        'app.core.context': {'TorrentInfo': SimpleNamespace},
    }
    modules = {}
    for name, attrs in stubs.items():
        modules[name] = ModuleType(name)
        vars(modules[name]).update(attrs)
    spec = importlib.util.spec_from_file_location('seedhub_under_test', PLUGIN)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    with injected_modules(modules):
        spec.loader.exec_module(module)
    return module


class SolverServer(ThreadingHTTPServer):
    calls: list[tuple[str, dict]]
    response: Callable[[dict], Any]


class SolverHTTP(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_POST(self):
        data = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        server = cast(SolverServer, self.server)
        server.calls.append((self.path, data))
        body = json.dumps(server.response(data)).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class SeedHubTransport(unittest.TestCase):
    def setUp(self):
        self.server = SolverServer(('127.0.0.1', 0), SolverHTTP)
        self.server.calls = []
        self.server.response = lambda data: {
            'status': 'ok',
            'solution': {'status': 200, 'url': data['url'], 'response': '<title>SeedHub</title>'},
        }
        thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
        thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(self.server.shutdown)
        self.url = f'http://127.0.0.1:{self.server.server_port}'
        self.module = load_plugin()
        self.plugin = self.module.SeedHubIndexer()
        self.plugin.get_data = lambda _key: None
        self.direct = Mock(return_value=SimpleNamespace(status_code=403, headers={}, text='blocked'))
        stub = ModuleType('app.utils.http')
        setattr(stub, 'RequestUtils', lambda **_kwargs: SimpleNamespace(get_res=self.direct))
        self.enterContext(injected_modules({stub.__name__: stub}))

    def request(self, url=TARGET, **kwargs):
        return self.plugin._SeedHubIndexer__request(url, retries=0, **kwargs)

    def test_403_falls_back_and_returns_real_solver_body(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url, 'flaresolverr_timeout': 22})
        response = self.request()
        self.assertEqual((response.status_code, response.text), (200, '<title>SeedHub</title>'))
        self.assertEqual(self.server.calls, [('/v1', {'cmd': 'request.get', 'url': TARGET, 'maxTimeout': 22000})])
        self.assertEqual(self.direct.call_count, 1)

    def test_empty_solver_configuration_preserves_direct_access(self):
        self.plugin.init_plugin({})
        self.assertIs(self.request(), self.direct.return_value)
        self.assertEqual(self.server.calls, [])

    def test_clean_direct_response_does_not_launch_a_browser(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        self.direct.return_value = SimpleNamespace(status_code=200, headers={}, text='<title>SeedHub</title>')
        self.assertIs(self.request(), self.direct.return_value)
        self.assertEqual(self.server.calls, [])

    def test_direct_same_origin_redirect_is_followed_without_solver(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        body = SimpleNamespace(status_code=200, headers={}, text='<title>SeedHub</title>')
        self.direct.side_effect = [SimpleNamespace(status_code=302, headers={'Location': '/s/normalized/'}, text=''), body]
        self.assertIs(self.request(), body)
        self.assertEqual([call.args[0] for call in self.direct.call_args_list], [TARGET, 'https://sidhub.cc/s/normalized/'])
        self.assertEqual(self.server.calls, [])

    def test_direct_redirect_to_another_origin_is_not_contacted(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        self.direct.return_value = SimpleNamespace(status_code=302,
                                                   headers={'Location': '//example.invalid/'}, text='')
        self.assertIsNone(self.request())
        self.assertEqual(self.direct.call_count, 1)
        self.assertEqual(self.server.calls, [])

    def test_challenge_header_triggers_fallback_even_on_503(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        self.direct.return_value = SimpleNamespace(status_code=503, headers={'CF-Mitigated': 'challenge'}, text='blocked')
        self.assertEqual(self.request().text, '<title>SeedHub</title>')

    def test_challenge_body_is_not_a_success_even_on_200(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        self.direct.return_value = SimpleNamespace(status_code=200, headers={}, text='<title>Just a moment...</title>')
        self.assertEqual(self.request().text, '<title>SeedHub</title>')

    def test_cf_script_on_a_real_seedhub_page_is_not_a_challenge(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        page = ('<title>SeedHub</title><a href="/movies/1/">Example</a>'
                '<script src="/cdn-cgi/challenge-platform/scripts/jsd/main.js"></script>')
        self.server.response = lambda req: {'status': 'ok', 'solution': {
            'status': 200, 'url': req['url'], 'response': page,
        }}
        self.assertEqual(self.request().text, page)

    def test_solver_error_never_leaks_payload_to_logs(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        self.server.response = lambda _data: {'status': 'error', 'message': 'test-private-response'}
        self.assertIsNone(self.request())
        self.assertTrue(self.module.logger.warn.called)
        self.assertNotIn('test-private-response', str(self.module.logger.mock_calls))

    def test_unreachable_solver_is_contained(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url, 'flaresolverr_timeout': 3})
        self.server.server_close()
        self.assertIsNone(self.request())

    def test_solver_does_not_receive_other_origins(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        self.assertIsNone(self.request('https://sidhub.cc.example.invalid/s/test/'))
        self.direct.assert_not_called()
        self.assertEqual(self.server.calls, [])

    def test_solver_drops_unexpected_final_origin(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        self.server.response = lambda _data: {'status': 'ok', 'solution': {
            'status': 200, 'url': 'https://example.invalid/', 'response': '<title>wrong site</title>',
        }}
        self.assertIsNone(self.request())

    def test_expired_deadline_does_not_send_a_request(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        self.assertIsNone(self.request(deadline=time.monotonic() - 1))
        self.direct.assert_not_called()
        self.assertEqual(self.server.calls, [])

    def test_solver_timeout_is_clamped_to_the_remaining_search_budget(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        self.assertEqual(self.request(deadline=time.monotonic() + 6).status_code, 200)
        self.assertGreater(self.server.calls[0][1]['maxTimeout'], 3000)
        self.assertLessEqual(self.server.calls[0][1]['maxTimeout'], 6000)

    def test_deadline_limits_waiting_for_a_solver_slot(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        slot = threading.BoundedSemaphore(1)
        slot.acquire()
        setattr(self.module, '_SOLVER_SLOTS', slot)
        start = time.monotonic()
        self.assertIsNone(self.request(deadline=start + .1))
        self.assertLess(time.monotonic() - start, 1)
        self.assertEqual(self.server.calls, [])

    def test_resolve_budget_caps_new_requests_but_keeps_cached_rows(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url, 'resolve_budget': 1})
        stamp = 'magnet:?xt=urn:btih:' + 'a' * 40
        self.plugin._magnet_cache['1'] = stamp
        self.plugin.save_data = Mock()
        data64 = base64.b64encode(stamp.encode()).decode()
        self.server.response = lambda data: {'status': 'ok', 'solution': {
            'status': 200, 'url': data['url'], 'response': f'<script>const data = "{data64}"</script>',
        }}
        rows = [{'seed_id': str(n)} for n in range(1, 8)]
        resolved, hits, fetched, skipped = self.plugin._SeedHubIndexer__resolve_rows(rows, deadline=time.monotonic() + 6)
        self.assertEqual((len(resolved), hits, fetched, skipped), (2, 1, 1, 5))
        self.assertEqual(len(self.server.calls), 1)
        self.plugin.save_data.assert_called_once()

    def test_solver_mode_keeps_existing_domain_on_failed_probe(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        self.server.response = lambda _data: {'status': 'error'}
        self.plugin.update_config = Mock()
        self.plugin._SeedHubIndexer__domain_check()
        self.plugin.update_config.assert_not_called()
        self.assertEqual(self.direct.call_count, 1)
        self.assertEqual(len(self.server.calls), 1)
        self.assertNotIn('尝试切换镜像', str(self.module.logger.mock_calls))

    def test_slow_discovery_does_not_hold_search_past_deadline(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        self.plugin._enabled = True
        setattr(self.module, '_SOLVER_DISCOVERY_SECONDS', .1)
        setattr(self.module, '_SOLVER_SEARCH_SECONDS', .2)
        entered = threading.Event()
        release = threading.Event()

        def slow_direct(*args, **kwargs):
            entered.set()
            release.wait(.6)
            return SimpleNamespace(status_code=200, headers={}, text='<title>SeedHub</title>')

        self.direct.side_effect = slow_direct
        start = time.monotonic()
        try:
            self.assertEqual(self.plugin.search_torrents(site={'id': 8, 'domain': 'sidhub.cc'}, keyword='slow'), [])
            self.assertTrue(entered.is_set())
            self.assertLess(time.monotonic() - start, .4)
        finally:
            release.set()

    def test_magnet_that_finishes_after_search_deadline_is_not_cached(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        release = threading.Event()
        completed = threading.Event()
        self.plugin.save_data = Mock()
        resolve = self.plugin._SeedHubIndexer__resolve_magnet
        stamp = 'magnet:?xt=urn:btih:' + 'b' * 40

        def slow_direct(*args, **kwargs):
            release.wait(1)
            data64 = base64.b64encode(stamp.encode()).decode()
            return SimpleNamespace(status_code=200, headers={}, text=f'<script>const data = "{data64}"</script>')

        def resolve_with_signal(*args, **kwargs):
            try:
                return resolve(*args, **kwargs)
            finally:
                completed.set()

        self.direct.side_effect = slow_direct
        self.plugin._SeedHubIndexer__resolve_magnet = resolve_with_signal
        try:
            results, *_ = self.plugin._SeedHubIndexer__resolve_rows([{'seed_id': '1'}], deadline=time.monotonic() + .1)
            self.assertEqual(results, [])
        finally:
            release.set()
        self.assertTrue(completed.wait(1))
        self.assertEqual(self.plugin._magnet_cache, {})
        self.plugin.save_data.assert_not_called()

    def test_discovery_timeout_keeps_rows_from_completed_details(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        self.plugin._enabled = True
        setattr(self.module, '_SOLVER_DISCOVERY_SECONDS', .15)
        setattr(self.module, '_SOLVER_SEARCH_SECONDS', .35)
        release = threading.Event()
        late_detail = threading.Event()
        stamp = 'magnet:?xt=urn:btih:' + 'a' * 40
        self.plugin._magnet_cache['1'] = stamp

        def direct(url, **kwargs):
            if '/s/' in url:
                text = '<a href="/movies/11/">First</a><a href="/movies/22/">Second</a>'
            elif url.endswith('/11/'):
                text = '<ul class="seeds"><li><a href="/link_start/?seed_id=1" title="First">First</a></li></ul>'
            else:
                late_detail.set()
                release.wait(.6)
                text = ''
            return SimpleNamespace(status_code=200, headers={}, text=text)

        self.direct.side_effect = direct
        try:
            rows = self.plugin.search_torrents(site={'id': 8, 'domain': 'sidhub.cc'}, keyword='multi')
            self.assertTrue(late_detail.is_set())
            self.assertEqual([row.enclosure for row in rows], [stamp])
        finally:
            release.set()

    def test_completed_future_after_deadline_is_not_returned_or_cached(self):
        self.plugin.init_plugin({'flaresolverr_url': self.url})
        self.plugin.save_data = Mock()
        deadline = time.monotonic() + .1
        stamp = 'magnet:?xt=urn:btih:' + 'a' * 40

        def resolve_after_deadline(_seed_id, limit):
            time.sleep(max(0, limit - time.monotonic()) + .02)
            return stamp

        def collect_completed(futures, timeout):
            for future in futures:
                future.result(timeout=1)
                yield future

        self.plugin._SeedHubIndexer__resolve_magnet = resolve_after_deadline
        setattr(self.module, 'as_completed', collect_completed)
        rows, *_ = self.plugin._SeedHubIndexer__resolve_rows([{'seed_id': '1'}], deadline=deadline)
        self.assertEqual(rows, [])
        self.assertEqual(self.plugin._magnet_cache, {})
        self.plugin.save_data.assert_not_called()
