"""Real loopback HTTP boundary; never contacts user services."""
import hashlib
import http.server
import io
import json
from pathlib import Path
import sys
import threading
import time
import unittest
from urllib.parse import unquote
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
try:
    import cloud
except ImportError:
    cloud = None


class Server(http.server.ThreadingHTTPServer):
    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.objects = {"/115": None, "/115/staging": None, "/115/watch": None}
        self.puts = []
        self.moves = []
        self.raw_headers = []
        self.api_headers = []
        self.fail_put = None
        self.fail_json = False
        self.duplicate = False
        self.storages = [dict(id=1, mount_path="/115", driver="115 Open", disabled=False)]
        self.after_put = None
        self.put_delay = 0
        self.list_delay = 0
        self.lists = []  # (path, refresh, page) of every /api/fs/list call
        self.gets = []   # path of every /api/fs/get call
        self.page_size = None  # cap entries per page to exercise pagination
        self.not_found_message = "failed to get obj: object not found"
        self.raw_base = None
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self):
        return "http://127.0.0.1:%s" % self.server_port

    def close(self):
        self.shutdown()
        self.server_close()
        self.thread.join()


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, data=None, code=200, http=200, message="secret-token PRIVATE-PASSKEY"):
        payload = json.dumps(dict(code=code, data=data, message=message)).encode()
        try:
            self.send_response(http)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):  # client already timed out
            pass

    def do_PUT(self):
        name = unquote(self.headers["File-Path"])
        content = self.rfile.read(int(self.headers["Content-Length"]))
        self.server.puts.append((name, content, dict(self.headers)))
        if self.server.put_delay:
            time.sleep(self.server.put_delay)  # real OpenList As-Task:false answers only after the 115 upload
        if self.server.fail_put == len(self.server.puts):
            return self.respond(code=500, http=503)
        if self.server.fail_json:
            return self.respond(code=500)
        if self.server.duplicate:
            return self.respond(code=409, http=409)
        self.server.objects[name] = content
        if self.server.after_put:
            self.server.after_put()
        self.respond()

    def do_POST(self):
        req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        objects = self.server.objects
        if self.path == "/api/fs/list":
            if self.server.list_delay:
                time.sleep(self.server.list_delay)
            parent = req["path"].rstrip("/")
            self.server.lists.append((parent, bool(req.get("refresh")), req.get("page", 1)))
            if parent not in objects:
                return self.respond(code=500, message=self.server.not_found_message)
            content = []
            for name, data in objects.items():
                if str(Path(name).parent) == parent:
                    content.append(dict(name=Path(name).name, size=len(data or b""), is_dir=data is None))
            total = len(content)
            per_page = min(req.get("per_page") or 200, self.server.page_size or 10**9)
            start = (req.get("page", 1) - 1) * per_page
            return self.respond(dict(content=content[start:start + per_page], total=total))
        if self.path == "/api/fs/mkdir":
            objects.setdefault(req["path"], None)
            return self.respond()
        if self.path == "/api/fs/get":
            self.server.api_headers.append(dict(self.headers))
            name = req["path"]
            self.server.gets.append(name)
            if name not in objects:
                return self.respond(code=500, message=self.server.not_found_message)
            data = objects[name]
            return self.respond(dict(name=Path(name).name, size=len(data or b""), is_dir=data is None,
                                     hash_info={"sha1": hashlib.sha1(data or b"").hexdigest()},
                                     raw_url=(self.server.raw_base or self.server.url) + "/raw/" + name))
        if self.path == "/api/fs/move":
            self.server.moves.append(req)
            for name in req["names"]:
                src = req["src_dir"] + "/" + name
                dst = req["dst_dir"] + "/" + name
                if dst in objects:
                    return self.respond(code=409, http=409)
                for key in list(objects):
                    if key == src or key.startswith(src + "/"):
                        objects[dst + key[len(src):]] = objects.pop(key)
            return self.respond()
        self.respond(code=404, http=404)

    def do_GET(self):
        if self.path.startswith("/api/admin/storage/list?"):
            return self.respond(dict(content=self.server.storages, total=len(self.server.storages)))
        self.server.raw_headers.append(dict(self.headers))
        name = unquote(self.path[len("/raw/"):])
        data = self.server.objects.get(name, b"") or b""
        data = data[:4096]
        self.send_response(206)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class CloudTests(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.addCleanup(self.server.close)

    def test_readability_uses_signed_link_user_agent_but_never_sends_token_to_raw_url(self):
        api = cloud.OpenList(self.server.url, "secret-token")
        content = b"media"
        digest = hashlib.sha1(content).hexdigest()
        self.server.objects["/115/staging/a.mkv"] = content
        api.verify("/115/staging/a.mkv", dict(size=len(content), sha1=digest, head=digest))
        headers = self.server.raw_headers[0]
        self.assertTrue(headers.get("User-Agent"), "115 download link signing requires consistent User-Agent")
        self.assertEqual(headers["User-Agent"], self.server.api_headers[-1]["User-Agent"])
        self.assertNotIn("Authorization", headers)
        self.assertEqual(headers["Range"], "bytes=0-4")

    def test_commit_requires_same_115_open_storage_without_nested_mounts(self):
        api = cloud.OpenList(self.server.url, "secret-token")
        self.assertTrue(callable(getattr(api, "validate_storage", None)))
        self.assertEqual(api.validate_storage("/115/staging", "/115/watch"), 1)
        self.server.storages.append(dict(id=2, mount_path="/115/watch/nested", driver="115 Open", disabled=False))
        with self.assertRaises(cloud.CloudError):
            api.validate_storage("/115/staging", "/115/watch")
        self.server.storages.pop()
        self.server.storages[0]["driver"] = "Local"
        with self.assertRaises(cloud.CloudError):
            api.validate_storage("/115/staging", "/115/watch")

    def test_stream_put_sends_real_content_and_versioned_sha1_header(self):
        self.assertIsNotNone(cloud)
        api = cloud.OpenList(self.server.url, "secret-token")
        data = b"REAL MEDIA" * 10000
        digest = hashlib.sha1(data).hexdigest()
        class BoundedStream(io.BytesIO):
            def read(self, size=-1):
                if size <= 0 or size > 1024 * 1024:
                    raise AssertionError("upload must use bounded reads")
                return super().read(size)
        api.put("/115/staging/中文.mkv", BoundedStream(data), len(data), digest)
        name, sent, headers = self.server.puts[0]
        self.assertEqual(sent, data)
        self.assertEqual(headers["X-File-Sha1"], digest)
        self.assertEqual(headers["As-Task"], "false")
        self.assertEqual(headers["Overwrite"], "false")
        self.assertEqual(headers["Content-Length"], str(len(data)))
        self.assertNotIn("中文", headers["File-Path"])
        self.assertEqual(name, "/115/staging/中文.mkv")
