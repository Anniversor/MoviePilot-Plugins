"""OpenList v4.2.7 HTTP boundary. Tokens exist only in this object's memory."""
import http.client
import ipaddress
import json
import socket
from pathlib import PurePosixPath
from urllib.parse import quote, urlsplit

USER_AGENT = "MoviePilot-Qb115Downloader/0.1"


class CloudError(RuntimeError):
    """Only fixed diagnostic strings/status codes, never response messages/URLs.

    ``transient`` marks failures where the remote state is merely unknown (transport, timeout,
    HTTP 5xx). Callers must probe/back off instead of treating them as a definitive "absent".
    """
    transient = False


class CloudTransientError(CloudError):
    """Remote state unknown: timeout/reset/no answer. ``outcome_unknown`` => request may still be running."""
    transient = True
    outcome_unknown = True


class CloudServerError(CloudTransientError):
    """The server answered HTTP 5xx: the request completed (unsuccessfully), nothing keeps running."""
    outcome_unknown = False


class CloudMissing(CloudError):
    missing = True


# 115 download links point at 115's CDN (hosts below are the 115 families I know; NOT verified against a live link
# in this environment - add real ones through the ``raw_url_allowed_hosts`` setting). A web-proxied storage answers
# with the OpenList host itself, which is always allowed.
RAW_HOST_SUFFIXES = ("115.com", "115cdn.com", "115cdn.net")


def raw_host_allowed(host, origin_host, extra=(), port=None, origin_port=None):
    """The unauthenticated readability probe may only contact the OpenList host or a known 115 CDN family.

    ``extra`` entries: ``example.org`` = that exact host, ``.example.org`` = that host and every subdomain.
    Same-host mode is pinned to the OpenList origin port when both ports are given: the same machine's other
    services (ssh, admin panels, ...) are never probed. CDN families / extra hosts keep any port."""
    host = str(host or "").lower().rstrip(".")
    if not host:
        return False
    if host == str(origin_host or "").lower().rstrip("."):
        return port is None or origin_port is None or port == origin_port
    if any(host == d or host.endswith("." + d) for d in RAW_HOST_SUFFIXES):
        return True
    for entry in extra:
        entry = str(entry or "").lower().rstrip(".")
        if entry.startswith("."):
            if host == entry[1:] or host.endswith(entry):
                return True
        elif entry and host == entry:
            return True
    return False


def _private_address(text):
    address = ipaddress.ip_address(str(text).split("%")[0])
    return address.is_loopback or address.is_private or address.is_link_local


def _default_resolver(host):
    return [info[4][0] for info in socket.getaddrinfo(host, None)]


def _plaintext_http_allowed(host, resolver):
    """Token travels in clear text over http: only loopback / private addresses (hostnames must resolve to those)."""
    try:
        return _private_address(host)
    except ValueError:
        pass  # not an IP literal: resolve it
    try:
        addresses = list(resolver(host))
        return bool(addresses) and all(_private_address(a) for a in addresses)
    except Exception:
        return False


class OpenList:
    # OpenList v4.2.7 handles ``As-Task:false`` PUT synchronously and answers only after the real 115
    # upload finished (115_open Put hashes/caches the stream first, then pushes it to OSS). The read
    # timeout of PUT therefore has to cover the whole upload; every other request keeps the short one.
    PUT_TIMEOUT_FLOOR = 600
    PUT_MIN_RATE = 1024 * 1024  # assumed worst sustained throughput, bytes/s
    PUT_TIMEOUT_CEILING = 24 * 3600

    def __init__(self, base_url, token, timeout=60, put_timeout=None, resolver=None, raw_hosts=()):
        url = urlsplit(base_url)
        if url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password or url.query or url.fragment or url.path not in {"", "/"}:
            raise ValueError("OpenList URL must be an origin without credentials")
        if url.scheme == "http" and not _plaintext_http_allowed(url.hostname, resolver or _default_resolver):
            raise ValueError("plaintext http OpenList origin must be loopback/private")
        self._origin = base_url.rstrip("/")
        self._token = token
        self.timeout = timeout
        self.put_timeout = put_timeout
        self.raw_hosts = tuple(raw_hosts)

    def put_timeout_for(self, size):
        """Explicit ``put_timeout`` wins; otherwise estimate from size with a generous floor."""
        if self.put_timeout:
            return self.put_timeout
        estimate = self.PUT_TIMEOUT_FLOOR + int(size or 0) / self.PUT_MIN_RATE
        return min(self.PUT_TIMEOUT_CEILING, max(self.PUT_TIMEOUT_FLOOR, estimate))

    def _request(self, method, endpoint, body=None, headers=None, read_timeout=None):
        url = urlsplit(self._origin)
        connection = (http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection)(url.hostname, url.port, timeout=self.timeout)
        try:
            connection.connect()  # connect keeps the short timeout
            if read_timeout:
                connection.sock.settimeout(read_timeout)
            h = {"Authorization": self._token, "User-Agent": USER_AGENT, **(headers or {})}
            connection.request(method, endpoint, body=body, headers=h)
            response = connection.getresponse()
            if not 200 <= response.status < 300:
                raise (CloudServerError if response.status >= 500 else CloudError)("OpenList HTTP %s" % response.status)
            payload = response.read(1024 * 1024 + 1)
            if len(payload) > 1024 * 1024:
                raise CloudError("OpenList response too large")
            result = json.loads(payload)
            if result.get("code") != 200:
                rejected = CloudError("OpenList rejected request")
                # OpenList answers a missing object with errs.ObjectNotFound ("object not found") inside a
                # code=500 JSON. Only this boolean leaves the response; the message text itself is dropped.
                rejected.not_found = "not found" in str(result.get("message") or "").lower()
                raise rejected
            return result.get("data")
        except CloudError:
            raise
        except (socket.timeout, TimeoutError):
            raise CloudTransientError("OpenList request timed out") from None
        except Exception:
            raise CloudTransientError("OpenList transport/response failure") from None
        finally:
            connection.close()

    def validate_storage(self, staging, monitor):
        """Same 115 Open mount only; don't treat cross-storage copy as atomic move."""
        mounts, page, seen = [], 1, 0
        while True:
            data = self._request("GET", "/api/admin/storage/list?page=%s&per_page=100" % page)
            content = data.get("content") or []
            # Never retain addition/access_token/refresh_token or full storage payload.
            mounts.extend({k: row.get(k) for k in ("id", "mount_path", "driver", "disabled")} for row in content)
            seen += len(content)
            if seen >= data["total"]:
                break
            if not content or page >= 100:
                raise CloudError("storage discovery incomplete")
            page += 1
        enabled = [m for m in mounts if m["disabled"] is False]
        base = [m for m in enabled if m["mount_path"] == "/115" and m["driver"] == "115 Open"]
        if len(base) != 1 or not base[0]["id"]:
            raise CloudError("a single /115 115 Open storage is required")
        for root in (staging, monitor):
            if not root.startswith("/115/"):
                raise CloudError("cloud roots must be below /115")
            for mount in enabled:
                path = str(mount["mount_path"] or "").rstrip("/")
                if path != "/115" and (path == root or path.startswith(root + "/") or root.startswith(path + "/")):
                    raise CloudError("nested/cross-storage handoff refused")
            obj = self.stat(root)
            if not obj or not obj.get("is_dir"):
                raise CloudError("staging and monitor must already exist")
        return base[0]["id"]

    def post(self, endpoint, data):
        return self._request("POST", endpoint, json.dumps(data).encode(), {"Content-Type": "application/json"})

    def list_dir(self, path, per_page=200):
        """All entries of ONE directory (no recursion). Only the first page forces a 115 refresh."""
        entries, page = [], 1
        while True:
            data = self.post("/api/fs/list", dict(path=path, refresh=(page == 1), page=page, per_page=per_page))
            content = data.get("content") or []
            entries.extend(content)
            if len(entries) >= data["total"]:
                return entries
            if not content or page >= 100:
                raise CloudError("incomplete directory listing")
            page += 1

    def stat(self, path):
        """``/api/fs/get`` first: one request, no directory scan (OpenList's 115 Open driver asks 115 directly).

        A recognised "object not found" answer means absent without listing anything. Any other rejection is
        confirmed with a single listing of the parent (first page refreshed); transport errors are never turned into
        "absent"."""
        try:
            return self.post("/api/fs/get", dict(path=path))
        except CloudError as err:
            if getattr(err, "transient", False):
                raise
            if getattr(err, "not_found", False):
                return None
        parent, name = str(PurePosixPath(path).parent), PurePosixPath(path).name
        if any(item["name"] == name for item in self.list_dir(parent)):
            return self.post("/api/fs/get", dict(path=path))
        return None

    def mkdir(self, path):
        existing = self.stat(path)
        if existing is None:
            self.post("/api/fs/mkdir", dict(path=path))
            existing = self.stat(path)
        if not existing or not existing.get("is_dir"):
            raise CloudError("directory creation not confirmed")

    @staticmethod
    def _expected_tree(root, manifest):
        expected = {root: set()}
        for item in manifest:
            parts = item["remote"].split("/")
            current = root
            for part in parts[:-1]:
                expected.setdefault(current, set()).add((part, True))
                current += "/" + part
                expected.setdefault(current, set())
            expected[current].add((parts[-1], False))
        return expected

    def verify_batch(self, root, manifest):
        """List only manifest-known directories. Refuse unknown entries; never recurse."""
        for path, names in self._expected_tree(root, manifest).items():
            entries = self.list_dir(path)
            actual = {(entry["name"], bool(entry["is_dir"])) for entry in entries}
            if len(entries) != len(actual) or actual != names:
                raise CloudError("stage contains unexpected or missing files")

    def count_present(self, root, manifest):
        """After the move: how many manifest files are still below ``root`` (CLM may already have taken some).

        One listing per manifest-known directory instead of two requests per file. A directory that is gone counts as
        empty; a file that is present with another size/type is a real mismatch."""
        listings = {}
        for path in self._expected_tree(root, manifest):
            try:
                listings[path] = {e["name"]: e for e in self.list_dir(path)}
            except CloudError as err:
                if not getattr(err, "not_found", False):
                    raise
                listings[path] = {}
        present = 0
        for item in manifest:
            parts = item["remote"].split("/")
            entry = listings.get("/".join([root] + parts[:-1]), {}).get(parts[-1])
            if entry is None:
                continue
            if entry.get("is_dir") or entry.get("size") != item["size"]:
                raise CloudError("remote size verification failed")
            present += 1
        return present

    def move(self, src_dir, dst_dir, name):
        return self.post("/api/fs/move", dict(src_dir=src_dir, dst_dir=dst_dir, names=[name],
                                             overwrite=False, skip_existing=False, merge=False))

    def verify(self, path, expected):
        import hashlib
        data = self.stat(path)
        if not data:
            raise CloudMissing("remote object missing")
        if data.get("is_dir") or data.get("size") != expected["size"]:
            raise CloudError("remote size verification failed")
        sha1 = (data.get("hash_info") or {}).get("sha1")
        if sha1 and sha1.lower() != expected["sha1"].lower():
            raise CloudError("remote sha1 verification failed")
        # No OpenList token on raw/CDN requests; no redirects or URL logging.
        url = urlsplit(data.get("raw_url") or "")
        if url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password:
            raise CloudError("remote readable URL unavailable")
        origin = urlsplit(self._origin)
        try:
            port = url.port or (443 if url.scheme == "https" else 80)
        except ValueError:
            raise CloudError("remote readable URL unavailable") from None
        origin_port = origin.port or (443 if origin.scheme == "https" else 80)
        if not raw_host_allowed(url.hostname, origin.hostname, self.raw_hosts, port=port, origin_port=origin_port):
            refused = CloudError("remote readable URL host not allowed")
            refused.raw_host_refused = True  # fixed code only: the host/URL is never echoed
            raise refused
        connection = (http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection)(url.hostname, url.port, timeout=self.timeout)
        try:
            endpoint = url.path + (("?" + url.query) if url.query else "")
            length = min(4096, expected["size"])
            connection.request("GET", endpoint, headers={"Range": "bytes=0-%s" % (length - 1), "User-Agent": USER_AGENT})
            response = connection.getresponse()
            content = response.read(length)
            if response.status not in {200, 206} or len(content) != length or hashlib.sha1(content).hexdigest() != expected["head"]:
                raise CloudError("remote readability verification failed")
        except CloudError:
            raise
        except Exception:
            raise CloudTransientError("remote readability request failed") from None
        finally:
            connection.close()
        return {"size": data["size"], "sha1": sha1 or "", "readable": True}

    def put(self, path, stream, size, sha1):
        return self._request("PUT", "/api/fs/put", stream,
                             read_timeout=self.put_timeout_for(size), headers=
                             {"File-Path": quote(path, safe=""), "As-Task": "false",
                              "Overwrite": "false", "Content-Length": str(size),
                              "X-File-Sha1": sha1, "Content-Type": "application/octet-stream"})
