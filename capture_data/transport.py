"""受控传输：来源限定、凭据不出源、可回放。"""
import json
import urllib.error
import urllib.request
from urllib.parse import urlsplit

from .io import Replay


class CrossOriginRequestBlocked(Exception):
    """拒绝向采集来源之外的主机发起请求（凭据不外发）。"""


def origin_of(url):
    p = urlsplit(url)
    scheme = p.scheme.lower()
    host = (p.hostname or '').lower()
    if p.port is not None:
        port = p.port
    elif scheme == 'https':
        port = 443
    elif scheme == 'http':
        port = 80
    else:
        port = None
    return (scheme, host, port)


class ReplayTransport:
    """按冻结的呼叫记录回放响应，用于离线核对与测试。"""

    def __init__(self, records):
        self.replay = Replay(records)

    @property
    def requests(self):
        return self.replay.requests

    def request(self, method, url, headers=None):
        return self.replay.next(method, url, headers)


class AuthGuard:
    """仅向 base_url 同源地址发请求并附加凭据；跨源请求一律拒绝。"""

    def __init__(self, transport, base_url, token=None):
        self.transport = transport
        self.base_origin = origin_of(base_url)
        self.token = token

    def request(self, method, url, headers=None):
        if origin_of(url) != self.base_origin:
            raise CrossOriginRequestBlocked(url)
        merged = dict(headers or {})
        if self.token:
            merged['Authorization'] = 'Bearer ' + self.token
        return self.transport.request(method, url, merged)


class UrllibTransport:
    """真实 HTTP 传输（无第三方依赖）；测试与离线核对请使用 ReplayTransport。"""

    def __init__(self, timeout=30):
        self.timeout = timeout

    def request(self, method, url, headers=None):
        req = urllib.request.Request(url, method=method, headers=dict(headers or {}))
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return {'status': resp.status, 'body': json.loads(resp.read().decode('utf-8'))}
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                body = json.loads(raw.decode('utf-8'))
            except ValueError:
                body = {'raw': raw.decode('utf-8', 'replace')}
            return {'status': exc.code, 'body': body}
