"""Приёмник webhook для e2e и ручной проверки: python -m tests.e2e.webhook_sink

POST /hooks/<name>  — запомнить вызов; имена с префиксом fail- отвечают 500
GET  /hooks/<name>  — принятые вызовы: {"calls": [{"headers": {...}, "body": {...}}]}
GET  /health
"""

from __future__ import annotations

import json
import os
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

_PREFIX = '/hooks/'
_HEADERS = ('content-type', 'x-event-type', 'x-payment-id', 'user-agent')
_calls: dict[str, list[dict[str, Any]]] = {}
_lock = threading.Lock()


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == '/health':
            self._reply(HTTPStatus.OK, {'status': 'ok'})
            return
        name = self._hook_name()
        if name is None:
            self._reply(HTTPStatus.NOT_FOUND, {'detail': 'not found'})
            return
        with _lock:
            calls = list(_calls.get(name, []))
        self._reply(HTTPStatus.OK, {'calls': calls})

    def do_POST(self) -> None:
        name = self._hook_name()
        if name is None:
            self._reply(HTTPStatus.NOT_FOUND, {'detail': 'not found'})
            return
        length = int(self.headers.get('content-length') or 0)
        raw = self.rfile.read(length)
        try:
            body: Any = json.loads(raw)
        except ValueError:
            body = raw.decode(errors='replace')
        headers = {key: self.headers[key] for key in _HEADERS if key in self.headers}
        with _lock:
            _calls.setdefault(name, []).append({'headers': headers, 'body': body})
        if name.startswith('fail-'):
            self._reply(HTTPStatus.INTERNAL_SERVER_ERROR, {'detail': 'simulated failure'})
            return
        self._reply(HTTPStatus.OK, {'status': 'received'})

    def log_message(self, format: str, *args: Any) -> None:
        print(f'webhook-sink: {self.command} {self.path} {format % args}', flush=True)

    def _hook_name(self) -> str | None:
        if not self.path.startswith(_PREFIX):
            return None
        name = self.path[len(_PREFIX) :].strip('/')
        return name or None

    def _reply(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main() -> None:
    port = int(os.environ.get('WEBHOOK_SINK_PORT', '8080'))
    server = ThreadingHTTPServer(('0.0.0.0', port), _Handler)
    print(f'webhook-sink listening on :{port}', flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()
