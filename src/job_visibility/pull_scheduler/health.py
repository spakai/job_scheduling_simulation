from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Protocol

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


class WorkerHealth(Protocol):
    @property
    def ready(self) -> bool: ...

    @property
    def health_details(self) -> dict[str, object]: ...


class HealthServer:
    def __init__(self, worker: WorkerHealth, port: int) -> None:
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/health/live":
                    self._json(200, {"status": "alive"})
                elif self.path == "/health/ready":
                    self._json(
                        200 if worker.ready else 503,
                        {"status": "ready" if worker.ready else "degraded"} | worker.health_details,
                    )
                elif self.path == "/metrics":
                    body = generate_latest()
                    self.send_response(200)
                    self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                else:
                    self._json(404, {"detail": "not found"})

            def _json(self, status: int, payload: dict[str, object]) -> None:
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, _format: str, *_args: object) -> None:
                return

        self.server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
