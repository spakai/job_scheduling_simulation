from __future__ import annotations

from typing import Any

import httpx


class ToxiproxyClient:
    """Small bounded client used by chaos fixtures; no production runtime imports it."""

    def __init__(
        self,
        base_url: str = "http://localhost:8474",
        *,
        connect_timeout_seconds: float = 2,
        read_timeout_seconds: float = 5,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            timeout=httpx.Timeout(
                connect=connect_timeout_seconds,
                read=read_timeout_seconds,
                write=read_timeout_seconds,
                pool=connect_timeout_seconds,
            ),
            transport=transport,
        )

    def close(self) -> None:
        self._client.close()

    def proxy(self, name: str) -> dict[str, Any]:
        response = self._client.get(f"/proxies/{name}")
        response.raise_for_status()
        return response.json()

    def add_toxic(
        self,
        proxy: str,
        name: str,
        toxic_type: str,
        *,
        stream: str = "downstream",
        toxicity: float = 1.0,
        attributes: dict[str, object] | None = None,
    ) -> dict[str, Any]:
        response = self._client.post(
            f"/proxies/{proxy}/toxics",
            json={
                "name": name,
                "type": toxic_type,
                "stream": stream,
                "toxicity": toxicity,
                "attributes": attributes or {},
            },
        )
        response.raise_for_status()
        return response.json()

    def remove_toxic(self, proxy: str, name: str) -> None:
        response = self._client.delete(f"/proxies/{proxy}/toxics/{name}")
        if response.status_code != 404:
            response.raise_for_status()

    def reset(self) -> None:
        response = self._client.post("/reset")
        response.raise_for_status()

    def __enter__(self) -> ToxiproxyClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
