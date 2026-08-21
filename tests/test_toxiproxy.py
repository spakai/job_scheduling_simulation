import httpx

from job_visibility.testing import ToxiproxyClient


def test_toxic_lifecycle_uses_named_bounded_api_calls() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(200, json={"name": "attempt-1-timeout"})
        return httpx.Response(204)

    with ToxiproxyClient(transport=httpx.MockTransport(handler)) as client:
        toxic = client.add_toxic(
            "cassandra",
            "attempt-1-timeout",
            "timeout",
            attributes={"timeout": 100},
        )
        client.remove_toxic("cassandra", "attempt-1-timeout")

    assert toxic["name"] == "attempt-1-timeout"
    assert requests[0].url.path == "/proxies/cassandra/toxics"
    assert b'"stream":"downstream"' in requests[0].content
    assert requests[1].url.path.endswith("/toxics/attempt-1-timeout")


def test_removing_absent_toxic_is_idempotent() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(404))

    with ToxiproxyClient(transport=transport) as client:
        client.remove_toxic("cassandra", "already-gone")
