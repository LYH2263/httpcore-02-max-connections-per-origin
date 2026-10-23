import logging
import typing

import hpack
import hyperframe.frame
import pytest
import trio as concurrency

import httpcore


@pytest.mark.anyio
async def test_connection_pool_with_keepalive():
    """
    By default HTTP/1.1 requests should be returned to the connection pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        async with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            assert (
                repr(pool)
                == "<AsyncConnectionPool [Requests: 1 active, 0 queued | Connections: 1 active, 0 idle]>"
            )
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 1]>"
        ]
        assert (
            repr(pool)
            == "<AsyncConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 1 idle]>"
        )

        # Sending a second request to the same origin will reuse the existing IDLE connection.
        async with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 2]>"
            ]
            assert (
                repr(pool)
                == "<AsyncConnectionPool [Requests: 1 active, 0 queued | Connections: 1 active, 0 idle]>"
            )
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 2]>"
        ]
        assert (
            repr(pool)
            == "<AsyncConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 1 idle]>"
        )

        # Sending a request to a different origin will not reuse the existing IDLE connection.
        async with pool.stream("GET", "http://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 2]>",
                "<AsyncHTTPConnection ['http://example.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
            ]
            assert (
                repr(pool)
                == "<AsyncConnectionPool [Requests: 1 active, 0 queued | Connections: 1 active, 1 idle]>"
            )
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 2]>",
            "<AsyncHTTPConnection ['http://example.com:80', HTTP/1.1, IDLE, Request Count: 1]>",
        ]
        assert (
            repr(pool)
            == "<AsyncConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 2 idle]>"
        )


@pytest.mark.anyio
async def test_connection_pool_with_close():
    """
    HTTP/1.1 requests that include a 'Connection: Close' header should
    not be returned to the connection pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        # Sending an intial request, which once complete will not return to the pool.
        async with pool.stream(
            "GET", "https://example.com/", headers={"Connection": "close"}
        ) as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == []


@pytest.mark.anyio
async def test_connection_pool_with_http2():
    """
    Test a connection pool with HTTP/2 requests.
    """
    network_backend = httpcore.AsyncMockBackend(
        buffer=[
            hyperframe.frame.SettingsFrame().serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=1,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=3,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=3, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
        ],
        http2=True,
    )

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        response = await pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>"
        ]

        # Sending a second request to the same origin will reuse the existing IDLE connection.
        response = await pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 2]>"
        ]


@pytest.mark.anyio
async def test_connection_pool_with_http2_goaway():
    """
    Test a connection pool with HTTP/2 requests, that cleanly disconnects
    with a GoAway frame after the first request.
    """
    network_backend = httpcore.AsyncMockBackend(
        buffer=[
            hyperframe.frame.SettingsFrame().serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=1,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
            hyperframe.frame.GoAwayFrame(
                stream_id=0, error_code=0, last_stream_id=1
            ).serialize(),
            b"",
        ],
        http2=True,
    )

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        response = await pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>"
        ]

        # Sending a second request to the same origin will require a new connection.
        # The original connection has now been closed.
        response = await pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>",
        ]


@pytest.mark.anyio
async def test_trace_request():
    """
    The 'trace' request extension allows for a callback function to inspect the
    internal events that occur while sending a request.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    called = []

    async def trace(name, kwargs):
        called.append(name)

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        await pool.request("GET", "https://example.com/", extensions={"trace": trace})

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.complete",
        "connection.start_tls.started",
        "connection.start_tls.complete",
        "http11.send_request_headers.started",
        "http11.send_request_headers.complete",
        "http11.send_request_body.started",
        "http11.send_request_body.complete",
        "http11.receive_response_headers.started",
        "http11.receive_response_headers.complete",
        "http11.receive_response_body.started",
        "http11.receive_response_body.complete",
        "http11.response_closed.started",
        "http11.response_closed.complete",
    ]


@pytest.mark.anyio
async def test_debug_request(caplog):
    """
    The 'trace' request extension allows for a callback function to inspect the
    internal events that occur while sending a request.
    """
    caplog.set_level(logging.DEBUG)

    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        await pool.request("GET", "http://example.com/")

    assert caplog.record_tuples == [
        (
            "httpcore.connection",
            logging.DEBUG,
            "connect_tcp.started host='example.com' port=80 local_address=None timeout=None socket_options=None",
        ),
        (
            "httpcore.connection",
            logging.DEBUG,
            "connect_tcp.complete return_value=<httpcore.AsyncMockStream>",
        ),
        (
            "httpcore.http11",
            logging.DEBUG,
            "send_request_headers.started request=<Request [b'GET']>",
        ),
        ("httpcore.http11", logging.DEBUG, "send_request_headers.complete"),
        (
            "httpcore.http11",
            logging.DEBUG,
            "send_request_body.started request=<Request [b'GET']>",
        ),
        ("httpcore.http11", logging.DEBUG, "send_request_body.complete"),
        (
            "httpcore.http11",
            logging.DEBUG,
            "receive_response_headers.started request=<Request [b'GET']>",
        ),
        (
            "httpcore.http11",
            logging.DEBUG,
            "receive_response_headers.complete return_value="
            "(b'HTTP/1.1', 200, b'OK', [(b'Content-Type', b'plain/text'), (b'Content-Length', b'13')])",
        ),
        (
            "httpcore.http11",
            logging.DEBUG,
            "receive_response_body.started request=<Request [b'GET']>",
        ),
        ("httpcore.http11", logging.DEBUG, "receive_response_body.complete"),
        ("httpcore.http11", logging.DEBUG, "response_closed.started"),
        ("httpcore.http11", logging.DEBUG, "response_closed.complete"),
        ("httpcore.connection", logging.DEBUG, "close.started"),
        ("httpcore.connection", logging.DEBUG, "close.complete"),
    ]


@pytest.mark.anyio
async def test_connection_pool_with_http_exception():
    """
    HTTP/1.1 requests that result in an exception during the connection should
    not be returned to the connection pool.
    """
    network_backend = httpcore.AsyncMockBackend([b"Wait, this isn't valid HTTP!"])

    called = []

    async def trace(name, kwargs):
        called.append(name)

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        # Sending an initial request, which once complete will not return to the pool.
        with pytest.raises(Exception):
            await pool.request(
                "GET", "https://example.com/", extensions={"trace": trace}
            )

        info = [repr(c) for c in pool.connections]
        assert info == []

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.complete",
        "connection.start_tls.started",
        "connection.start_tls.complete",
        "http11.send_request_headers.started",
        "http11.send_request_headers.complete",
        "http11.send_request_body.started",
        "http11.send_request_body.complete",
        "http11.receive_response_headers.started",
        "http11.receive_response_headers.failed",
        "http11.response_closed.started",
        "http11.response_closed.complete",
    ]


@pytest.mark.anyio
async def test_connection_pool_with_connect_exception():
    """
    HTTP/1.1 requests that result in an exception during connection should not
    be returned to the connection pool.
    """

    class FailedConnectBackend(httpcore.AsyncMockBackend):
        async def connect_tcp(
            self,
            host: str,
            port: int,
            timeout: typing.Optional[float] = None,
            local_address: typing.Optional[str] = None,
            socket_options: typing.Optional[
                typing.Iterable[httpcore.SOCKET_OPTION]
            ] = None,
        ) -> httpcore.AsyncNetworkStream:
            raise httpcore.ConnectError("Could not connect")

    network_backend = FailedConnectBackend([])

    called = []

    async def trace(name, kwargs):
        called.append(name)

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        # Sending an initial request, which once complete will not return to the pool.
        with pytest.raises(Exception):
            await pool.request(
                "GET", "https://example.com/", extensions={"trace": trace}
            )

        info = [repr(c) for c in pool.connections]
        assert info == []

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.failed",
    ]


@pytest.mark.anyio
async def test_connection_pool_with_immediate_expiry():
    """
    Connection pools with keepalive_expiry=0.0 should immediately expire
    keep alive connections.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        keepalive_expiry=0.0,
        network_backend=network_backend,
    ) as pool:
        # Sending an intial request, which once complete will not return to the pool.
        async with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == []


@pytest.mark.anyio
async def test_connection_pool_with_no_keepalive_connections_allowed():
    """
    When 'max_keepalive_connections=0' is used, IDLE connections should not
    be returned to the pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        max_keepalive_connections=0, network_backend=network_backend
    ) as pool:
        # Sending an intial request, which once complete will not return to the pool.
        async with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == []


@pytest.mark.trio
async def test_connection_pool_concurrency():
    """
    HTTP/1.1 requests made in concurrency must not ever exceed the maximum number
    of allowable connection in the pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async def fetch(pool, domain, info_list):
        async with pool.stream("GET", f"http://{domain}/") as response:
            info = [repr(c) for c in pool.connections]
            info_list.append(info)
            await response.aread()

    async with httpcore.AsyncConnectionPool(
        max_connections=1, network_backend=network_backend
    ) as pool:
        info_list: typing.List[str] = []
        async with concurrency.open_nursery() as nursery:
            for domain in ["a.com", "b.com", "c.com", "d.com", "e.com"]:
                nursery.start_soon(fetch, pool, domain, info_list)

        for item in info_list:
            # Check that each time we inspected the connection pool, only a
            # single connection was established at any one time.
            assert len(item) == 1
            # Each connection was to a different host, and only sent a single
            # request on that connection.
            assert item[0] in [
                "<AsyncHTTPConnection ['http://a.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://b.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://c.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://d.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://e.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
            ]


@pytest.mark.trio
async def test_connection_pool_concurrency_same_domain_closing():
    """
    HTTP/1.1 requests made in concurrency must not ever exceed the maximum number
    of allowable connection in the pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"Connection: close\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async def fetch(pool, domain, info_list):
        async with pool.stream("GET", f"https://{domain}/") as response:
            info = [repr(c) for c in pool.connections]
            info_list.append(info)
            await response.aread()

    async with httpcore.AsyncConnectionPool(
        max_connections=1, network_backend=network_backend, http2=True
    ) as pool:
        info_list: typing.List[str] = []
        async with concurrency.open_nursery() as nursery:
            for domain in ["a.com", "a.com", "a.com", "a.com", "a.com"]:
                nursery.start_soon(fetch, pool, domain, info_list)

        for item in info_list:
            # Check that each time we inspected the connection pool, only a
            # single connection was established at any one time.
            assert len(item) == 1
            # Only a single request was sent on each connection.
            assert (
                item[0]
                == "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            )


@pytest.mark.trio
async def test_connection_pool_concurrency_same_domain_keepalive():
    """
    HTTP/1.1 requests made in concurrency must not ever exceed the maximum number
    of allowable connection in the pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
        * 5
    )

    async def fetch(pool, domain, info_list):
        async with pool.stream("GET", f"https://{domain}/") as response:
            info = [repr(c) for c in pool.connections]
            info_list.append(info)
            await response.aread()

    async with httpcore.AsyncConnectionPool(
        max_connections=1, network_backend=network_backend, http2=True
    ) as pool:
        info_list: typing.List[str] = []
        async with concurrency.open_nursery() as nursery:
            for domain in ["a.com", "a.com", "a.com", "a.com", "a.com"]:
                nursery.start_soon(fetch, pool, domain, info_list)

        for item in info_list:
            # Check that each time we inspected the connection pool, only a
            # single connection was established at any one time.
            assert len(item) == 1
            # The connection sent multiple requests.
            assert item[0] in [
                "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 2]>",
                "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 3]>",
                "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 4]>",
                "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 5]>",
            ]

    assert (
        repr(pool)
        == "<AsyncConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 0 idle]>"
    )


@pytest.mark.anyio
async def test_unsupported_protocol():
    async with httpcore.AsyncConnectionPool() as pool:
        with pytest.raises(httpcore.UnsupportedProtocol):
            await pool.request("GET", "ftp://www.example.com/")

        with pytest.raises(httpcore.UnsupportedProtocol):
            await pool.request("GET", "://www.example.com/")


@pytest.mark.anyio
async def test_connection_pool_closed_while_request_in_flight():
    """
    Closing a connection pool while a request/response is still in-flight
    should raise an error.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Send a request, and then close the connection pool while the
        # response has not yet been streamed.
        async with pool.stream("GET", "https://example.com/") as response:
            await pool.aclose()
            with pytest.raises(httpcore.ReadError):
                await response.aread()


@pytest.mark.anyio
async def test_connection_pool_timeout():
    """
    Ensure that exceeding max_connections can cause a request to timeout.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        # Send a request to a pool that is configured to only support a single
        # connection, and then ensure that a second concurrent request
        # fails with a timeout.
        async with pool.stream("GET", "https://example.com/"):
            with pytest.raises(httpcore.PoolTimeout):
                extensions = {"timeout": {"pool": 0.0001}}
                await pool.request("GET", "https://example.com/", extensions=extensions)


@pytest.mark.anyio
async def test_connection_pool_timeout_zero():
    """
    A pool timeout of 0 shouldn't raise a PoolTimeout if there's
    no need to wait on a new connection.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    # Use a pool timeout of zero.
    extensions = {"timeout": {"pool": 0}}

    # A connection pool configured to allow only one connection at a time.
    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        # Two consecutive requests with a pool timeout of zero.
        # Both succeed without raising a timeout.
        response = await pool.request(
            "GET", "https://example.com/", extensions=extensions
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

        response = await pool.request(
            "GET", "https://example.com/", extensions=extensions
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

    # A connection pool configured to allow only one connection at a time.
    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        # Two concurrent requests with a pool timeout of zero.
        # Only the first will succeed without raising a timeout.
        async with pool.stream(
            "GET", "https://example.com/", extensions=extensions
        ) as response:
            # The first response hasn't yet completed.
            with pytest.raises(httpcore.PoolTimeout):
                # So a pool timeout occurs.
                await pool.request("GET", "https://example.com/", extensions=extensions)
            # The first response now completes.
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"


@pytest.mark.anyio
async def test_max_connections_per_origin_invalid():
    """
    'max_connections_per_origin' must be an integer greater than or equal to 1.
    """
    with pytest.raises(ValueError):
        httpcore.AsyncConnectionPool(max_connections_per_origin=0)
    with pytest.raises(ValueError):
        httpcore.AsyncConnectionPool(max_connections_per_origin=-1)


@pytest.mark.trio
async def test_max_connections_per_origin_http11():
    """
    With 'max_connections_per_origin' set, the number of non-closed
    connections to a single origin must not exceed the limit. Once the limit
    is reached, further requests reuse the existing IDLE connections rather
    than opening an additional connection.
    """
    response_bytes = [
        b"HTTP/1.1 200 OK\r\n",
        b"Content-Type: plain/text\r\n",
        b"Content-Length: 13\r\n",
        b"\r\n",
        b"Hello, world!",
    ]

    # Block new connections until all three have been requested, so that the
    # pool deterministically holds three concurrent connections regardless of
    # how the concurrent tasks/threads are scheduled.
    connect_gate = concurrency.Event()
    connect_lock = concurrency.Lock()
    state = {"connections": 0}

    class GatedMockBackend(httpcore.AsyncMockBackend):
        async def connect_tcp(
            self,
            host,
            port,
            timeout=None,
            local_address=None,
            socket_options=None,
        ):
            async with connect_lock:
                state["connections"] += 1
                if state["connections"] == 3:
                    connect_gate.set()
            await connect_gate.wait()
            return await super().connect_tcp(
                host,
                port,
                timeout=timeout,
                local_address=local_address,
                socket_options=socket_options,
            )

    network_backend = GatedMockBackend(response_bytes * 2)

    async def fetch(pool, domain, info_list):
        async with pool.stream("GET", f"http://{domain}/") as response:
            info_list.append(list(pool.connections))
            await response.aread()

    origin = httpcore.Origin(b"http", b"a.com", 80)

    async with httpcore.AsyncConnectionPool(
        max_connections_per_origin=3, network_backend=network_backend
    ) as pool:
        info_list: typing.List[typing.List[httpcore.AsyncConnectionInterface]] = []
        async with concurrency.open_nursery() as nursery:
            for _ in range(3):
                nursery.start_soon(fetch, pool, "a.com", info_list)

        # While the requests were in flight the pool never held more than the
        # allowed number of connections, and all of them were to 'a.com'.
        for connections in info_list:
            assert len(connections) <= 3
            assert all(
                connection.can_handle_request(origin) for connection in connections
            )

        # The three completed connections are kept alive as IDLE connections.
        info = [repr(c) for c in pool.connections]
        assert len(info) == 3
        assert info == [
            "<AsyncHTTPConnection ['http://a.com:80', HTTP/1.1, IDLE, Request Count: 1]>",
            "<AsyncHTTPConnection ['http://a.com:80', HTTP/1.1, IDLE, Request Count: 1]>",
            "<AsyncHTTPConnection ['http://a.com:80', HTTP/1.1, IDLE, Request Count: 1]>",
        ]

        # A fourth request reuses one of the IDLE connections, and does not
        # open a fourth connection to the origin.
        response = await pool.request("GET", "http://a.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert len(info) == 3
        assert sum("Request Count: 2" in entry for entry in info) == 1
        assert sum("Request Count: 1" in entry for entry in info) == 2


@pytest.mark.trio
async def test_max_connections_per_origin_http2():
    """
    Under HTTP/2 a single available connection services multiple requests.
    The per-origin limit counts connections, not streams, so any number of
    concurrent requests must share the single HTTP/2 connection.
    """
    network_backend = httpcore.AsyncMockBackend(
        buffer=[
            hyperframe.frame.SettingsFrame().serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=1,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=3,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=3, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=5,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=5, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
        ],
        http2=True,
    )

    async def fetch(pool, domain, info_list):
        async with pool.stream("GET", f"https://{domain}/") as response:
            info_list.append(list(pool.connections))
            await response.aread()

    origin = httpcore.Origin(b"https", b"a.com", 443)

    async with httpcore.AsyncConnectionPool(
        http2=True, max_connections_per_origin=1, network_backend=network_backend
    ) as pool:
        info_list: typing.List[typing.List[httpcore.AsyncConnectionInterface]] = []
        async with concurrency.open_nursery() as nursery:
            for _ in range(3):
                nursery.start_soon(fetch, pool, "a.com", info_list)

        # All the concurrent requests multiplexed onto a single connection.
        for connections in info_list:
            assert len(connections) == 1
            assert connections[0].can_handle_request(origin)

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://a.com:443', HTTP/2, IDLE, Request Count: 3]>"
        ]


@pytest.mark.anyio
async def test_max_connections_per_origin_other_origin_proceeds():
    """
    When one origin has reached its per-origin limit, requests to other
    origins must still be able to obtain connections, while requests to the
    saturated origin wait in the queue and may fail on the pool timeout.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        max_connections=5,
        max_connections_per_origin=1,
        network_backend=network_backend,
    ) as pool:
        # Hold a single connection to 'a.com' open and active.
        async with pool.stream("GET", "http://a.com/"):
            # A second request to 'a.com' cannot open another connection,
            # and times out waiting in the queue.
            with pytest.raises(httpcore.PoolTimeout):
                extensions = {"timeout": {"pool": 0.0001}}
                await pool.request("GET", "http://a.com/", extensions=extensions)

            # Requests to a different origin are unaffected and complete.
            response = await pool.request("GET", "http://b.com/")
            assert response.status == 200
            assert response.content == b"Hello, world!"

            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['http://a.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://b.com:80', HTTP/1.1, IDLE, Request Count: 1]>",
            ]


@pytest.mark.anyio
async def test_max_connections_per_origin_no_idle_eviction():
    """
    A request blocked on the per-origin limit must not evict IDLE connections
    to force a new connection for its origin. Requests to other origins may
    still evict IDLE connections belonging to other origins as usual.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        max_connections=2,
        max_connections_per_origin=1,
        network_backend=network_backend,
    ) as pool:
        # A completed request leaves an IDLE connection to 'c.com'.
        response = await pool.request("GET", "http://c.com/")
        assert response.status == 200

        # Hold a single active connection to 'a.com'. The pool is now full.
        async with pool.stream("GET", "http://a.com/"):
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['http://c.com:80', HTTP/1.1, IDLE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://a.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
            ]

            # A second request to 'a.com' cannot evict the IDLE connection to
            # 'c.com' in order to force a new connection to 'a.com'.
            with pytest.raises(httpcore.PoolTimeout):
                extensions = {"timeout": {"pool": 0.0001}}
                await pool.request("GET", "http://a.com/", extensions=extensions)

            # The IDLE connection to 'c.com' is still present.
            info = [repr(c) for c in pool.connections]
            assert any("http://c.com:80" in entry for entry in info)

            # A request to 'b.com' may evict the IDLE connection to 'c.com'
            # and establish a new connection.
            response = await pool.request("GET", "http://b.com/")
            assert response.status == 200
            assert response.content == b"Hello, world!"

            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['http://a.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://b.com:80', HTTP/1.1, IDLE, Request Count: 1]>",
            ]


@pytest.mark.anyio
async def test_http11_upgrade_connection():
    """
    HTTP "101 Switching Protocols" indicates an upgraded connection.

    We should return the response, so that the network stream
    may be used for the upgraded connection.

    https://httpwg.org/specs/rfc9110.html#status.101
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/101
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 101 Switching Protocols\r\n",
            b"Connection: upgrade\r\n",
            b"Upgrade: custom\r\n",
            b"\r\n",
            b"...",
        ]
    )

    called = []

    async def trace(name, kwargs):
        called.append(name)

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        async with pool.stream(
            "GET",
            "wss://example.com/",
            headers={"Connection": "upgrade", "Upgrade": "custom"},
            extensions={"trace": trace},
        ) as response:
            assert response.status == 101
            network_stream = response.extensions["network_stream"]
            content = await network_stream.read(max_bytes=1024)
            assert content == b"..."

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.complete",
        "connection.start_tls.started",
        "connection.start_tls.complete",
        "http11.send_request_headers.started",
        "http11.send_request_headers.complete",
        "http11.send_request_body.started",
        "http11.send_request_body.complete",
        "http11.receive_response_headers.started",
        "http11.receive_response_headers.complete",
        "http11.response_closed.started",
        "http11.response_closed.complete",
    ]
