# Auth and Fleet-over-WebSocket tests for RepeRPC.jl
# Requires: import HTTP (loaded by runtests.jl to trigger extension)

using Test
using RepeRPC
using Sockets

# Ask the kernel for an unused TCP port. There is a tiny race between the
# close() and the caller's listen() that the tests below tolerate; it is
# strictly better than hardcoding a port that may collide with concurrent
# test runs or unrelated processes.
function _find_free_tcp_port()
    sock = Sockets.listen(Sockets.IPv4(0), 0)
    port = Int(Sockets.getsockname(sock)[2])
    close(sock)
    return port
end

@testset "WebSocket Auth and WSS Fleet" begin

    #==========================================================================#
    # _parse_ws_url: internal URL parser used by NodeConfig(url; ...).
    #==========================================================================#
    @testset "_parse_ws_url" begin
        @testset "ws:// with explicit port" begin
            p = RepeRPC._parse_ws_url("ws://localhost:8080")
            @test p.host == "localhost"
            @test p.port == 8080
            @test p.scheme == "ws"
        end

        @testset "wss:// with explicit port" begin
            p = RepeRPC._parse_ws_url("wss://example.com:8443")
            @test p.host == "example.com"
            @test p.port == 8443
            @test p.scheme == "wss"
        end

        @testset "wss:// default port 443" begin
            p = RepeRPC._parse_ws_url("wss://example.com")
            @test p.host == "example.com"
            @test p.port == 443
            @test p.scheme == "wss"
        end

        @testset "ws:// default port 80" begin
            p = RepeRPC._parse_ws_url("ws://example.com")
            @test p.host == "example.com"
            @test p.port == 80
            @test p.scheme == "ws"
        end

        @testset "URL with path strips path from host" begin
            p = RepeRPC._parse_ws_url("wss://example.com:8443/repe/v1")
            @test p.host == "example.com"
            @test p.port == 8443
        end

        @testset "URL with query string" begin
            p = RepeRPC._parse_ws_url("wss://example.com?token=abc")
            @test p.host == "example.com"
            @test p.port == 443
        end

        @testset "Missing scheme rejected" begin
            @test_throws ArgumentError RepeRPC._parse_ws_url("example.com:8080")
            @test_throws ArgumentError RepeRPC._parse_ws_url("https://example.com")
        end

        @testset "Missing host rejected" begin
            @test_throws ArgumentError RepeRPC._parse_ws_url("wss://")
            @test_throws ArgumentError RepeRPC._parse_ws_url("ws:///path")
        end

        @testset "Invalid port rejected" begin
            @test_throws ArgumentError RepeRPC._parse_ws_url("ws://localhost:abc")
        end
    end

    #==========================================================================#
    # NodeConfig URL-style constructor
    #==========================================================================#
    @testset "NodeConfig with URL" begin
        @testset "wss URL populates fields" begin
            cfg = RepeRPC.NodeConfig("wss://data-1.example.com:8443/repe";
                                  auth_token = "secret-123",
                                  tags       = ["team"])
            @test cfg.url == "wss://data-1.example.com:8443/repe"
            @test cfg.host == "data-1.example.com"
            @test cfg.port == 8443
            @test cfg.auth_token == "secret-123"
            @test cfg.tags == ["team"]
            @test cfg.require_ssl_verification == true
            # name defaults to the URL when not provided
            @test cfg.name == "wss://data-1.example.com:8443/repe"
        end

        @testset "Default require_ssl_verification is true" begin
            cfg = RepeRPC.NodeConfig("wss://example.com")
            @test cfg.require_ssl_verification == true
        end

        @testset "Disable ssl verification" begin
            cfg = RepeRPC.NodeConfig("wss://example.com"; require_ssl_verification = false)
            @test cfg.require_ssl_verification == false
        end

        @testset "Custom name" begin
            cfg = RepeRPC.NodeConfig("ws://10.0.0.5:8080"; name = "edge-1")
            @test cfg.name == "edge-1"
        end

        @testset "host/port constructor retains backward compat" begin
            cfg = RepeRPC.NodeConfig("localhost", 8080)
            @test cfg.url === nothing
            @test cfg.auth_token === nothing
            @test cfg.sslconfig === nothing
            @test cfg.require_ssl_verification == true
        end
    end

    #==========================================================================#
    # Constant-time string comparison used for bearer token check.
    # NOTE: this testset verifies correctness only. The timing property of
    # the comparison (no early exit during the byte loop) is enforced by
    # construction in the implementation; timing assertions in CI are
    # unreliable and intentionally not made here.
    #==========================================================================#
    @testset "_constant_time_eq" begin
        ext = Base.get_extension(RepeRPC, :RepeRPCHTTPExt)
        @test ext !== nothing
        eq = ext._constant_time_eq
        @test eq("hello", "hello") == true
        @test eq("hello", "world") == false
        @test eq("", "") == true
        @test eq("a", "ab") == false
        @test eq("ab", "a") == false
        @test eq("token-with-unicode-é", "token-with-unicode-é") == true
    end

    #==========================================================================#
    # Bearer token auth: end-to-end over plain ws://.
    #==========================================================================#
    @testset "Bearer token auth" begin
        token = "super-secret-token-123"

        @testset "Correct token connects and calls" begin
            server = RepeRPC.WSServer("127.0.0.1", 0; auth_token = token)
            RepeRPC.register(server, "/echo", (params, request) -> params)
            RepeRPC.listen(server; async = true)
            try
                client = RepeRPC.WSClient("ws://127.0.0.1:$(server.port)";
                                       auth_token = token)
                RepeRPC.connect(client)
                try
                    result = RepeRPC.send_request(client, "/echo", Dict("v" => 42))
                    @test result["v"] == 42
                finally
                    RepeRPC.disconnect(client)
                end
            finally
                RepeRPC.stop(server)
            end
        end

        @testset "Missing token rejected at upgrade" begin
            server = RepeRPC.WSServer("127.0.0.1", 0; auth_token = token)
            RepeRPC.register(server, "/echo", (params, request) -> params)
            RepeRPC.listen(server; async = true)
            try
                client = RepeRPC.WSClient("ws://127.0.0.1:$(server.port)")
                # HTTP.WebSockets.open raises when the server returns 401.
                @test_throws Exception RepeRPC.connect(client)
                @test !RepeRPC.isconnected(client)
            finally
                RepeRPC.stop(server)
            end
        end

        @testset "Wrong token rejected at upgrade" begin
            server = RepeRPC.WSServer("127.0.0.1", 0; auth_token = token)
            RepeRPC.register(server, "/echo", (params, request) -> params)
            RepeRPC.listen(server; async = true)
            try
                client = RepeRPC.WSClient("ws://127.0.0.1:$(server.port)";
                                       auth_token = "wrong-token")
                @test_throws Exception RepeRPC.connect(client)
                @test !RepeRPC.isconnected(client)
            finally
                RepeRPC.stop(server)
            end
        end

        @testset "Token on unprotected server is accepted (ignored)" begin
            server = RepeRPC.WSServer("127.0.0.1", 0)
            RepeRPC.register(server, "/ping", (params, request) -> "pong")
            RepeRPC.listen(server; async = true)
            try
                client = RepeRPC.WSClient("ws://127.0.0.1:$(server.port)";
                                       auth_token = "any-token")
                RepeRPC.connect(client)
                try
                    @test RepeRPC.send_request(client, "/ping", nothing) == "pong"
                finally
                    RepeRPC.disconnect(client)
                end
            finally
                RepeRPC.stop(server)
            end
        end
    end

    #==========================================================================#
    # Custom auth_verifier: server-side callback that inspects the HTTP request.
    #==========================================================================#
    @testset "Custom auth_verifier" begin
        # Allow requests carrying X-Team header matching an expected value.
        verifier = function(http)
            headers = http.message.headers
            for (k, v) in headers
                if lowercase(String(k)) == "x-team" && String(v) == "alpha"
                    return true
                end
            end
            return false
        end

        @testset "Verifier accepts matching header" begin
            server = RepeRPC.WSServer("127.0.0.1", 0; auth_verifier = verifier)
            RepeRPC.register(server, "/whoami", (params, request) -> "alpha")
            RepeRPC.listen(server; async = true)
            try
                client = RepeRPC.WSClient("ws://127.0.0.1:$(server.port)";
                                       extra_headers = ["X-Team" => "alpha"])
                RepeRPC.connect(client)
                try
                    @test RepeRPC.send_request(client, "/whoami", nothing) == "alpha"
                finally
                    RepeRPC.disconnect(client)
                end
            finally
                RepeRPC.stop(server)
            end
        end

        @testset "Verifier rejects on mismatch" begin
            server = RepeRPC.WSServer("127.0.0.1", 0; auth_verifier = verifier)
            RepeRPC.register(server, "/whoami", (params, request) -> "alpha")
            RepeRPC.listen(server; async = true)
            try
                client = RepeRPC.WSClient("ws://127.0.0.1:$(server.port)";
                                       extra_headers = ["X-Team" => "beta"])
                @test_throws Exception RepeRPC.connect(client)
            finally
                RepeRPC.stop(server)
            end
        end

        @testset "Verifier swallows thrown errors as rejection" begin
            throwing_verifier = function(_)
                error("boom")
            end
            server = RepeRPC.WSServer("127.0.0.1", 0; auth_verifier = throwing_verifier)
            RepeRPC.register(server, "/x", (params, request) -> "ok")
            RepeRPC.listen(server; async = true)
            try
                client = RepeRPC.WSClient("ws://127.0.0.1:$(server.port)")
                @test_throws Exception RepeRPC.connect(client)
            finally
                RepeRPC.stop(server)
            end
        end

        @testset "auth_verifier takes precedence over auth_token" begin
            # When both are set, the verifier wins. Here the verifier always
            # returns true so any token (or none) is accepted.
            always_true = function(_)
                return true
            end
            server = RepeRPC.WSServer("127.0.0.1", 0;
                                   auth_token = "ignored",
                                   auth_verifier = always_true)
            RepeRPC.register(server, "/p", (params, request) -> "ok")
            RepeRPC.listen(server; async = true)
            try
                client = RepeRPC.WSClient("ws://127.0.0.1:$(server.port)")
                RepeRPC.connect(client)
                try
                    @test RepeRPC.send_request(client, "/p", nothing) == "ok"
                finally
                    RepeRPC.disconnect(client)
                end
            finally
                RepeRPC.stop(server)
            end
        end
    end

    #==========================================================================#
    # Fleet over WebSocket transport.
    #==========================================================================#
    @testset "Fleet over WebSocket" begin
        token = "fleet-token-XYZ"

        # Bring up two authenticated WS servers on ephemeral ports.
        srv_a = RepeRPC.WSServer("127.0.0.1", 0; auth_token = token)
        srv_b = RepeRPC.WSServer("127.0.0.1", 0; auth_token = token)
        RepeRPC.register(srv_a, "/who", (params, request) -> "a")
        RepeRPC.register(srv_b, "/who", (params, request) -> "b")
        RepeRPC.register(srv_a, "/echo", (params, request) -> params)
        RepeRPC.register(srv_b, "/echo", (params, request) -> params)
        RepeRPC.listen(srv_a; async = true)
        RepeRPC.listen(srv_b; async = true)

        try
            configs = [
                RepeRPC.NodeConfig("ws://127.0.0.1:$(srv_a.port)";
                                name = "a", auth_token = token, tags = ["team"]),
                RepeRPC.NodeConfig("ws://127.0.0.1:$(srv_b.port)";
                                name = "b", auth_token = token, tags = ["team"]),
            ]
            fleet = RepeRPC.Fleet(configs)

            @testset "Node uses WSClient when url is set" begin
                node_a = fleet["a"]
                ext = Base.get_extension(RepeRPC, :RepeRPCHTTPExt)
                @test node_a.client isa ext._WSClient
                @test node_a.client.auth_token == token
                @test node_a.client isa RepeRPC.AbstractClient
            end

            status = RepeRPC.connect!(fleet)
            @test sort(status.connected) == ["a", "b"]
            @test isempty(status.failed)

            try
                @testset "Broadcast over WSS-capable fleet" begin
                    results = RepeRPC.broadcast(fleet, "/who", nothing)
                    @test results["a"].value == "a"
                    @test results["b"].value == "b"
                    @test RepeRPC.succeeded(results["a"])
                    @test RepeRPC.succeeded(results["b"])
                end

                @testset "Tag filtering over WebSocket fleet" begin
                    results = RepeRPC.broadcast(fleet, "/echo", Dict("n" => 7);
                                             tags = ["team"])
                    @test length(results) == 2
                    @test results["a"].value["n"] == 7
                    @test results["b"].value["n"] == 7
                end

                @testset "Single-node call over WebSocket fleet" begin
                    r = RepeRPC.call(fleet, "a", "/echo", Dict("k" => "v"))
                    @test RepeRPC.succeeded(r)
                    @test r.value["k"] == "v"
                end
            finally
                RepeRPC.disconnect!(fleet)
            end
        finally
            RepeRPC.stop(srv_a)
            RepeRPC.stop(srv_b)
        end
    end

    #==========================================================================#
    # Fleet refuses to authenticate when the token is wrong.
    # NOTE: this test asserts only the (connected, failed) status from
    # connect!. The companion "Auth failure is not retried" testset below
    # asserts that the failure is non-retryable, which is what makes
    # connect! return promptly rather than hammering the server with
    # retries. Keep these two in sync.
    #==========================================================================#
    @testset "Fleet rejects on bad token" begin
        srv = RepeRPC.WSServer("127.0.0.1", 0; auth_token = "right")
        RepeRPC.register(srv, "/x", (params, request) -> "ok")
        RepeRPC.listen(srv; async = true)

        try
            fleet = RepeRPC.Fleet([
                RepeRPC.NodeConfig("ws://127.0.0.1:$(srv.port)";
                                name = "bad", auth_token = "wrong"),
            ])
            status = RepeRPC.connect!(fleet)
            @test status.connected == String[]
            @test status.failed == ["bad"]
        finally
            RepeRPC.stop(srv)
        end
    end

    #==========================================================================#
    # sslconfig plumbing: verify it round-trips into the client and server
    # structs without exercising real TLS (cert generation is out of scope).
    #==========================================================================#
    @testset "sslconfig plumbing" begin
        sentinel = (marker = :tls_test,)

        srv = RepeRPC.WSServer("127.0.0.1", 0; sslconfig = sentinel)
        @test srv.sslconfig === sentinel

        client = RepeRPC.WSClient("wss://example.com"; sslconfig = sentinel,
                                                     require_ssl_verification = false)
        ext = Base.get_extension(RepeRPC, :RepeRPCHTTPExt)
        @test client isa ext._WSClient
        @test client.sslconfig === sentinel
        @test client.require_ssl_verification == false

        # Fleet should forward sslconfig from NodeConfig into the WSClient.
        cfg = RepeRPC.NodeConfig("wss://example.com"; sslconfig = sentinel,
                                                    require_ssl_verification = false,
                                                    name = "tls-node")
        fleet = RepeRPC.Fleet([cfg])
        node = fleet["tls-node"]
        @test node.client isa ext._WSClient
        @test node.client.sslconfig === sentinel
        @test node.client.require_ssl_verification == false
    end

    #==========================================================================#
    # Mixed transport Fleet: one TCP node and one WSS-auth node side by side.
    # Verifies AbstractClient polymorphism end-to-end across Fleet operations.
    #==========================================================================#
    @testset "Mixed TCP + WSS Fleet" begin
        token = "mixed-fleet-token"
        # RepeRPC.Server (TCP) does not currently update server.port after
        # binding to port 0, so we ask the kernel for an unused port and
        # hand it to the server explicitly.
        tcp_port = _find_free_tcp_port()

        tcp_server = RepeRPC.Server("127.0.0.1", tcp_port)
        RepeRPC.register(tcp_server, "/who", function(params, request)
            return Dict("transport" => "tcp")
        end)
        RepeRPC.listen(tcp_server; async = true)
        RepeRPC.wait_for_server("127.0.0.1", tcp_port)

        ws_server = RepeRPC.WSServer("127.0.0.1", 0; auth_token = token)
        RepeRPC.register(ws_server, "/who", function(params, request)
            return Dict("transport" => "ws")
        end)
        RepeRPC.listen(ws_server; async = true)

        try
            configs = [
                RepeRPC.NodeConfig("127.0.0.1", tcp_port; name = "tcp-node",
                                tags = ["lan"]),
                RepeRPC.NodeConfig("ws://127.0.0.1:$(ws_server.port)";
                                name = "ws-node",
                                auth_token = token,
                                tags = ["wan"]),
            ]
            fleet = RepeRPC.Fleet(configs)
            ext = Base.get_extension(RepeRPC, :RepeRPCHTTPExt)

            @testset "Node clients have distinct concrete types" begin
                @test fleet["tcp-node"].client isa RepeRPC.Client
                @test fleet["ws-node"].client isa ext._WSClient
                # Both still satisfy the abstract supertype.
                @test fleet["tcp-node"].client isa RepeRPC.AbstractClient
                @test fleet["ws-node"].client isa RepeRPC.AbstractClient
            end

            status = RepeRPC.connect!(fleet)
            @test sort(status.connected) == ["tcp-node", "ws-node"]
            @test isempty(status.failed)

            try
                @testset "Broadcast across transports" begin
                    results = RepeRPC.broadcast(fleet, "/who", nothing)
                    @test RepeRPC.succeeded(results["tcp-node"])
                    @test RepeRPC.succeeded(results["ws-node"])
                    @test results["tcp-node"].value["transport"] == "tcp"
                    @test results["ws-node"].value["transport"] == "ws"
                end

                @testset "Tag filter selects only matching transport" begin
                    wan_only = RepeRPC.broadcast(fleet, "/who", nothing; tags = ["wan"])
                    @test collect(keys(wan_only)) == ["ws-node"]
                    lan_only = RepeRPC.broadcast(fleet, "/who", nothing; tags = ["lan"])
                    @test collect(keys(lan_only)) == ["tcp-node"]
                end

                @testset "Single-node call hits the right transport" begin
                    r_tcp = RepeRPC.call(fleet, "tcp-node", "/who", nothing)
                    r_ws  = RepeRPC.call(fleet, "ws-node",  "/who", nothing)
                    @test r_tcp.value["transport"] == "tcp"
                    @test r_ws.value["transport"]  == "ws"
                end
            finally
                RepeRPC.disconnect!(fleet)
            end
        finally
            RepeRPC.stop(tcp_server)
            RepeRPC.stop(ws_server)
        end
    end

    #==========================================================================#
    # Fleet helpers (health_check, map_reduce, reconnect!) over WebSocket nodes.
    #==========================================================================#
    @testset "Fleet helpers over WebSocket" begin
        token = "helpers-token"
        srv_a = RepeRPC.WSServer("127.0.0.1", 0; auth_token = token)
        srv_b = RepeRPC.WSServer("127.0.0.1", 0; auth_token = token)
        RepeRPC.register(srv_a, "/status", (params, request) -> Dict("ok" => true))
        RepeRPC.register(srv_b, "/status", (params, request) -> Dict("ok" => true))
        RepeRPC.register(srv_a, "/double",
                      (params, request) -> Dict("result" => params["x"] * 2))
        RepeRPC.register(srv_b, "/double",
                      (params, request) -> Dict("result" => params["x"] * 2))
        RepeRPC.listen(srv_a; async = true)
        RepeRPC.listen(srv_b; async = true)

        try
            configs = [
                RepeRPC.NodeConfig("ws://127.0.0.1:$(srv_a.port)";
                                name = "a", auth_token = token),
                RepeRPC.NodeConfig("ws://127.0.0.1:$(srv_b.port)";
                                name = "b", auth_token = token),
            ]
            fleet = RepeRPC.Fleet(configs)
            RepeRPC.connect!(fleet)

            try
                @testset "health_check over WS" begin
                    results = RepeRPC.health_check(fleet; health_endpoint = "/status")
                    @test results["a"].healthy
                    @test results["b"].healthy
                    @test results["a"].error === nothing
                    @test results["b"].error === nothing
                    @test results["a"].latency > 0
                    @test results["b"].latency > 0
                end

                @testset "map_reduce over WS" begin
                    total = RepeRPC.map_reduce(fleet, "/double", Dict("x" => 5)) do results
                        sum(r.value["result"] for r in results if RepeRPC.succeeded(r))
                    end
                    # 5 doubled on each of two nodes.
                    @test total == 20
                end

                @testset "reconnect! over WS" begin
                    RepeRPC.disconnect!(fleet)
                    @test !RepeRPC.isconnected(fleet)
                    status = RepeRPC.reconnect!(fleet)
                    @test sort(status.reconnected) == ["a", "b"]
                    @test isempty(status.failed)
                    @test RepeRPC.isconnected(fleet)
                    # Sanity: post-reconnect call still works.
                    r = RepeRPC.call(fleet, "a", "/status", nothing)
                    @test RepeRPC.succeeded(r)
                end
            finally
                RepeRPC.disconnect!(fleet)
            end
        finally
            RepeRPC.stop(srv_a)
            RepeRPC.stop(srv_b)
        end
    end

    @testset "add_node! / remove_node! with WS" begin
        token = "dynamic-token"
        srv = RepeRPC.WSServer("127.0.0.1", 0; auth_token = token)
        RepeRPC.register(srv, "/ping", (params, request) -> "pong")
        RepeRPC.listen(srv; async = true)
        ext = Base.get_extension(RepeRPC, :RepeRPCHTTPExt)

        try
            fleet = RepeRPC.Fleet()
            @test length(fleet) == 0

            RepeRPC.add_node!(fleet,
                RepeRPC.NodeConfig("ws://127.0.0.1:$(srv.port)";
                                name = "ws", auth_token = token))
            @test length(fleet) == 1
            @test fleet["ws"].client isa ext._WSClient

            RepeRPC.connect!(fleet)
            try
                r = RepeRPC.call(fleet, "ws", "/ping", nothing)
                @test RepeRPC.succeeded(r)
                @test r.value == "pong"
            finally
                RepeRPC.disconnect!(fleet)
            end

            RepeRPC.remove_node!(fleet, "ws")
            @test length(fleet) == 0
        finally
            RepeRPC.stop(srv)
        end
    end

    #==========================================================================#
    # send_notify over an authenticated WebSocket connection.
    #==========================================================================#
    @testset "send_notify over authenticated WebSocket" begin
        token = "notify-token"

        @testset "Authorized notify reaches handler" begin
            received = Ref(false)
            received_val = Ref{Any}(nothing)

            srv = RepeRPC.WSServer("127.0.0.1", 0; auth_token = token)
            RepeRPC.register(srv, "/notify_handler", function(params, request)
                received[]     = true
                received_val[] = params["value"]
                return Dict("ok" => true)
            end)
            RepeRPC.listen(srv; async = true)

            try
                client = RepeRPC.WSClient("ws://127.0.0.1:$(srv.port)";
                                       auth_token = token)
                RepeRPC.connect(client)
                try
                    RepeRPC.send_notify(client, "/notify_handler",
                                     Dict("value" => 42))
                    # Notifications are fire-and-forget. Poll briefly.
                    deadline = time() + 2.0
                    while !received[] && time() < deadline
                        sleep(0.05)
                    end
                    @test received[] == true
                    @test received_val[] == 42
                finally
                    RepeRPC.disconnect(client)
                end
            finally
                RepeRPC.stop(srv)
            end
        end

        @testset "Wrong-token client never reaches notify path" begin
            srv = RepeRPC.WSServer("127.0.0.1", 0; auth_token = token)
            RepeRPC.register(srv, "/notify_handler",
                          (params, request) -> Dict("ok" => true))
            RepeRPC.listen(srv; async = true)

            try
                client = RepeRPC.WSClient("ws://127.0.0.1:$(srv.port)";
                                       auth_token = "wrong-notify")
                # Auth gate runs at upgrade, so connect() throws before any
                # message frame can be sent. send_notify is therefore
                # unreachable for an unauthorized caller.
                @test_throws Exception RepeRPC.connect(client)
                @test !RepeRPC.isconnected(client)
            finally
                RepeRPC.stop(srv)
            end
        end
    end

    #==========================================================================#
    # Retry classification: HTTP-layer auth rejection must not trigger the
    # transport-error retry loop, otherwise a misconfigured token would
    # multiply server-side 401s and inflate elapsed time per call.
    #==========================================================================#
    @testset "Auth failure is not retried" begin
        srv = RepeRPC.WSServer("127.0.0.1", 0; auth_token = "right")
        RepeRPC.register(srv, "/x", (params, request) -> "ok")
        RepeRPC.listen(srv; async = true)

        try
            # Configure a fleet with the WRONG token and a deliberately long
            # retry delay. If 401s were retried we would see roughly
            # (max_attempts - 1) * retry_delay seconds of sleep per call. With
            # the whitelist-based classifier, the failure short-circuits and
            # the call returns immediately.
            fleet = RepeRPC.Fleet(
                [RepeRPC.NodeConfig("ws://127.0.0.1:$(srv.port)";
                                 name = "bad", auth_token = "wrong")];
                retry_delay = 2.0,
                max_retry_attempts = 3,
            )

            # connect! reports failure without retry storm (fast path).
            t0 = time()
            status = RepeRPC.connect!(fleet)
            connect_elapsed = time() - t0
            @test status.connected == String[]
            @test status.failed == ["bad"]
            @test connect_elapsed < 1.5

            # broadcast also runs through _call_with_retry, which is where the
            # retry classification actually fires. With 3 attempts and a 2 s
            # delay a retry storm would take roughly 4 s; non-retryable
            # classification should keep it well under 1.5 s.
            t1 = time()
            results = RepeRPC.broadcast(fleet, "/x", nothing)
            broadcast_elapsed = time() - t1
            @test RepeRPC.failed(results["bad"])
            @test broadcast_elapsed < 1.5
            # The recorded error class should be an HTTP/transport exception,
            # not a REPE TimeoutError or REPEError (which would mean the
            # request had reached server-side handler logic).
            err = results["bad"].error
            @test err !== nothing
            @test !(err isa RepeRPC.TimeoutError)
            @test !(err isa RepeRPC.REPEError)
        finally
            RepeRPC.stop(srv)
        end
    end

    #==========================================================================#
    # Construction-time validation: empty tokens, conflicting config, IPv6.
    #==========================================================================#
    @testset "Construction-time validation" begin
        @testset "Empty auth_token rejected by NodeConfig" begin
            @test_throws ArgumentError RepeRPC.NodeConfig("wss://example.com";
                                                      auth_token = "")
            @test_throws ArgumentError RepeRPC.NodeConfig("localhost", 8080;
                                                      auth_token = "")
        end

        @testset "Empty auth_token rejected by WSServer" begin
            @test_throws ArgumentError RepeRPC.WSServer("127.0.0.1", 0;
                                                    auth_token = "")
        end

        @testset "Empty auth_token rejected by WSClient" begin
            @test_throws ArgumentError RepeRPC.WSClient("ws://127.0.0.1:8080";
                                                    auth_token = "")
        end

        @testset "auth_token without url is rejected" begin
            @test_throws ArgumentError RepeRPC.NodeConfig("localhost", 8080;
                                                      auth_token = "secret")
        end

        @testset "sslconfig without url is rejected" begin
            @test_throws ArgumentError RepeRPC.NodeConfig("localhost", 8080;
                                                      sslconfig = (marker = :ssl,))
        end

        @testset "require_ssl_verification=false without url is rejected" begin
            @test_throws ArgumentError RepeRPC.NodeConfig("localhost", 8080;
                                                      require_ssl_verification = false)
        end

        @testset "TCP NodeConfig with default security kwargs still works" begin
            # The validation must only fire when a security kwarg is
            # actually set, not when the user accepts the defaults.
            cfg = RepeRPC.NodeConfig("localhost", 8080)
            @test cfg.url === nothing
            @test cfg.auth_token === nothing
            @test cfg.sslconfig === nothing
            @test cfg.require_ssl_verification == true
        end

        @testset "IPv6 bracketed URL rejected by parser" begin
            @test_throws ArgumentError RepeRPC._parse_ws_url("wss://[::1]:8443")
            @test_throws ArgumentError RepeRPC._parse_ws_url("ws://[2001:db8::1]:8080")
            @test_throws ArgumentError RepeRPC.NodeConfig("wss://[::1]:8443")
        end

        @testset "Authorization collision in extra_headers rejected" begin
            # auth_token + an Authorization entry in extra_headers would
            # put two Authorization headers on the wire.
            @test_throws ArgumentError RepeRPC.WSClient("ws://127.0.0.1:8080";
                auth_token    = "tok",
                extra_headers = ["Authorization" => "Bearer other"])
            # Case-insensitive: same rejection for lowercased header name.
            @test_throws ArgumentError RepeRPC.WSClient("ws://127.0.0.1:8080";
                auth_token    = "tok",
                extra_headers = ["authorization" => "Bearer other"])
            # Either alone is fine.
            c1 = RepeRPC.WSClient("ws://127.0.0.1:8080"; auth_token = "tok")
            ext = Base.get_extension(RepeRPC, :RepeRPCHTTPExt)
            @test c1 isa ext._WSClient
            c2 = RepeRPC.WSClient("ws://127.0.0.1:8080";
                extra_headers = ["Authorization" => "Bearer literal"])
            @test c2 isa ext._WSClient
        end

        @testset "Oversized Authorization header is ignored by server" begin
            # A malicious client could send an enormous Authorization
            # header. _extract_bearer_token should refuse to parse it.
            # Use the internal helper directly with a synthetic Stream-like
            # object instead of spinning up a real server.
            ext = Base.get_extension(RepeRPC, :RepeRPCHTTPExt)
            big = "Bearer " * repeat("a", 16384)  # 16 KB
            fake_req = HTTP.Request("GET", "/", ["Authorization" => big], "")
            fake_http = (message = fake_req,)
            @test ext._extract_bearer_token(fake_http) === nothing

            # Sanity: a normal-size header still parses.
            small = "Bearer tok"
            ok_req = HTTP.Request("GET", "/", ["Authorization" => small], "")
            ok_http = (message = ok_req,)
            @test ext._extract_bearer_token(ok_http) == "tok"
        end
    end
end
