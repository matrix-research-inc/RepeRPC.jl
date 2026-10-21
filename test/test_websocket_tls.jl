# WebSocket TLS and frame-size limits
# Requires: import HTTP and Reseau (loaded by runtests.jl)

# A self-signed certificate for `localhost` and `127.0.0.1`, valid until 2126.
# It is its own trust root, so a client trusts it by naming it as `ca_file`.
const _TLS_FIXTURES = joinpath(@__DIR__, "fixtures")
const _TLS_CERT = joinpath(_TLS_FIXTURES, "localhost.crt")
const _TLS_KEY = joinpath(_TLS_FIXTURES, "localhost.key")

_server_tls() = Reseau.TLS.Config(cert_file = _TLS_CERT, key_file = _TLS_KEY)

function _echo_server(; kwargs...)
    server = RepeRPC.WSServer("127.0.0.1", 0; kwargs...)
    RepeRPC.register(server, "/echo", (params, request) -> params)
    RepeRPC.register(server, "/blob", (params, request) -> Dict("data" => repeat("x", params["n"])))
    RepeRPC.listen(server; async = true)
    return server
end

# Run `f(client)` against a connected client, always tearing both ends down.
function _with_client(f, server, url; kwargs...)
    try
        client = RepeRPC.WSClient(url; kwargs...)
        RepeRPC.connect(client)
        try
            f(client)
        finally
            RepeRPC.disconnect(client)
        end
    finally
        RepeRPC.stop(server)
    end
end

@testset "WebSocket TLS" begin
    @testset "wss round trip with a trusted CA" begin
        server = _echo_server(tls_config = _server_tls())
        @test occursin("wss://", sprint(show, server))
        _with_client(server, "wss://localhost:$(server.port)";
                     tls_config = Reseau.TLS.Config(ca_file = _TLS_CERT)) do client
            @test RepeRPC.send_request(client, "/echo", Dict("x" => 1))["x"] == 1
        end
    end

    @testset "wss without verification" begin
        server = _echo_server(tls_config = _server_tls())
        _with_client(server, "wss://localhost:$(server.port)";
                     require_ssl_verification = false) do client
            @test RepeRPC.send_request(client, "/echo", Dict("x" => 2))["x"] == 2
        end
    end

    @testset "untrusted certificate is refused" begin
        server = _echo_server(tls_config = _server_tls())
        try
            client = RepeRPC.WSClient("wss://localhost:$(server.port)")
            @test_throws Exception RepeRPC.connect(client)
            @test !RepeRPC.isconnected(client)
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "plain client cannot reach a wss server" begin
        server = _echo_server(tls_config = _server_tls())
        try
            client = RepeRPC.WSClient("ws://localhost:$(server.port)"; timeout = 5.0)
            @test_throws Exception RepeRPC.connect(client)
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "tls_config and require_ssl_verification = false conflict" begin
        @test_throws ArgumentError RepeRPC.WSClient("wss://localhost:1";
            tls_config = Reseau.TLS.Config(ca_file = _TLS_CERT),
            require_ssl_verification = false)
    end

    @testset "NodeConfig carries tls_config to its client" begin
        tls = Reseau.TLS.Config(ca_file = _TLS_CERT)
        cfg = RepeRPC.NodeConfig("wss://localhost:8443"; tls_config = tls)
        @test cfg.tls_config === tls
        @test RepeRPC._make_client(cfg).tls_config === tls
        @test_throws ArgumentError RepeRPC.NodeConfig("localhost", 8443; tls_config = tls)
    end
end

@testset "WebSocket frame size" begin
    @testset "defaults to 16 MiB" begin
        server = RepeRPC.WSServer("127.0.0.1", 0)
        client = RepeRPC.WSClient("ws://127.0.0.1:1")
        @test server.max_frame_size == 16 * 1024 * 1024
        @test client.max_frame_size == 16 * 1024 * 1024
    end

    @testset "server closes a connection that sends an oversized frame" begin
        server = _echo_server(max_frame_size = 1024)
        _with_client(server, "ws://127.0.0.1:$(server.port)"; timeout = 10.0) do client
            @test RepeRPC.send_request(client, "/echo", Dict("s" => "small"))["s"] == "small"
            @test_throws RepeRPC.ConnectionError RepeRPC.send_request(
                client, "/echo", Dict("s" => repeat("x", 4096)))
        end
    end

    @testset "client closes on an oversized response" begin
        server = _echo_server()
        _with_client(server, "ws://127.0.0.1:$(server.port)";
                     timeout = 10.0, max_frame_size = 1024) do client
            @test length(RepeRPC.send_request(client, "/blob", Dict("n" => 100))["data"]) == 100
            @test_throws RepeRPC.ConnectionError RepeRPC.send_request(client, "/blob", Dict("n" => 4096))
        end
    end

    @testset "raised limits carry messages past the default" begin
        n = 20 * 1024 * 1024
        limit = 32 * 1024 * 1024
        server = _echo_server(max_frame_size = limit)
        _with_client(server, "ws://127.0.0.1:$(server.port)";
                     timeout = 60.0, max_frame_size = limit) do client
            @test length(RepeRPC.send_request(client, "/blob", Dict("n" => n))["data"]) == n
        end
    end

    @testset "invalid limits are rejected" begin
        @test_throws ArgumentError RepeRPC.WSServer("127.0.0.1", 0; max_frame_size = 0)
        @test_throws ArgumentError RepeRPC.WSClient("ws://127.0.0.1:1"; max_frame_size = -1)
        @test_throws ArgumentError RepeRPC.NodeConfig("ws://127.0.0.1:1"; max_frame_size = 0)
        @test_throws ArgumentError RepeRPC.NodeConfig("127.0.0.1", 1; max_frame_size = 1024)
    end

    @testset "NodeConfig forwards max_frame_size" begin
        @test RepeRPC._make_client(RepeRPC.NodeConfig("ws://127.0.0.1:1")).max_frame_size ==
              16 * 1024 * 1024
        @test RepeRPC._make_client(
            RepeRPC.NodeConfig("ws://127.0.0.1:1"; max_frame_size = 4096)).max_frame_size == 4096
    end
end
