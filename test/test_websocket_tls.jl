# WebSocket TLS, server lifecycle, and frame-size limits
# Requires: import HTTP and Reseau (loaded by runtests.jl)

# A self-signed certificate for `localhost` and `127.0.0.1`, valid until 2126.
# It is its own trust root, so a client trusts it by naming it as `ca_file`.
const _TLS_FIXTURES = joinpath(@__DIR__, "fixtures")
const _TLS_CERT = joinpath(_TLS_FIXTURES, "localhost.crt")
const _TLS_KEY = joinpath(_TLS_FIXTURES, "localhost.key")

const _MAX_FRAME_SIZE = RepeRPC._WS_DEFAULT_MAX_FRAME_SIZE

_server_tls() = Reseau.TLS.Config(cert_file = _TLS_CERT, key_file = _TLS_KEY)

# Run `f(server)` against a listening echo server, always stopping it.
function _with_server(f; kwargs...)
    server = RepeRPC.WSServer("127.0.0.1", 0; kwargs...)
    RepeRPC.register(server, "/echo", (params, request) -> params)
    RepeRPC.register(server, "/blob", (params, request) -> Dict("data" => repeat("x", params["n"])))
    RepeRPC.listen(server; async = true)
    try
        f(server)
    finally
        RepeRPC.stop(server)
    end
end

# Run `f(client)` against a connected client of a fresh server.
function _with_client(f, url_for; server_kwargs = (;), kwargs...)
    _with_server(; server_kwargs...) do server
        client = RepeRPC.WSClient(url_for(server); kwargs...)
        RepeRPC.connect(client)
        try
            f(client)
        finally
            RepeRPC.disconnect(client)
        end
    end
end

_wss(server) = "wss://localhost:$(server.port)"
_ws(server) = "ws://127.0.0.1:$(server.port)"

@testset "WebSocket TLS" begin
    @testset "wss round trip with a trusted CA" begin
        _with_client(_wss; server_kwargs = (tls_config = _server_tls(),),
                     tls_config = Reseau.TLS.Config(ca_file = _TLS_CERT)) do client
            @test RepeRPC.send_request(client, "/echo", Dict("x" => 1))["x"] == 1
        end
    end

    @testset "wss without verification" begin
        _with_client(_wss; server_kwargs = (tls_config = _server_tls(),),
                     require_ssl_verification = false) do client
            @test RepeRPC.send_request(client, "/echo", Dict("x" => 2))["x"] == 2
        end
    end

    @testset "untrusted certificate is refused" begin
        _with_server(tls_config = _server_tls()) do server
            @test occursin("wss://", sprint(show, server))
            client = RepeRPC.WSClient(_wss(server))
            err = try
                RepeRPC.connect(client)
                nothing
            catch e
                e
            end
            @test occursin("certificate", lowercase(sprint(showerror, err)))
            @test !RepeRPC.isconnected(client)
        end
    end

    @testset "plain client cannot reach a wss server" begin
        _with_server(tls_config = _server_tls()) do server
            client = RepeRPC.WSClient("ws://localhost:$(server.port)"; timeout = 5.0)
            @test_throws Exception RepeRPC.connect(client)
        end
    end
end

@testset "WebSocket server lifecycle" begin
    @testset "a second listen is refused and leaves the server running" begin
        _with_server() do server
            @test_throws ArgumentError RepeRPC.listen(server; async = true)
            @test server.running
            client = RepeRPC.connect(RepeRPC.WSClient(_ws(server)))
            try
                @test RepeRPC.send_request(client, "/echo", Dict("x" => 3))["x"] == 3
            finally
                RepeRPC.disconnect(client)
            end
        end
    end

    @testset "a blocking listen stops the server when interrupted" begin
        server = RepeRPC.WSServer("127.0.0.1", 0)
        task = @async RepeRPC.listen(server)
        timedwait(() -> server.server_handle !== nothing, 10.0)
        sleep(0.2)  # let the task reach its wait
        @test server.running
        schedule(task, InterruptException(); error = true)
        @test_throws TaskFailedException wait(task)
        @test !server.running
        @test server.server_handle === nothing
    end

    @testset "IPv6 literals are bracketed in URLs" begin
        server = RepeRPC.WSServer("::1", 8080)
        @test occursin("ws://[::1]:8080", sprint(show, server))
        @test RepeRPC.WSClient("::1", 8080).url == "ws://[::1]:8080"
    end
end

@testset "WebSocket frame size" begin
    @testset "defaults to HTTP.jl's frame size" begin
        @test _MAX_FRAME_SIZE == HTTP.WebSockets.DEFAULT_MAX_FRAME_SIZE
        @test RepeRPC.WSServer("127.0.0.1", 0).max_frame_size == _MAX_FRAME_SIZE
        @test RepeRPC.WSClient("ws://127.0.0.1:1").max_frame_size == _MAX_FRAME_SIZE
        @test RepeRPC._make_client(RepeRPC.NodeConfig("ws://127.0.0.1:1")).max_frame_size ==
              _MAX_FRAME_SIZE
    end

    @testset "NodeConfig forwards max_frame_size" begin
        cfg = RepeRPC.NodeConfig("ws://127.0.0.1:1"; max_frame_size = 4096)
        @test RepeRPC._make_client(cfg).max_frame_size == 4096
    end

    @testset "server closes a connection that sends an oversized frame" begin
        _with_client(_ws; server_kwargs = (max_frame_size = 1024,), timeout = 10.0) do client
            @test RepeRPC.send_request(client, "/echo", Dict("s" => "small"))["s"] == "small"
            @test_throws RepeRPC.ConnectionError RepeRPC.send_request(
                client, "/echo", Dict("s" => repeat("x", 4096)))
        end
    end

    @testset "client closes on an oversized response" begin
        _with_client(_ws; timeout = 10.0, max_frame_size = 1024) do client
            @test length(RepeRPC.send_request(client, "/blob", Dict("n" => 100))["data"]) == 100
            @test_throws RepeRPC.ConnectionError RepeRPC.send_request(client, "/blob", Dict("n" => 4096))
        end
    end

    @testset "raised limits carry messages past the default" begin
        n = 20 * 1024 * 1024
        limit = 32 * 1024 * 1024
        _with_client(_ws; server_kwargs = (max_frame_size = limit,),
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
end
