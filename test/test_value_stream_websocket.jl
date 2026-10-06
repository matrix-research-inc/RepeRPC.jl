# SVS value-stream parity over the WebSocket transport. The same pull driver and
# producer drive a WSClient/WSServer: pull is plain request/response, and both
# transports dispatch through the shared `_process_request_pipeline`, so this
# should be free. Requires HTTP (loaded by runtests.jl) and CodecZstd.
import CodecZstd

@testset "Value Stream (SVS) over WebSocket" begin
    function _with_ws_svs(f, resolve; opts)
        server = RepeRPC.WSServer("127.0.0.1", 0)
        RepeRPC.serve_value_stream(server, resolve; opts = opts)
        RepeRPC.listen(server; async = true)
        sleep(0.3)
        client = RepeRPC.WSClient("127.0.0.1", server.port)
        RepeRPC.connect(client)
        try
            f(client)
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end

    for comp in (:none, :zstd)
        @testset "pull_value generic over WebSocket ($comp)" begin
            _with_ws_svs(_svs_resolve; opts = _svs_opts(comp)) do client
                v = RepeRPC.pull_value(client, "dict")
                @test v["samples"] == _svs_dict()["samples"]
                @test v["tags"] == _svs_dict()["tags"]
            end
        end
    end

    @testset "pull_value typed over WebSocket (zstd)" begin
        _with_ws_svs(_svs_resolve; opts = _svs_opts(:zstd)) do client
            v = RepeRPC.pull_value(SvsTyped, client, "typed")
            @test v.samples == _svs_typed().samples
            @test v.label == _svs_typed().label
        end
    end

    @testset "file mode over WebSocket (zstd)" begin
        _with_ws_svs(_svs_resolve; opts = _svs_opts(:zstd)) do client
            mktempdir() do dir
                p = joinpath(dir, "ws.beve")
                RepeRPC.pull_to_beve_file(client, "dict", p)
                @test BeveFormat.read_beve_file(p)["samples"] == _svs_dict()["samples"]
            end
        end
    end
end
