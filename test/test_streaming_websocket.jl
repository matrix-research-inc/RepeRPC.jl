# Streaming behavior over the WebSocket transport.
# Requires: import HTTP (loaded by runtests.jl to trigger the extension).
#
# Streaming buys nothing over WebSocket: HTTP.jl's `receive` reassembles every
# fragment into one in-memory buffer before yielding it (no per-frame read API),
# so a receiver materializes the whole body no matter how the sender frames it,
# and the memory bound StreamBody exists to provide is unreachable end-to-end.
# So the contract here is: a returned StreamBody is rejected with a clear error,
# Fleet stream/download fall back to a materialized request, and that fallback is
# bounded by the node's max_body_length (a post-receive ceiling on WebSocket --
# HTTP.jl has already buffered the frame -- but it still refuses to parse or
# deliver an oversized body and surfaces a clean failure).

@testset "Streaming over WebSocket" begin
    payload = rand(UInt8, 1_500_000)

    server = RepeRPC.WSServer("127.0.0.1", 0)
    # Handler that returns a StreamBody -> must be rejected by the WS server.
    RepeRPC.register(server, "/blob_stream",
        (params, request) -> RepeRPC.StreamBody(IOBuffer(payload), length(payload); own=true))
    # Handler that returns a materialized raw-binary body -> WS fallback can write it.
    RepeRPC.register(server, "/blob_raw", function(params, request)
        return RepeRPC.Message(id=request.header.id, query=request.query, body=payload,
                            body_format=UInt16(RepeRPC.BODY_RAW_BINARY), ec=UInt32(RepeRPC.EC_OK))
    end)
    RepeRPC.register(server, "/text", (params, request) -> "a string body")
    # Structured (JSON) body: the materialized fallback cannot write it byte-for-byte.
    RepeRPC.register(server, "/json", (params, request) -> Dict("k" => "v"))
    # Empty (0-byte) raw body: the fallback must treat it as a 0-byte write.
    RepeRPC.register(server, "/empty_raw", function(params, request)
        return RepeRPC.Message(id=request.header.id, query=request.query, body=UInt8[],
                            body_format=UInt16(RepeRPC.BODY_RAW_BINARY), ec=UInt32(RepeRPC.EC_OK))
    end)

    RepeRPC.listen(server; async=true)
    # Wait for the async listener to bind and publish its port, rather than a
    # fixed sleep that can flake on a cold or loaded CI before the port is set.
    @test timedwait(() -> server.port != 0, 10.0) == :ok

    try
        @testset "WS server rejects a StreamBody response" begin
            client = RepeRPC.WSClient("127.0.0.1", server.port)
            RepeRPC.connect(client)
            try
                local err
                threw = false
                try
                    RepeRPC.send_request(client, "/blob_stream", nothing; timeout=10.0)
                catch e
                    threw = true
                    err = e
                end
                @test threw
                @test occursin("StreamBody", string(err))
                # Connection remains usable after the rejection.
                @test RepeRPC.send_request(client, "/text", nothing; timeout=10.0) == "a string body"
            finally
                RepeRPC.disconnect(client)
            end
        end

        @testset "Fleet stream/download fall back to materialized over WS" begin
            fleet = RepeRPC.Fleet([RepeRPC.NodeConfig("ws://127.0.0.1:$(server.port)"; name="wsnode")])
            RepeRPC.connect!(fleet)
            try
                sink = IOBuffer()
                r = RepeRPC.stream(fleet, "wsnode", "/blob_raw"; into=sink, body_format=RepeRPC.BODY_RAW_BINARY)
                @test RepeRPC.succeeded(r)
                @test r.value.bytes == length(payload)
                @test r.value.body_format == RepeRPC.BODY_RAW_BINARY
                @test take!(sink) == payload

                dest = tempname()
                r2 = RepeRPC.download(fleet, "wsnode", "/blob_raw", dest; body_format=RepeRPC.BODY_RAW_BINARY)
                @test RepeRPC.succeeded(r2)
                @test read(dest) == payload
                rm(dest, force=true)

                # String body falls back too (written as UTF-8 bytes).
                sink2 = IOBuffer()
                r3 = RepeRPC.stream(fleet, "wsnode", "/text"; into=sink2)
                @test RepeRPC.succeeded(r3)
                @test r3.value.body_format == RepeRPC.BODY_UTF8
                @test String(take!(sink2)) == "a string body"

                # Empty (0-byte) body: the fallback must treat it as a 0-byte
                # write (matching the TCP path), not reject it. parse_body returns
                # nothing for an empty body; before the fix that raised here.
                sink_empty = IOBuffer()
                re = RepeRPC.stream(fleet, "wsnode", "/empty_raw"; into=sink_empty)
                @test RepeRPC.succeeded(re)
                @test re.value.bytes == 0
                @test position(sink_empty) == 0

                # Streaming a StreamBody handler over WS -> server rejects -> failed result.
                r4 = RepeRPC.stream(fleet, "wsnode", "/blob_stream"; into=IOBuffer(), timeout=10.0)
                @test RepeRPC.failed(r4)

                # A structured (non-binary) body cannot be written by the fallback:
                # it fails with an ArgumentError captured into the RemoteResult.
                r5 = RepeRPC.stream(fleet, "wsnode", "/json"; into=IOBuffer(), timeout=10.0)
                @test RepeRPC.failed(r5)
                @test r5.error isa ArgumentError
            finally
                RepeRPC.disconnect!(fleet)
            end
        end

        @testset "max_body_length caps the WS materialized fallback" begin
            # Cap well below the 1.5 MB payload but above the small bodies. The
            # fallback must refuse the oversize response (a clean failed
            # RemoteResult, nothing written to the sink), still serve a body
            # within the cap, and stay usable after the rejection -- a WebSocket
            # frame is self-delimiting, so one oversize response does not desync
            # the connection.
            capped = RepeRPC.Fleet([RepeRPC.NodeConfig("ws://127.0.0.1:$(server.port)";
                                                 name="capped", max_body_length=1024)])
            RepeRPC.connect!(capped)
            try
                sink = IOBuffer()
                r = RepeRPC.stream(capped, "capped", "/blob_raw"; into=sink, body_format=RepeRPC.BODY_RAW_BINARY)
                @test RepeRPC.failed(r)
                @test r.error isa RepeRPC.REPEError
                @test occursin("too large", r.error.message)
                @test position(sink) == 0          # oversize body never reached the sink

                dest = tempname()
                rd = RepeRPC.download(capped, "capped", "/blob_raw", dest; body_format=RepeRPC.BODY_RAW_BINARY)
                @test RepeRPC.failed(rd)
                @test filesize(dest) == 0           # truncated at open, nothing written
                rm(dest, force=true)

                # A body within the cap still streams, proving the cap rejects
                # only oversize responses and the connection survived the refusal.
                ok = IOBuffer()
                rs = RepeRPC.stream(capped, "capped", "/text"; into=ok)
                @test RepeRPC.succeeded(rs)
                @test String(take!(ok)) == "a string body"
            finally
                RepeRPC.disconnect!(capped)
            end
        end
    finally
        RepeRPC.stop(server)
    end
end
