# Streaming tests (TCP).

@testset "Streaming" begin

    #==========================================================================#
    # Types and unit-level helpers
    #==========================================================================#
    @testset "StreamBody / FileBody / StreamedResponse" begin
        buf = IOBuffer(UInt8[1, 2, 3])
        sb = RepeRPC.StreamBody(buf, 3)
        @test sb.length == 3
        @test sb.body_format == RepeRPC.BODY_RAW_BINARY
        @test sb.own == true

        sb2 = RepeRPC.StreamBody(IOBuffer(), 0; body_format=RepeRPC.BODY_UTF8, own=false)
        @test sb2.body_format == RepeRPC.BODY_UTF8
        @test sb2.own == false

        @test_throws ArgumentError RepeRPC.StreamBody(IOBuffer(), -1)

        path = tempname()
        write(path, UInt8[10, 20, 30, 40])
        fb = RepeRPC.FileBody(path)
        @test fb.length == 4
        @test fb.own == true
        close(fb.io)
        rm(path, force=true)

        # FileBody rejects a non-regular file (a directory opens fine on POSIX but
        # has no streamable body) at construction with a clear error, rather than
        # declaring a bogus length and failing later as a mid-transfer short read.
        dir = mktempdir()
        @test_throws ArgumentError RepeRPC.FileBody(dir)
        rm(dir, force=true, recursive=true)

        sr = RepeRPC.StreamedResponse(123, RepeRPC.BODY_RAW_BINARY)
        @test sr.bytes == 123
        @test sr.body_format == RepeRPC.BODY_RAW_BINARY
        @test occursin("123 bytes", sprint(show, sr))

        # Wire-length guard: a set cap bounds the length (== cap allowed); an
        # unlimited cap still refuses a length past typemax(Int), which would
        # otherwise throw InexactError when read into an Int-indexed buffer.
        @test RepeRPC._within_length_limit(1000, 500)
        @test RepeRPC._within_length_limit(1000, 1000)
        @test !RepeRPC._within_length_limit(1000, 1001)
        @test RepeRPC._within_length_limit(nothing, 1_000_000)
        @test RepeRPC._within_length_limit(nothing, typemax(Int))
        @test !RepeRPC._within_length_limit(nothing, UInt64(2)^63)
        @test !RepeRPC._within_length_limit(nothing, typemax(UInt64))
    end

    @testset "_pump_exact" begin
        src = IOBuffer(collect(UInt8, 1:100))
        dest = IOBuffer()
        n = RepeRPC._pump_exact(src, dest, 100, 7)   # tiny chunk -> many iterations
        @test n == 100
        @test take!(dest) == collect(UInt8, 1:100)

        # Short source surfaces as EOFError (truncated transfer)
        src2 = IOBuffer(UInt8[1, 2, 3])
        @test_throws EOFError RepeRPC._pump_exact(src2, IOBuffer(), 10, 4)

        # Zero-length is a no-op
        @test RepeRPC._pump_exact(IOBuffer(), IOBuffer(), 0, 4) == 0

        @test_throws ArgumentError RepeRPC._pump_exact(IOBuffer(), IOBuffer(), 5, 0)
    end

    @testset "copy-reduction helpers" begin
        msg = RepeRPC.Message(id=7, query="/q", body=UInt8[9, 8, 7],
                           body_format=UInt16(RepeRPC.BODY_RAW_BINARY))
        full = RepeRPC.serialize_message(msg)
        # deserialize_message(header, payload) avoids re-parsing the header
        header = RepeRPC.deserialize_header(full[1:RepeRPC.HEADER_SIZE])
        rebuilt = RepeRPC.deserialize_message(header, full[RepeRPC.HEADER_SIZE+1:end])
        @test rebuilt.query == "/q"
        @test rebuilt.body == UInt8[9, 8, 7]

        # parse_body copy=false aliases / consumes the body
        m2 = RepeRPC.Message(id=1, query="", body=UInt8[1, 2, 3],
                          body_format=UInt16(RepeRPC.BODY_RAW_BINARY))
        aliased = RepeRPC.parse_body(m2; copy=false)
        @test aliased === m2.body          # same array, no copy
        m3 = RepeRPC.Message(id=1, query="", body=UInt8[4, 5, 6],
                          body_format=UInt16(RepeRPC.BODY_RAW_BINARY))
        copied = RepeRPC.parse_body(m3; copy=true)
        @test copied == UInt8[4, 5, 6]
        @test copied !== m3.body           # independent copy (default)
    end

    #==========================================================================#
    # Response streaming: server -> client (the primary use case)
    #==========================================================================#
    @testset "Response streaming (download)" begin
        port = 9611
        server = RepeRPC.Server("localhost", port)

        payload = rand(UInt8, 3_500_000)   # > default 1 MiB chunk: many iterations
        small = Vector{UInt8}("hello streaming")
        diskpath = tempname()
        write(diskpath, payload)

        # In-memory source, own=true (server closes the IOBuffer afterwards)
        RepeRPC.register(server, "/blob",
            (params, request) -> RepeRPC.StreamBody(IOBuffer(payload), length(payload); own=true))
        # File source via FileBody
        RepeRPC.register(server, "/file", (params, request) -> RepeRPC.FileBody(diskpath))
        # Small body, UTF8 format tag preserved
        RepeRPC.register(server, "/small",
            (params, request) -> RepeRPC.StreamBody(IOBuffer(small), length(small);
                                                 body_format=RepeRPC.BODY_UTF8))
        # Empty streamed body
        RepeRPC.register(server, "/empty",
            (params, request) -> RepeRPC.StreamBody(IOBuffer(UInt8[]), 0))
        # Params-driven endpoint (the /artifacts/get pattern)
        RepeRPC.register(server, "/sized", function(params, request)
            n = Int(params["n"])
            return RepeRPC.StreamBody(IOBuffer(payload[1:n]), n)
        end)
        # A normal handler, to confirm normal RPC still works on the same client
        RepeRPC.register(server, "/add", (params, request) -> Dict("r" => params["a"] + params["b"]))

        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)

        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)

        try
            @testset "stream_response into IOBuffer" begin
                sink = IOBuffer()
                resp = RepeRPC.stream_response(client, "/blob"; into=sink)
                @test resp isa RepeRPC.StreamedResponse
                @test resp.bytes == length(payload)
                @test resp.body_format == RepeRPC.BODY_RAW_BINARY
                @test take!(sink) == payload
            end

            @testset "small buffer_size -> many chunks" begin
                sink = IOBuffer()
                resp = RepeRPC.stream_response(client, "/blob"; into=sink, buffer_size=64 * 1024)
                @test resp.bytes == length(payload)
                @test take!(sink) == payload
            end

            @testset "download to file" begin
                dest = tempname()
                resp = RepeRPC.download(client, "/file", dest)
                @test resp.bytes == length(payload)
                @test read(dest) == payload
                rm(dest, force=true)
            end

            @testset "body_format tag preserved" begin
                sink = IOBuffer()
                resp = RepeRPC.stream_response(client, "/small"; into=sink)
                @test resp.body_format == RepeRPC.BODY_UTF8
                @test take!(sink) == small
            end

            @testset "empty streamed body" begin
                sink = IOBuffer()
                resp = RepeRPC.stream_response(client, "/empty"; into=sink)
                @test resp.bytes == 0
                @test isempty(take!(sink))
            end

            @testset "params-driven download" begin
                dest = tempname()
                resp = RepeRPC.download(client, "/sized", dest; params=Dict("n" => 1000))
                @test resp.bytes == 1000
                @test read(dest) == payload[1:1000]
                rm(dest, force=true)
            end

            @testset "normal RPC unaffected on same client" begin
                # Interleave normal requests around a stream on one connection.
                @test RepeRPC.send_request(client, "/add", Dict("a" => 2, "b" => 3))["r"] == 5
                sink = IOBuffer()
                RepeRPC.stream_response(client, "/blob"; into=sink)
                @test RepeRPC.send_request(client, "/add", Dict("a" => 10, "b" => 1))["r"] == 11
            end
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
            rm(diskpath, force=true)
        end
    end

    #==========================================================================#
    # StreamBody.own semantics and failure model
    #==========================================================================#
    @testset "own=false keeps the source open" begin
        port = 9612
        server = RepeRPC.Server("localhost", port)
        reused = IOBuffer(Vector{UInt8}("reuse-me"))
        RepeRPC.register(server, "/reused", function(params, request)
            seekstart(reused)
            return RepeRPC.StreamBody(reused, reused.size; own=false)
        end)
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        try
            sink = IOBuffer()
            resp = RepeRPC.stream_response(client, "/reused"; into=sink)
            @test String(take!(sink)) == "reuse-me"
            # own=false: the server must NOT have closed the caller's IOBuffer
            @test isopen(reused)
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end

    @testset "truncated source surfaces as a transport error" begin
        port = 9613
        server = RepeRPC.Server("localhost", port)
        # Declares 1_000_000 bytes but the source only has 10: the server pumps
        # what it has, then the pump hits EOF and the connection is torn down.
        RepeRPC.register(server, "/liar",
            (params, request) -> RepeRPC.StreamBody(IOBuffer(rand(UInt8, 10)), 1_000_000))
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        try
            sink = IOBuffer()
            # Caller must see an error, not a silently short body.
            @test_throws Exception RepeRPC.stream_response(client, "/liar"; into=sink, timeout=10.0)
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end

    @testset "server error instead of body" begin
        port = 9614
        server = RepeRPC.Server("localhost", port)
        # No /missing handler -> EC_METHOD_NOT_FOUND error response (not a body).
        RepeRPC.register(server, "/ok", (params, request) -> Dict("ok" => true))
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        try
            sink = IOBuffer()
            @test_throws Exception RepeRPC.stream_response(client, "/missing"; into=sink, timeout=10.0)
            # The connection stays usable: the error response was framed normally.
            @test RepeRPC.send_request(client, "/ok", nothing)["ok"] == true
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end

    #==========================================================================#
    # Request streaming: client -> server (upload)
    #==========================================================================#
    @testset "Request streaming (upload)" begin
        port = 9615
        server = RepeRPC.Server("localhost", port)
        RepeRPC.register(server, "/upload", function(params, request)
            return Dict("size" => length(request.body),
                        "sum" => isempty(request.body) ? 0 : Int(sum(Int.(request.body))))
        end)
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        try
            payload = rand(UInt8, 2_000_000)
            src = IOBuffer(payload)
            resp = RepeRPC.stream_request(client, "/upload", src, length(payload);
                                       body_format=RepeRPC.BODY_RAW_BINARY, buffer_size=128 * 1024)
            @test resp["size"] == length(payload)
            @test resp["sum"] == sum(Int.(payload))

            # A normal request afterwards still works on the same connection.
            small = UInt8[1, 2, 3, 4]
            resp2 = RepeRPC.stream_request(client, "/upload", IOBuffer(small), length(small);
                                        body_format=RepeRPC.BODY_RAW_BINARY)
            @test resp2["size"] == 4
            @test resp2["sum"] == 10
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end

    #==========================================================================#
    # max_body_length guard
    #==========================================================================#
    @testset "max_body_length guard" begin
        @testset "constructor validation" begin
            @test_throws ArgumentError RepeRPC.Server("localhost", 9616; buffer_size=0)
            @test_throws ArgumentError RepeRPC.Server("localhost", 9616; max_body_length=-1)
            @test_throws ArgumentError RepeRPC.Client("localhost", 9616; buffer_size=0)
            @test_throws ArgumentError RepeRPC.Client("localhost", 9616; max_body_length=-5)
            # nothing is allowed (unlimited, default)
            @test RepeRPC.Server("localhost", 9616).max_body_length === nothing
            @test RepeRPC.Client("localhost", 9616).max_body_length === nothing
        end

        @testset "server rejects oversized request" begin
            port = 9617
            server = RepeRPC.Server("localhost", port; max_body_length=1_000_000)
            RepeRPC.register(server, "/echo_size", (params, request) -> Dict("n" => length(request.body)))
            RepeRPC.listen(server; async=true)
            RepeRPC.wait_for_server("localhost", port)
            client = RepeRPC.Client("localhost", port)
            RepeRPC.connect(client)
            try
                # Under the cap: fine
                ok = RepeRPC.send_request(client, "/echo_size", rand(UInt8, 500_000);
                                       body_format=RepeRPC.BODY_RAW_BINARY, timeout=10.0)
                @test ok["n"] == 500_000
                # Over the cap on a fresh connection: rejected (error or closed)
                client2 = RepeRPC.Client("localhost", port)
                RepeRPC.connect(client2)
                @test_throws Exception RepeRPC.send_request(client2, "/echo_size", rand(UInt8, 2_000_000);
                                                         body_format=RepeRPC.BODY_RAW_BINARY, timeout=10.0)
                RepeRPC.disconnect(client2)
            finally
                RepeRPC.disconnect(client)
                RepeRPC.stop(server)
            end
        end

        @testset "client rejects oversized response" begin
            port = 9618
            server = RepeRPC.Server("localhost", port)
            RepeRPC.register(server, "/big",
                (params, request) -> RepeRPC.StreamBody(IOBuffer(rand(UInt8, 3_000_000)), 3_000_000))
            RepeRPC.listen(server; async=true)
            RepeRPC.wait_for_server("localhost", port)
            client = RepeRPC.Client("localhost", port; max_body_length=1_000_000)
            RepeRPC.connect(client)
            try
                @test_throws Exception RepeRPC.stream_response(client, "/big"; into=IOBuffer(), timeout=10.0)
            finally
                RepeRPC.disconnect(client)
                RepeRPC.stop(server)
            end
        end
    end

    #==========================================================================#
    # Event-based wakeup: timeout still fires; rapid sequential requests
    #==========================================================================#
    @testset "event-based wakeup" begin
        port = 9619
        server = RepeRPC.Server("localhost", port)
        RepeRPC.register(server, "/ping", (params, request) -> Dict("pong" => true))
        RepeRPC.register(server, "/slow", function(params, request)
            sleep(0.5)
            return Dict("done" => true)
        end)
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        try
            # Timeout still raises TimeoutError
            @test_throws RepeRPC.TimeoutError RepeRPC.send_request(client, "/slow", nothing; timeout=0.05)
            # Rapid sequential requests complete correctly (no poll-interval floor)
            for _ in 1:200
                @test RepeRPC.send_request(client, "/ping", nothing)["pong"] == true
            end
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end

    #==========================================================================#
    # Fleet streaming (TCP)
    #==========================================================================#
    @testset "Fleet streaming (TCP)" begin
        port = 9620
        server = RepeRPC.Server("localhost", port)
        payload = rand(UInt8, 2_500_000)
        RepeRPC.register(server, "/blob",
            (params, request) -> RepeRPC.StreamBody(IOBuffer(payload), length(payload)))
        RepeRPC.register(server, "/status", (params, request) -> Dict("ok" => true))
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)

        fleet = RepeRPC.Fleet([RepeRPC.NodeConfig("localhost", port; name="n1")])
        RepeRPC.connect!(fleet)
        try
            @testset "stream into IOBuffer" begin
                sink = IOBuffer()
                r = RepeRPC.stream(fleet, "n1", "/blob"; into=sink)
                @test r isa RepeRPC.RemoteResult{RepeRPC.StreamedResponse}
                @test RepeRPC.succeeded(r)
                @test r.value.bytes == length(payload)
                @test take!(sink) == payload
            end

            @testset "download to file" begin
                dest = tempname()
                r = RepeRPC.download(fleet, "n1", "/blob", dest)
                @test RepeRPC.succeeded(r)
                @test read(dest) == payload
                rm(dest, force=true)
            end

            @testset "unknown node captured into RemoteResult, not thrown" begin
                rs = RepeRPC.stream(fleet, "nope", "/blob"; into=IOBuffer())
                @test RepeRPC.failed(rs)
                @test rs.error isa KeyError
                @test_throws KeyError rs[]   # getindex rethrows the captured miss

                # download must NOT open/truncate dest when the node is unknown:
                # the lookup is captured before `open`, leaving any existing file intact.
                dest = tempname()
                write(dest, "sentinel")
                rd = RepeRPC.download(fleet, "nope", "/blob", dest)
                @test RepeRPC.failed(rd)
                @test rd.error isa KeyError
                @test read(dest, String) == "sentinel"
                rm(dest, force=true)
            end

            @testset "failure captured into RemoteResult" begin
                # No /missing handler -> error response -> failed RemoteResult (not throw)
                r = RepeRPC.stream(fleet, "n1", "/missing"; into=IOBuffer(), timeout=10.0)
                @test RepeRPC.failed(r)
            end

            @testset "normal broadcast still works" begin
                res = RepeRPC.broadcast(fleet, "/status")
                @test RepeRPC.succeeded(res["n1"])
            end
        finally
            RepeRPC.disconnect!(fleet)
            RepeRPC.stop(server)
        end
    end

    #==========================================================================#
    # Additional edge cases and guarantees
    #==========================================================================#
    @testset "parse_body copy=false (UTF8 move semantics)" begin
        m = RepeRPC.Message(id=1, query="", body=Vector{UInt8}("hello"),
                         body_format=UInt16(RepeRPC.BODY_UTF8))
        s = RepeRPC.parse_body(m; copy=false)
        @test s == "hello"
        @test isempty(m.body)              # String() consumed the buffer
        # copy=true (default) leaves the body intact and returns an independent value
        m2 = RepeRPC.Message(id=1, query="", body=Vector{UInt8}("hello"),
                          body_format=UInt16(RepeRPC.BODY_UTF8))
        @test RepeRPC.parse_body(m2; copy=true) == "hello"
        @test String(m2.body) == "hello"
    end

    @testset "deserialize_message(header, payload) validation" begin
        msg = RepeRPC.Message(id=3, query="/x", body=UInt8[1, 2, 3, 4],
                           body_format=UInt16(RepeRPC.BODY_RAW_BINARY))
        @test_throws ArgumentError RepeRPC.deserialize_message(msg.header, UInt8[])  # too small
    end

    @testset "max_body_length boundary (== cap is allowed)" begin
        port = 9621
        cap = 4096
        server = RepeRPC.Server("localhost", port; max_body_length=cap)
        RepeRPC.register(server, "/echo_size", (params, request) -> Dict("n" => length(request.body)))
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        try
            ok = RepeRPC.send_request(client, "/echo_size", rand(UInt8, cap);
                                   body_format=RepeRPC.BODY_RAW_BINARY, timeout=10.0)
            @test ok["n"] == cap            # exactly at the cap: allowed (<=)
            c2 = RepeRPC.Client("localhost", port)
            RepeRPC.connect(c2)
            @test_throws Exception RepeRPC.send_request(c2, "/echo_size", rand(UInt8, cap + 1);
                                                     body_format=RepeRPC.BODY_RAW_BINARY, timeout=10.0)
            RepeRPC.disconnect(c2)
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end

    @testset "stream_response streams a non-StreamBody handler's body" begin
        port = 9623
        server = RepeRPC.Server("localhost", port)
        RepeRPC.register(server, "/json", (params, request) -> Dict("k" => "v"))
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        try
            sink = IOBuffer()
            resp = RepeRPC.stream_response(client, "/json"; into=sink)
            @test resp.body_format == RepeRPC.BODY_JSON
            # The materialized JSON body was streamed in verbatim: a streamed
            # transfer is byte-for-byte identical to the non-streamed encoding.
            @test take!(sink) == RepeRPC.encode_body(Dict("k" => "v"), RepeRPC.BODY_JSON)
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end

    @testset "own=true closes the source" begin
        port = 9624
        server = RepeRPC.Server("localhost", port)
        shared = IOBuffer(Vector{UInt8}("close-me"))
        RepeRPC.register(server, "/owned",
            (params, request) -> (seekstart(shared); RepeRPC.StreamBody(shared, shared.size; own=true)))
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        try
            sink = IOBuffer()
            RepeRPC.stream_response(client, "/owned"; into=sink)
            @test String(take!(sink)) == "close-me"
            # The server closes the source in its finally; allow for the small
            # window between the client receiving the last byte and that close.
            @test timedwait(() -> !isopen(shared), 2.0) == :ok
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end

    @testset "failed download leaves a partial file (not rolled back)" begin
        port = 9625
        server = RepeRPC.Server("localhost", port)
        # Declares 5_000_000 bytes but the source only has 10: truncated transfer.
        RepeRPC.register(server, "/liar",
            (params, request) -> RepeRPC.StreamBody(IOBuffer(rand(UInt8, 10)), 5_000_000))
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        try
            dest = tempname()
            @test_throws Exception RepeRPC.download(client, "/liar", dest; timeout=10.0)
            @test isfile(dest)             # truncated at open, left in place on failure
            rm(dest, force=true)
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end

    @testset "stream_request short source tears down, client stays reusable" begin
        port = 9626
        server = RepeRPC.Server("localhost", port)
        RepeRPC.register(server, "/sink", (params, request) -> Dict("n" => length(request.body)))
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        try
            # Declares 1_000_000 bytes but the source has 3: the send-side pump
            # hits EOF after a partial frame is already on the wire.
            @test_throws Exception RepeRPC.stream_request(client, "/sink",
                IOBuffer(UInt8[1, 2, 3]), 1_000_000; body_format=RepeRPC.BODY_RAW_BINARY)
            # The partial frame desynced the socket, so the failed call must tear
            # the connection down rather than leave it "connected" and corrupt.
            @test client.connected == false
            # Reusing the client transparently reconnects onto a clean stream;
            # before the teardown fix this silently corrupted and timed out.
            ok = RepeRPC.send_request(client, "/sink", UInt8[1, 2, 3, 4];
                                   body_format=RepeRPC.BODY_RAW_BINARY, timeout=10.0)
            @test ok["n"] == 4
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end

    @testset "explicit timeout on a stream raises TimeoutError" begin
        port = 9627
        server = RepeRPC.Server("localhost", port)
        rel = Base.Event()
        RepeRPC.register(server, "/slow_blob", function(params, request)
            wait(rel)                      # withhold the response until released
            return RepeRPC.StreamBody(IOBuffer(UInt8[1, 2, 3]), 3)
        end)
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        try
            @test_throws RepeRPC.TimeoutError RepeRPC.stream_response(client, "/slow_blob";
                into=IOBuffer(), timeout=0.2)
            # A timed-out stream tears the connection down: you cannot resync a
            # half-read body, so the contract is uniform whether the timeout fires
            # before the header (here) or mid-body (next testset).
            @test client.connected == false
        finally
            notify(rel)                    # let the handler finish and the server task unwind
            sleep(0.1)
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end

    @testset "streamed receive timeout mid-body tears the connection down" begin
        # The headline case: a timeout WHILE the reader is pumping the body must
        # abort the pump (not keep draining the whole body into the abandoned sink
        # in the background) and tear the connection down. The server streams the
        # bytes it has, then stalls mid-body (a BufferStream that is written once
        # and never closed), so the client reliably times out mid-pump.
        port = 9629
        server = RepeRPC.Server("localhost", port; buffer_size=4096)
        declared = 50_000_000             # header declares 50 MB...
        src = Base.BufferStream()
        write(src, rand(UInt8, 65_536))   # ...but only 65_536 bytes are ever available
        RepeRPC.register(server, "/stall",
            (params, request) -> RepeRPC.StreamBody(src, declared; own=false))
        RepeRPC.register(server, "/add", (params, request) -> Dict("r" => params["a"] + params["b"]))
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        try
            sink = IOBuffer()
            @test_throws RepeRPC.TimeoutError RepeRPC.stream_response(client, "/stall";
                into=sink, timeout=0.5, buffer_size=4096)
            # Torn down, not left "connected" with a background drain.
            @test client.connected == false
            # Bounded: the sink holds at most what the server actually sent
            # (65_536), never the declared 50 MB. The server only ever put 65_536
            # bytes on the wire, so this can never exceed that regardless of timing.
            @test position(sink) <= 65_536
            @test position(sink) < declared
            # The client transparently reconnects onto a clean stream; before the
            # fix the leaked reader stayed blocked and reuse would time out.
            r = RepeRPC.send_request(client, "/add", Dict("a" => 2, "b" => 3); timeout=10.0)
            @test r["r"] == 5
        finally
            close(src)                     # unblock the server's stalled pump
            sleep(0.1)
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end

    @testset "no-timeout stream wakes when the connection drops" begin
        port = 9628
        server = RepeRPC.Server("localhost", port)
        rel = Base.Event()
        RepeRPC.register(server, "/never", function(params, request)
            wait(rel)                      # never responds before we disconnect
            return RepeRPC.StreamBody(IOBuffer(UInt8[1, 2, 3]), 3)
        end)
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        # No-timeout stream: it must wake when the connection drops, not hang forever.
        t = @async try
            RepeRPC.stream_response(client, "/never"; into=IOBuffer())
            :no_error
        catch e
            e
        end
        sleep(0.3)                         # let the request be sent and the reader block
        RepeRPC.disconnect(client)            # tears down the connection
        # Watchdog: a hang regression makes this fail rather than stall the suite.
        @test timedwait(() -> istaskdone(t), 10.0) == :ok
        @test fetch(t) isa Exception
        notify(rel)
        sleep(0.1)
        RepeRPC.stop(server)
    end

    @testset "mid-stream connection drop surfaces ConnectionError" begin
        # A streamed receive whose connection drops WHILE the reader is pumping
        # the body must surface ConnectionError (the documented contract), not the
        # raw EOFError leaked from the socket read. The server streams a partial
        # body then drops (its own source EOFs), so the reader is reliably
        # mid-pump and is the sole deliverer (no client-side disconnect race).
        port = 9630
        server = RepeRPC.Server("localhost", port; buffer_size=4096)
        src = Base.BufferStream()
        write(src, rand(UInt8, 65_536))
        RepeRPC.register(server, "/drop",
            (params, request) -> RepeRPC.StreamBody(src, 50_000_000; own=false))
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        t = @async try
            RepeRPC.stream_response(client, "/drop"; into=IOBuffer(), buffer_size=4096)
            :no_error
        catch e
            e
        end
        sleep(0.3)                         # reader receives the partial body, blocks mid-pump
        close(src)                         # server's pump hits EOF -> it closes the socket mid-stream
        @test timedwait(() -> istaskdone(t), 10.0) == :ok
        @test fetch(t) isa RepeRPC.ConnectionError
        sleep(0.1)
        RepeRPC.disconnect(client)
        RepeRPC.stop(server)
    end

    @testset "streamed fleet calls are not retried" begin
        deadport = 9699                    # nothing listening here
        fleet = RepeRPC.Fleet([RepeRPC.NodeConfig("localhost", deadport; name="dead")];
                           max_retry_attempts=3, retry_delay=0.5)
        # A normal call retries the (retryable) connect failure: ~2 delays of work.
        rc = RepeRPC.call(fleet, "dead", "/x", nothing)
        @test RepeRPC.failed(rc)
        # A streamed call makes a single attempt: it fails well under one retry delay.
        t0 = time()
        rs = RepeRPC.stream(fleet, "dead", "/x"; into=IOBuffer())
        elapsed = time() - t0
        @test RepeRPC.failed(rs)
        @test elapsed < 0.5                # no retry delay incurred
        @test rs.elapsed < rc.elapsed      # strictly faster than the retried call
    end

    @testset "guard rejects an oversized query_length, not just body_length" begin
        # deserialize_header only checks query_length + body_length against the
        # header's `length`, so a peer can declare a huge query_length with a tiny
        # body_length and slip past a body-only guard. Hand-craft such a header and
        # confirm the server refuses it (EC_INVALID_HEADER) before allocating.
        port = 9631
        server = RepeRPC.Server("localhost", port; max_body_length=1_000_000)
        RepeRPC.register(server, "/x", (params, request) -> "ok")
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)
        try
            qlen = 1 << 40                 # 1 TiB query, body_length = 0 (within cap)
            hdr = RepeRPC.Header(length = RepeRPC.HEADER_SIZE + qlen, id = 77,
                              query_length = qlen, body_length = 0)
            sock = Sockets.connect("127.0.0.1", port)
            try
                write(sock, RepeRPC.serialize_header(hdr))   # header only; never the 1 TiB query
                flush(sock)
                resp = RepeRPC.deserialize_header(read(sock, RepeRPC.HEADER_SIZE))
                @test resp.ec == UInt32(RepeRPC.EC_INVALID_HEADER)
                @test resp.id == 77        # echoed so the caller can correlate
            finally
                close(sock)
            end
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "client rejects a response length past typemax(Int) cleanly" begin
        # A garbled/malicious response header with body_length >= 2^63 must be
        # refused with REPEError(EC_INVALID_HEADER), not crash with InexactError
        # when the length is narrowed to Int for allocation. Default cap is
        # unlimited, so this exercises the typemax(Int) bound specifically.
        port = 9632
        listener = Sockets.listen(Sockets.localhost, port)
        srv = @async try
            conn = Sockets.accept(listener)
            reqhdr = RepeRPC.deserialize_header(read(conn, RepeRPC.HEADER_SIZE))
            read(conn, Int(reqhdr.query_length) + Int(reqhdr.body_length))  # discard request
            blen = UInt64(2)^63
            resp = RepeRPC.Header(length = RepeRPC.HEADER_SIZE + blen, id = reqhdr.id,
                               body_length = blen, ec = UInt32(RepeRPC.EC_OK))
            write(conn, RepeRPC.serialize_header(resp))      # header only; never the absurd body
            flush(conn)
            sleep(0.2)
            close(conn)
        catch
            # listener closed during teardown
        end
        try
            client = RepeRPC.Client("127.0.0.1", port)
            RepeRPC.connect(client)
            try
                @test_throws RepeRPC.REPEError RepeRPC.send_request(client, "/x", nothing; timeout=10.0)
            finally
                RepeRPC.disconnect(client)
            end
        finally
            close(listener)
            wait(srv)
        end
    end
end
