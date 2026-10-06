# Serialized Value Stream (SVS) tests over TCP. CodecZstd (a test dependency)
# loads RepeRPCCodecZstdExt, enabling the compressed paths.
import CodecZstd
import BeveFormat

# A struct streamed end-to-end, exercising both the generic (Dict) and typed
# decode of the same BEVE object.
struct SvsTyped
    id::UInt64
    label::String
    samples::Vector{Float64}
end

_svs_typed() = SvsTyped(0xFEEDBEEF, "serialized value stream", collect(0.0:0.5:200000.0))
_svs_dict() = Dict("label" => "svs", "samples" => collect(-1000.0:0.25:1000.0),
                   "tags" => ["tag-$i" for i in 0:63])

_svs_opts(c) = RepeRPC.StreamOpts(chunk_bytes = 16 * 1024, compression = c, session_depth = 4)

# Bind an ephemeral port, read it back, release it. Small race, fine for tests.
function _free_port()
    s = Sockets.listen(Sockets.IPv4(0), 0)
    p = Int(Sockets.getsockname(s)[2])
    close(s)
    return p
end

# Spin up a TCP SVS server for `resolve`, run `f(client)`, tear both down.
function _with_tcp_svs(f, resolve; opts)
    port = _free_port()
    srv = RepeRPC.Server("127.0.0.1", port)
    RepeRPC.serve_value_stream(srv, resolve; opts = opts)
    RepeRPC.listen(srv; async = true)
    RepeRPC.wait_for_server("127.0.0.1", port)
    client = RepeRPC.Client("127.0.0.1", port)
    RepeRPC.connect(client)
    try
        f(client)
    finally
        RepeRPC.disconnect(client)
        RepeRPC.stop(srv)
    end
end

function _svs_resolve(resource)
    resource == "typed" && return _svs_typed()
    resource == "dict" && return _svs_dict()
    return nothing
end

# A fake client whose `next` yields `fail_after` non-terminal chunks and then
# throws, simulating a connection dropped mid-stream. It lets the atomic-output
# test drive `_pull_to_file`'s real commit-on-`last` logic deterministically,
# without racing a real socket teardown against a fast loopback transfer.
struct _DropClient <: RepeRPC.AbstractClient
    fail_after::Int
    count::Base.RefValue{Int}
end
_DropClient(fail_after::Int) = _DropClient(fail_after, Ref(0))

function RepeRPC.send_request(::Type{RepeRPC.Message}, c::_DropClient, method::AbstractString, params; kwargs...)
    c.count[] += 1
    c.count[] > c.fail_after && error("simulated connection drop")
    # A non-terminal chunk: last flag 0x00 in the query, opaque bytes in the body.
    return RepeRPC.Message(id = UInt64(c.count[]), query = String(UInt8[0x00]),
                        body = fill(UInt8(c.count[] & 0xFF), 4096),
                        query_format = UInt16(RepeRPC.QUERY_RAW_BINARY),
                        body_format = UInt16(RepeRPC.BODY_RAW_BINARY))
end

@testset "Value Stream (SVS)" begin
    @testset "wire structs round-trip through BEVE" begin
        o = RepeRPC._OpenResponse(RepeRPC.SVS_VERSION, UInt64(7), UInt16(1), UInt8(1))
        o2 = BeveFormat.deser_beve(RepeRPC._OpenResponse, BeveFormat.to_beve(o))
        @test (o2.version, o2.stream_id, o2.format, o2.compression) == (1, 7, 1, 1)
        @test BeveFormat.deser_beve(RepeRPC._NextRequest, BeveFormat.to_beve(RepeRPC._NextRequest(UInt64(99)))).stream_id == 99
        c = BeveFormat.deser_beve(RepeRPC._CancelRequest, BeveFormat.to_beve(RepeRPC._CancelRequest(UInt64(3), "bye")))
        @test c.stream_id == 3 && c.reason == "bye"
    end

    @testset "zstd extension loaded" begin
        @test RepeRPC._zstd_available()
    end

    for comp in (:none, :zstd)
        @testset "pull_value generic ($comp)" begin
            _with_tcp_svs(_svs_resolve; opts = _svs_opts(comp)) do client
                v = RepeRPC.pull_value(client, "dict")
                ref = _svs_dict()
                @test v["samples"] == ref["samples"]
                @test v["tags"] == ref["tags"]
                @test v["label"] == ref["label"]
            end
        end

        @testset "pull_value typed ($comp)" begin
            _with_tcp_svs(_svs_resolve; opts = _svs_opts(comp)) do client
                v = RepeRPC.pull_value(SvsTyped, client, "typed")
                ref = _svs_typed()
                @test v.id == ref.id
                @test v.label == ref.label
                @test v.samples == ref.samples
            end
        end
    end

    @testset "file modes (zstd)" begin
        _with_tcp_svs(_svs_resolve; opts = _svs_opts(:zstd)) do client
            mktempdir() do dir
                bp = joinpath(dir, "out.beve")
                RepeRPC.pull_to_beve_file(client, "dict", bp)
                @test isfile(bp)
                @test !isfile(bp * ".svspart")          # committed atomically, temp gone
                @test BeveFormat.read_beve_file(bp)["samples"] == _svs_dict()["samples"]

                zp = joinpath(dir, "out.beve.zst")
                RepeRPC.pull_to_beve_zst_file(client, "dict", zp)
                @test isfile(zp)
                @test !isfile(zp * ".svspart")
                @test BeveFormat.read_beve_zstd_file(zp)["samples"] == _svs_dict()["samples"]
            end
        end
    end

    @testset "unknown resource errors, does not hang" begin
        _with_tcp_svs(_svs_resolve; opts = _svs_opts(:zstd)) do client
            @test_throws Exception RepeRPC.pull_value(client, "does-not-exist")
        end
    end

    @testset "output incompatible with stream tags is rejected" begin
        # An uncompressed producer cannot satisfy a .beve.zst / .beve request; the
        # client must reject (and leave no output file) rather than mislabel.
        _with_tcp_svs(_svs_resolve; opts = _svs_opts(:none)) do client
            mktempdir() do dir
                p = joinpath(dir, "x.beve.zst")
                @test_throws Exception RepeRPC.pull_to_beve_zst_file(client, "dict", p)
                @test !isfile(p)
                @test !isfile(p * ".svspart")
            end
        end
    end

    # ---- session lifecycle (abandoned / cancelled / capped pulls) -------------
    #
    # The dict spans many 8 KiB chunks uncompressed, so a single `next` leaves the
    # stream mid-flight and the session a live reaper/cancel candidate. These tests
    # drive the wire directly (`_open_stream` / raw `next` / `_cancel_stream`)
    # because the high-level helpers always drain or cancel, hiding these states.
    _svs_open_next(client, resource) = begin
        info = RepeRPC._open_stream(client, resource, nothing)
        r = RepeRPC.send_request(RepeRPC.Message, client, RepeRPC.SVS_NEXT,
                              RepeRPC._NextRequest(info.stream_id);
                              query_format = RepeRPC.QUERY_JSON_POINTER, body_format = RepeRPC.BODY_BEVE)
        (info, r)
    end
    _svs_next_throws(client, stream_id) = @test_throws Exception RepeRPC.send_request(
        RepeRPC.Message, client, RepeRPC.SVS_NEXT, RepeRPC._NextRequest(stream_id);
        query_format = RepeRPC.QUERY_JSON_POINTER, body_format = RepeRPC.BODY_BEVE)

    @testset "idle session is reaped after the timeout" begin
        opts = RepeRPC.StreamOpts(chunk_bytes = 8 * 1024, compression = :none,
                               session_depth = 2, idle_timeout = 1.0)
        _with_tcp_svs(_svs_resolve; opts = opts) do client
            info, r1 = _svs_open_next(client, "dict")
            @test codeunit(r1.query, 1) == 0x00          # not the last chunk: session persists
            sleep(2.5)                                    # exceed idle_timeout, send no next/cancel
            _svs_next_throws(client, info.stream_id)      # reaper reclaimed the abandoned session
        end
    end

    @testset "cancel mid-stream frees the session" begin
        opts = RepeRPC.StreamOpts(chunk_bytes = 8 * 1024, compression = :none, session_depth = 2)
        _with_tcp_svs(_svs_resolve; opts = opts) do client
            info, r1 = _svs_open_next(client, "dict")
            @test codeunit(r1.query, 1) == 0x00
            RepeRPC._cancel_stream(client, info.stream_id, "test cancel")
            # Same connection, so TCP ordering guarantees the cancel is processed
            # before this next; the session is gone.
            _svs_next_throws(client, info.stream_id)
        end
    end

    @testset "max_sessions caps concurrent opens" begin
        opts = RepeRPC.StreamOpts(chunk_bytes = 8 * 1024, compression = :none,
                               session_depth = 2, max_sessions = 2)
        _with_tcp_svs(_svs_resolve; opts = opts) do client
            i1 = RepeRPC._open_stream(client, "dict", nothing)   # undrained: holds a session
            i2 = RepeRPC._open_stream(client, "dict", nothing)
            @test i1.stream_id != i2.stream_id
            @test_throws Exception RepeRPC._open_stream(client, "dict", nothing)   # over the cap
            RepeRPC._cancel_stream(client, i1.stream_id, "make room")
            i3 = RepeRPC._open_stream(client, "dict", nothing)   # a freed slot admits a new open
            @test i3.stream_id ∉ (i1.stream_id, i2.stream_id)
        end
    end

    @testset "malformed stream_id is rejected without killing the connection" begin
        _with_tcp_svs(_svs_resolve; opts = _svs_opts(:none)) do client
            @test_throws Exception RepeRPC.send_request(RepeRPC.Message, client, RepeRPC.SVS_NEXT,
                Dict("stream_id" => "not-a-number");
                query_format = RepeRPC.QUERY_JSON_POINTER, body_format = RepeRPC.BODY_BEVE)
            # The connection survives a malformed request: a valid open still works.
            info = RepeRPC._open_stream(client, "dict", nothing)
            @test info.stream_id > 0
        end
    end

    @testset "atomic output: a mid-stream drop leaves no final file" begin
        # `_pull_to_file` must commit (rename) only after the terminal `last`
        # chunk. Drive it with a client that drops after three non-terminal chunks
        # and assert neither the final file nor its temp survives.
        mktempdir() do dir
            p = joinpath(dir, "out.beve")
            info = RepeRPC._OpenInfo(UInt64(1), UInt16(RepeRPC.BODY_BEVE), RepeRPC._COMPRESSION_NONE)
            @test_throws Exception RepeRPC._pull_to_file(_DropClient(3), info, p, nothing, false)
            @test !isfile(p)
            @test !isfile(p * ".svspart")
        end
    end

    # ---- connection-scoped teardown hook --------------------------------------

    @testset "ServerConnection.on_close runs callbacks once, in order" begin
        conn = RepeRPC.ServerConnection()
        log = Int[]
        RepeRPC.on_close(conn, () -> push!(log, 1))
        RepeRPC.on_close(conn, () -> push!(log, 2))
        RepeRPC._close_connection!(conn)
        @test log == [1, 2]
        RepeRPC._close_connection!(conn)                       # idempotent
        @test log == [1, 2]
        RepeRPC.on_close(conn, () -> push!(log, 3))            # registering after close runs now
        @test log == [1, 2, 3]

        # A throwing callback is isolated (logs an error) and does not block others.
        conn2 = RepeRPC.ServerConnection()
        seen = Int[]
        RepeRPC.on_close(conn2, () -> error("boom"))
        RepeRPC.on_close(conn2, () -> push!(seen, 9))
        RepeRPC._close_connection!(conn2)
        @test seen == [9]
    end

    @testset "connection teardown reclaims the sessions it owns" begin
        # Drive `_svs_open` directly to assert the wiring: an open registers the
        # connection hook, and closing the connection removes every session that
        # connection owns (one hook regardless of how many streams it opened).
        opts = RepeRPC.StreamOpts(compression = :none)
        req = RepeRPC.Message(id = UInt64(1), query = "", body = UInt8[])
        table = RepeRPC._SessionTable()
        conn = RepeRPC.ServerConnection()
        RepeRPC._svs_open(table, _svs_resolve, opts, Dict("resource" => "dict"), req, conn)
        RepeRPC._svs_open(table, _svs_resolve, opts, Dict("resource" => "typed"), req, conn)
        @test length(table.sessions) == 2
        @test conn.id ∈ table.hooked_conns
        RepeRPC._close_connection!(conn)
        @test isempty(table.sessions)                       # both reclaimed
        @test conn.id ∉ table.hooked_conns

        # An open on an already-closed connection frees its session immediately,
        # so a drop racing the open never orphans it.
        closed = RepeRPC.ServerConnection()
        RepeRPC._close_connection!(closed)
        RepeRPC._svs_open(table, _svs_resolve, opts, Dict("resource" => "dict"), req, closed)
        @test isempty(table.sessions)
    end

    @testset "a dropped connection frees its session immediately" begin
        # Idle reaping disabled, so only the connection teardown hook can free the
        # abandoned session; max_sessions = 1 makes that observable through a later
        # open from a fresh connection.
        opts = RepeRPC.StreamOpts(chunk_bytes = 8 * 1024, compression = :none,
                               session_depth = 2, max_sessions = 1, idle_timeout = Inf)
        port = _free_port()
        srv = RepeRPC.Server("127.0.0.1", port)
        RepeRPC.serve_value_stream(srv, _svs_resolve; opts = opts)
        RepeRPC.listen(srv; async = true)
        RepeRPC.wait_for_server("127.0.0.1", port)
        try
            a = RepeRPC.Client("127.0.0.1", port)
            RepeRPC.connect(a)
            RepeRPC._open_stream(a, "dict", nothing)                          # takes the only slot
            @test_throws Exception RepeRPC._open_stream(a, "dict", nothing)   # cap is full
            RepeRPC.disconnect(a)                                             # drop without cancel

            # The server reclaims A's session in its connection teardown; with idle
            # reaping off, a fresh connection's open can only succeed because of it.
            b = RepeRPC.Client("127.0.0.1", port)
            RepeRPC.connect(b)
            opened = false
            for _ in 1:60
                try
                    RepeRPC._open_stream(b, "dict", nothing)
                    opened = true
                    break
                catch
                    sleep(0.05)
                end
            end
            @test opened
            RepeRPC.disconnect(b)
        finally
            RepeRPC.stop(srv)
        end
    end

    @testset "on_server_stop runs hooks once" begin
        srv = RepeRPC.Server("127.0.0.1", 0)
        fired = Int[]
        RepeRPC.on_server_stop(srv, () -> push!(fired, 1))
        RepeRPC.on_server_stop(srv, () -> push!(fired, 2))
        srv.running = true                  # simulate a started server so stop() runs hooks
        RepeRPC.stop(srv)
        @test fired == [1, 2]
        srv.running = true                  # a second stop must not re-run popped hooks
        RepeRPC.stop(srv)
        @test fired == [1, 2]
    end

    @testset "server stop drains sessions regardless of lifetime" begin
        # Regression: a server whose whole lifetime is shorter than a sweep
        # interval must still have its sessions drained on stop. The drain is
        # delivered by the `on_server_stop` hook, so it does not depend on the
        # reaper ever observing the server running. Exercised for both the
        # no-idle-reaping (Inf) and idle-reaping (finite) configurations.
        req = RepeRPC.Message(id = UInt64(1), query = "", body = UInt8[])
        for idle in (Inf, 3600.0)
            opts = RepeRPC.StreamOpts(compression = :none, idle_timeout = idle)
            srv = RepeRPC.Server("127.0.0.1", 0)
            table = RepeRPC._SessionTable()
            RepeRPC._start_session_reaper(srv, table, opts)
            RepeRPC._svs_open(table, _svs_resolve, opts, Dict("resource" => "dict"),
                           req, RepeRPC.ServerConnection())
            @test length(table.sessions) == 1
            srv.running = true
            RepeRPC.stop(srv)
            @test isempty(table.sessions)   # drained synchronously by the stop hook
        end
    end

    @testset "_invoke_handler passes conn only to non-vararg 3-arg handlers" begin
        conn = RepeRPC.ServerConnection()
        req = RepeRPC.Message(id = UInt64(1), query = "", body = UInt8[])
        @test RepeRPC._invoke_handler((p, r) -> :two, nothing, req, conn) === :two
        @test RepeRPC._invoke_handler((p, r, c) -> (c === conn ? :got : :wrong), nothing, req, conn) === :got
        h_default = (p, r, c = nothing) -> (c === conn ? :got : :default)
        @test RepeRPC._invoke_handler(h_default, nothing, req, conn) === :got
        @test RepeRPC._invoke_handler(h_default, nothing, req, nothing) === :default
        # A vararg handler must NOT be handed conn (it would land in `rest`).
        h_va = (p, r, rest...) -> (isempty(rest) ? :no_extra : :got_extra)
        @test RepeRPC._invoke_handler(h_va, nothing, req, conn) === :no_extra
    end

    # ---- Fleet-level SVS pull wrappers ---------------------------------------
    #
    # The fleet methods are thin envelopes over the single-client pulls: they look
    # up the node, connect-if-needed, and capture the outcome (or error) into a
    # `RemoteResult` without retrying. These drive a real one-node fleet against a
    # live SVS server to confirm the wrapping, the no-throw error capture, and that
    # an unknown node is captured as a failed RemoteResult (not thrown).
    @testset "Fleet SVS: pull_value (generic + typed)" begin
        for comp in (:none, :zstd)
            port = _free_port()
            srv = RepeRPC.Server("127.0.0.1", port)
            RepeRPC.serve_value_stream(srv, _svs_resolve; opts = _svs_opts(comp))
            RepeRPC.listen(srv; async = true)
            RepeRPC.wait_for_server("127.0.0.1", port)
            fleet = RepeRPC.Fleet([RepeRPC.NodeConfig("127.0.0.1", port; name = "svs-1")])
            RepeRPC.connect!(fleet)
            try
                r = RepeRPC.pull_value(fleet, "svs-1", "dict")
                @test RepeRPC.succeeded(r)
                @test r.node == "svs-1"
                @test r.elapsed >= 0
                @test r[]["samples"] == _svs_dict()["samples"]

                rt = RepeRPC.pull_value(SvsTyped, fleet, "svs-1", "typed")
                @test RepeRPC.succeeded(rt)
                ref = _svs_typed()
                @test rt[].id == ref.id
                @test rt[].label == ref.label
                @test rt[].samples == ref.samples

                # An unknown node is captured as a failed RemoteResult (a KeyError),
                # not thrown, so one bad name can't abort a batch of pulls; r[] rethrows.
                rmiss = RepeRPC.pull_value(fleet, "missing-node", "dict")
                @test RepeRPC.failed(rmiss)
                @test rmiss.error isa KeyError
                @test rmiss.node == "missing-node"
                @test_throws KeyError rmiss[]

                # A resolve miss surfaces as a failed RemoteResult, not a throw
                # (no-retry: the wrapper captures it, mirroring `call`).
                rbad = RepeRPC.pull_value(fleet, "svs-1", "no-such-resource")
                @test RepeRPC.failed(rbad)
                @test rbad.error !== nothing
            finally
                RepeRPC.disconnect!(fleet)
                RepeRPC.stop(srv)
            end
        end
    end

    @testset "Fleet SVS: file modes (zstd) and atomic commit" begin
        port = _free_port()
        srv = RepeRPC.Server("127.0.0.1", port)
        RepeRPC.serve_value_stream(srv, _svs_resolve; opts = _svs_opts(:zstd))
        RepeRPC.listen(srv; async = true)
        RepeRPC.wait_for_server("127.0.0.1", port)
        fleet = RepeRPC.Fleet([RepeRPC.NodeConfig("127.0.0.1", port; name = "svs-1")])
        RepeRPC.connect!(fleet)
        try
            mktempdir() do dir
                bp = joinpath(dir, "out.beve")
                rb = RepeRPC.pull_to_beve_file(fleet, "svs-1", "dict", bp)
                @test RepeRPC.succeeded(rb)
                @test rb[] === nothing              # successful file pull carries no value
                @test isfile(bp)
                @test !isfile(bp * ".svspart")
                @test BeveFormat.read_beve_file(bp)["samples"] == _svs_dict()["samples"]

                zp = joinpath(dir, "out.beve.zst")
                rz = RepeRPC.pull_to_beve_zst_file(fleet, "svs-1", "dict", zp)
                @test RepeRPC.succeeded(rz)
                @test isfile(zp)
                @test !isfile(zp * ".svspart")
                @test BeveFormat.read_beve_zstd_file(zp)["samples"] == _svs_dict()["samples"]
            end
        finally
            RepeRPC.disconnect!(fleet)
            RepeRPC.stop(srv)
        end
    end
end

@testset "SVS robustness polish" begin
    @testset "server error surfaces as a structured REPEError with .code" begin
        # A pull for an unresolved resource now raises a typed REPEError carrying the
        # server's error code, so callers can switch on `.code` instead of matching a
        # message substring.
        _with_tcp_svs(_svs_resolve; opts = _svs_opts(:none)) do client
            err = try
                RepeRPC.pull_value(client, "does-not-exist")
                nothing
            catch e
                e
            end
            @test err isa RepeRPC.REPEError
            @test err.code == RepeRPC.EC_METHOD_NOT_FOUND
        end
    end

    @testset "duplicate SVS producer registration warns" begin
        srv = RepeRPC.Server("127.0.0.1", _free_port())
        quiet = RepeRPC.StreamOpts(compression = :none, idle_timeout = Inf)
        RepeRPC.serve_value_stream(srv, _ -> nothing; opts = quiet)
        # The second registration replaces the first; that must be visible.
        @test_logs (:warn,) match_mode = :any RepeRPC.serve_file_stream(srv, _ -> nothing; opts = quiet)
        RepeRPC.stop(srv)   # never started: returns without running the (unused) hooks
    end

    @testset "total_timeout bounds the whole transfer" begin
        # Unit: a ChunkReader whose deadline already passed must abort in `_fetch!`
        # *before* issuing any `next` round trip (the client below would otherwise
        # hand back chunks, never the drop). This exercises the whole-transfer guard
        # in isolation, independent of producer cadence.
        r = RepeRPC.ChunkReader(_DropClient(1_000_000), UInt64(1), nothing;
                             deadline = time() - 1.0, total_timeout = 0.25)
        @test_throws RepeRPC.TimeoutError RepeRPC._fetch!(r)

        # Integration: a generous budget does not interfere with a normal pull.
        _with_tcp_svs(_svs_resolve; opts = _svs_opts(:none)) do client
            v = RepeRPC.pull_value(client, "dict"; total_timeout = 120.0)
            @test v["samples"] == _svs_dict()["samples"]
        end
    end
end

# A cancel predicate that returns `false` for its first `n-1` polls and `true`
# thereafter. Each pull polls `cancel` once before the open round trip, then once
# per chunk in `_fetch!`, so `_cancel_after(2)` fires on the first fetch (after the
# stream opened) and `_cancel_after(3)` fires on the second fetch (genuinely
# mid-stream, after real data has flowed).
_cancel_after(n::Int) = (c = Ref(0); () -> (c[] += 1; c[] >= n))

@testset "SVS cooperative cancellation" begin
    @testset "cancel aborts in _fetch! (unit), and wins over the deadline" begin
        # Fires before any `next`: the drop client would otherwise hand back chunks
        # forever, so a throw proves the cancel guard ran first.
        r = RepeRPC.ChunkReader(_DropClient(1_000_000), UInt64(7), nothing;
                             cancel = () -> true, resource = "res")
        err = try; RepeRPC._fetch!(r); nothing; catch e; e; end
        @test err isa RepeRPC.CancelledError
        @test err.stream_id == 7
        @test err.resource == "res"

        # Checked before the deadline, so an explicit cancel wins over a
        # coincidentally-expired budget (the caller gets the clearer error).
        r2 = RepeRPC.ChunkReader(_DropClient(1_000_000), UInt64(8), nothing;
                              cancel = () -> true, deadline = time() - 1.0, total_timeout = 0.25)
        @test_throws RepeRPC.CancelledError RepeRPC._fetch!(r2)

        # A predicate that never fires does not interfere: the fetch completes.
        r3 = RepeRPC.ChunkReader(_DropClient(1_000_000), UInt64(9), nothing; cancel = () -> false)
        @test RepeRPC._fetch!(r3) === nothing
    end

    @testset "cancel reaches _fetch! on every single-client path" begin
        # Genuinely mid-stream (second fetch) on a multi-chunk uncompressed pull:
        # the error carries the real stream_id, and the connection is left usable.
        _with_tcp_svs(_svs_resolve; opts = _svs_opts(:none)) do client
            err = try
                RepeRPC.pull_value(client, "dict"; cancel = _cancel_after(3))
                nothing
            catch e
                e
            end
            @test err isa RepeRPC.CancelledError
            @test err.stream_id != 0                       # aborted after the stream opened
            @test err.resource == "dict"
            # Cleanup ran (session released, decoder freed): a normal pull still works.
            @test RepeRPC.pull_value(client, "dict")["samples"] == _svs_dict()["samples"]
        end

        # First-fetch abort across every entry point and both compressions. File
        # helpers must leave neither the final file nor the `.svspart` temp.
        for comp in (:none, :zstd)
            _with_tcp_svs(_svs_resolve; opts = _svs_opts(comp)) do client
                @test_throws RepeRPC.CancelledError RepeRPC.pull_value(client, "dict"; cancel = _cancel_after(2))
                @test_throws RepeRPC.CancelledError RepeRPC.pull_value(SvsTyped, client, "typed";
                                                                 cancel = _cancel_after(2))
                mktempdir() do dir
                    p = joinpath(dir, "c.out")
                    @test_throws RepeRPC.CancelledError RepeRPC.pull_to_file(client, "dict", p;
                                                                       cancel = _cancel_after(2))
                    @test !isfile(p)
                    @test !isfile(p * ".svspart")
                end
            end
        end

        # The `.beve`/`.beve.zst` helpers require a compressed producer.
        _with_tcp_svs(_svs_resolve; opts = _svs_opts(:zstd)) do client
            mktempdir() do dir
                bp = joinpath(dir, "c.beve")
                @test_throws RepeRPC.CancelledError RepeRPC.pull_to_beve_file(client, "dict", bp;
                                                                        cancel = _cancel_after(2))
                @test !isfile(bp) && !isfile(bp * ".svspart")

                zp = joinpath(dir, "c.beve.zst")
                @test_throws RepeRPC.CancelledError RepeRPC.pull_to_beve_zst_file(client, "dict", zp;
                                                                            cancel = _cancel_after(2))
                @test !isfile(zp) && !isfile(zp * ".svspart")
            end
        end
    end

    @testset "cancel before open skips the open round trip" begin
        # A predicate already true at entry aborts before `_open_stream`, so no
        # session is created; the error's stream_id is 0 to mark "before open".
        _with_tcp_svs(_svs_resolve; opts = _svs_opts(:none)) do client
            err = try
                RepeRPC.pull_value(client, "dict"; cancel = () -> true)
                nothing
            catch e
                e
            end
            @test err isa RepeRPC.CancelledError
            @test err.stream_id == 0
            @test err.resource == "dict"
        end
    end

    @testset "Fleet SVS: cancel is captured and is distinct from a fault" begin
        port = _free_port()
        srv = RepeRPC.Server("127.0.0.1", port)
        RepeRPC.serve_value_stream(srv, _svs_resolve; opts = _svs_opts(:zstd))
        RepeRPC.listen(srv; async = true)
        RepeRPC.wait_for_server("127.0.0.1", port)
        fleet = RepeRPC.Fleet([RepeRPC.NodeConfig("127.0.0.1", port; name = "svs-1")])
        RepeRPC.connect!(fleet)
        try
            r = RepeRPC.pull_value(fleet, "svs-1", "dict"; cancel = _cancel_after(2))
            @test RepeRPC.failed(r)                       # a cancel is still "not a success"
            @test RepeRPC.cancelled(r)
            @test !RepeRPC.succeeded(r)
            @test r.error isa RepeRPC.CancelledError

            # The recommended filter separates a caller-cancel from a producer fault.
            rbad = RepeRPC.pull_value(fleet, "svs-1", "no-such-resource")
            @test RepeRPC.failed(rbad) && !RepeRPC.cancelled(rbad)     # a genuine fault
            @test !(RepeRPC.failed(r) && !RepeRPC.cancelled(r))        # the cancel is excluded

            mktempdir() do dir
                p = joinpath(dir, "c.beve")
                rc = RepeRPC.pull_to_beve_file(fleet, "svs-1", "dict", p; cancel = _cancel_after(2))
                @test RepeRPC.cancelled(rc)
                @test !isfile(p) && !isfile(p * ".svspart")
            end
        finally
            RepeRPC.disconnect!(fleet)
            RepeRPC.stop(srv)
        end
    end

    @testset "CancelledError shape and identity" begin
        e = RepeRPC.CancelledError(UInt64(42); resource = "/data/x")
        s = sprint(showerror, e)
        @test occursin("CancelledError", s)
        @test occursin("cancelled by caller", s)
        @test occursin("42", s) && occursin("/data/x", s)
        # Distinct types so catch sites separate a cancel from a timeout / Ctrl-C.
        @test !(e isa RepeRPC.TimeoutError)
        @test !(e isa InterruptException)
        # A pre-open cancel (stream_id 0) does not print a stream_id.
        @test !occursin("stream_id", sprint(showerror, RepeRPC.CancelledError(0)))
    end

    @testset "compressed reader close frees the decoder (no DCtx leak)" begin
        # TranscodingStreams installs no finalizer, so the zstd decoder's native
        # `ZSTD_DCtx` is freed only on `close`. Every pull now closes its reader in a
        # `finally`; that close cascades to the underlying ChunkReader, which must
        # define a (no-op) `close` or the cascade would `MethodError` and silently
        # reinstate the leak. Guard both the no-op and the cascade here directly.
        raw = RepeRPC.ChunkReader(_DropClient(1), UInt64(1), nothing)
        @test close(raw) === nothing               # ChunkReader.close is a defined no-op
        @test close(raw) === nothing               # idempotent

        dec = RepeRPC._zstd_decompressor_stream(RepeRPC.ChunkReader(_DropClient(1), UInt64(2), nothing))
        @test close(dec) === nothing               # frees the DCtx, cascades to the no-op
        @test close(dec) === nothing               # idempotent, never throws
    end
end
