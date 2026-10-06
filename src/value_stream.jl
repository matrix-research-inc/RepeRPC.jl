# Serialized Value Stream (SVS) — streaming a serialized, optionally
# zstd-compressed in-memory value over ordinary REPE request/response.
#
# SVS moves a large value (multi-GB even compressed) from a producer to a
# consumer without either side materializing a full serialized or compressed
# copy. The value is BEVE-serialized and, on the compressed path, zstd-encoded
# as it is produced; the bytes cross the wire as a sequence of opaque chunks
# pulled one round trip at a time, so flow control is the request/response
# cadence itself — no ACK window, no unbounded queue. The receiver chooses, per
# transfer, to write the compressed bytes to a file, decompress them to a file,
# or stream-decode them into a value.
#
# The wire contract (routes, the `stream_id` encoding, the `last`-flag placement,
# the `format`/`compression` tags) is the cross-language SVS spec in the REPE org
# repository; everything else here (chunk sizing, the bounded producer channel,
# the one-chunk lookahead that detects the final chunk) is local engine policy.
#
# Direction is download-only: the server produces, the client pulls. The blocking
# `next` handler parks its task on `take!` until the producer has the next chunk
# ready — that park *is* the backpressure. `take!` yields cooperatively, so the
# producer task (and every other connection) keeps running; this works on the TCP
# `Server` (one task per connection) and the WebSocket server (one task per
# request) alike, since both dispatch through `_process_request_pipeline`.

"""
    SVS_VERSION

The SVS contract version this implementation speaks, reported in the `open`
response and validated by the client.
"""
const SVS_VERSION = UInt8(1)

# Reserved `/_svs` namespace. Routes beginning with `_` are reserved for REPE
# protocol extensions; SVS claims `/_svs`.
const SVS_OPEN = "/_svs/open"
const SVS_NEXT = "/_svs/next"
const SVS_CANCEL = "/_svs/cancel"

const _COMPRESSION_NONE = UInt8(0)
const _COMPRESSION_ZSTD = UInt8(1)

"""
    StreamOpts(; chunk_bytes = 1 MiB, compression = :zstd, zstd_level = 3,
               session_depth = 4, max_sessions = 256, idle_timeout = 300.0)

Local producer-side knobs for an SVS download. None of these are on the wire, so
a producer and a consumer interoperate regardless of how they are set (SVS §4).

- `chunk_bytes`: target size of each chunk pushed into the session channel. The
  consumer issues one round trip per chunk, so larger chunks mean fewer round
  trips: for GB-scale transfers over TCP, 8–16 MiB is a good choice. The "keep it
  small" consideration applies only to the WebSocket transport, where each chunk
  becomes one binary frame and should stay modest (a few MiB). 1 MiB default.
- `compression`: `:zstd` (default) or `:none`. `:zstd` requires `CodecZstd` to be
  loaded (`using CodecZstd`).
- `zstd_level`: zstd level when `compression = :zstd`.
- `session_depth`: bounded depth of the per-stream producer channel. This is
  where memory is bounded and backpressure lives: the serializer parks once this
  many chunks are buffered ahead of the consumer.
- `max_sessions`: cap on concurrently open producer sessions on this server. A
  fresh `open` past the cap is rejected (rather than spawning an unbounded number
  of producer tasks, each pinning its resolved value). Bounds the blast radius of
  abandoned pulls and of a peer that opens streams it never drains.
- `idle_timeout`: seconds a session may sit without a `next` before the reaper
  reclaims it (closing its channel, which wakes and ends the producer task and
  releases the resolved value). A dropped connection already frees its sessions at
  once through the server's connection-teardown hook, and cooperative `cancel` and
  the terminal `last` chunk free a session immediately; this timeout is the
  backstop for the remaining case — a connection left open while its pull is
  abandoned (no `next`, no `cancel`). `Inf` disables idle reaping (sessions are
  still freed on disconnect and drained when the server stops).
"""
struct StreamOpts
    chunk_bytes::Int
    compression::UInt8
    zstd_level::Int
    session_depth::Int
    max_sessions::Int
    idle_timeout::Float64

    function StreamOpts(; chunk_bytes::Integer = 1024 * 1024,
                        compression::Union{Symbol,Integer} = :zstd,
                        zstd_level::Integer = 3,
                        session_depth::Integer = 4,
                        max_sessions::Integer = 256,
                        idle_timeout::Real = 300.0)
        chunk_bytes < 1 && throw(ArgumentError("chunk_bytes must be positive, got $chunk_bytes"))
        session_depth < 1 && throw(ArgumentError("session_depth must be positive, got $session_depth"))
        max_sessions < 1 && throw(ArgumentError("max_sessions must be positive, got $max_sessions"))
        idle_timeout > 0 ||
            throw(ArgumentError("idle_timeout must be positive (use Inf to disable reaping), got $idle_timeout"))
        comp = _compression_code(compression)
        new(Int(chunk_bytes), comp, Int(zstd_level), Int(session_depth),
            Int(max_sessions), Float64(idle_timeout))
    end
end

function _compression_code(c::Symbol)
    c === :none && return _COMPRESSION_NONE
    c === :zstd && return _COMPRESSION_ZSTD
    throw(ArgumentError("compression must be :none or :zstd, got :$c"))
end
function _compression_code(c::Integer)
    (c == 0 || c == 1) || throw(ArgumentError("compression code must be 0 (none) or 1 (zstd), got $c"))
    return UInt8(c)
end

"""
    FileResource(path; format = BODY_BEVE, compression = :auto)

Describes an on-disk file to stream verbatim through [`serve_file_stream`](@ref),
bypassing the serialize/deserialize round trip of [`serve_value_stream`](@ref).

- `path`: the file whose bytes are streamed.
- `format`: the `BodyFormat` to advertise in the `open` response (defaults to
  `BODY_BEVE`, since the common case is a `.beve`/`.beve.zst` file). It changes
  nothing on the wire beyond the tag a consumer validates against.
- `compression`: the compression the file is **already stored in** — `:none`,
  `:zstd`, or `:auto` (the default: `:zstd` when `path` ends in `.zst`, else
  `:none`). The producer compares this with the stream's wire compression
  (`StreamOpts.compression`) to decide whether to pass the bytes through
  untouched, compress them on the fly, or decompress them on the fly. A
  `:zstd`-stored file served on a `:zstd` wire is a pure file read — no codec, no
  BEVE — which is the whole point of the file-stream mode.

A `serve_file_stream` resolver may also return a bare path `String`, which is
equivalent to `FileResource(path)`.
"""
struct FileResource
    path::String
    format::UInt16
    stored_compression::UInt8
end

function FileResource(path::AbstractString; format::Union{Integer,BodyFormat} = BODY_BEVE,
                      compression::Union{Symbol,Integer} = :auto)
    comp = compression === :auto ?
           (endswith(lowercase(String(path)), ".zst") ? _COMPRESSION_ZSTD : _COMPRESSION_NONE) :
           _compression_code(compression)
    return FileResource(String(path), UInt16(format), comp)
end

# ---- wire message bodies (BEVE) ----------------------------------------------
#
# Field names and integer widths match the SVS spec (and the repe-rs reference
# structs) so the BEVE object keys line up across languages.

struct _OpenRequest
    resource::String
end

struct _OpenResponse
    version::UInt8
    stream_id::UInt64
    format::UInt16
    compression::UInt8
end

struct _NextRequest
    stream_id::UInt64
end

struct _CancelRequest
    stream_id::UInt64
    reason::String
end

struct _CancelAck
    ok::Bool
end

# ---- zstd codec hooks (provided by the CodecZstd package extension) -----------
#
# The streaming zstd codec lives behind the optional `CodecZstd` weakdep so the
# core stays free of the libzstd C dependency. `RepeRPCCodecZstdExt` adds the only
# methods of these functions; until it loads they have none, and `_require_zstd`
# turns that into a clear error rather than a `MethodError`.

function _zstd_compressor_stream end
function _zstd_decompressor_stream end

_zstd_available() = !isempty(methods(_zstd_compressor_stream))

function _require_zstd()
    _zstd_available() ||
        error("REPE SVS: zstd compression requires the CodecZstd package. Run `using CodecZstd` " *
              "(or choose `compression = :none`).")
    return nothing
end

#==============================================================================#
# Server — producer session
#==============================================================================#

# One item in the producer→handler channel. Terminating the stream with an
# explicit `_End`/`_Fail` (rather than relying on the channel closing) keeps the
# "did it finish cleanly?" answer ordered with the chunks: a truncated production
# surfaces as `_Fail`, never as a silently-short clean end.
struct _Chunk
    bytes::Vector{UInt8}
end
struct _End end
struct _Fail
    msg::String
end
const _Msg = Union{_Chunk,_End,_Fail}

"""
    BoundedSink <: IO

A write sink that batches the producer's byte stream into `chunk_bytes`-sized
chunks and pushes each into a bounded `Channel`. `put!` parks the writing task
once the channel is full — that park is the producer-side backpressure. `close`
is a deliberate no-op (a wrapping `ZstdCompressorStream`'s `close` cascades here
after flushing its epilogue; the channel must outlive it to carry the final
chunk and the `_End` marker, which `_produce` sends explicitly).
"""
mutable struct BoundedSink <: IO
    chan::Channel{_Msg}
    buf::Vector{UInt8}
    chunk_bytes::Int
end

function BoundedSink(chan::Channel{_Msg}, chunk_bytes::Int)
    buf = Vector{UInt8}()
    sizehint!(buf, chunk_bytes)
    return BoundedSink(chan, buf, chunk_bytes)
end

function _emit!(s::BoundedSink)
    put!(s.chan, _Chunk(s.buf))           # blocks (backpressure) when the channel is full
    s.buf = Vector{UInt8}()
    sizehint!(s.buf, s.chunk_bytes)
    return nothing
end

# Push the trailing partial chunk, if any. Does not close the channel.
function _flush_remaining!(s::BoundedSink)
    isempty(s.buf) || _emit!(s)
    return nothing
end

function _append!(s::BoundedSink, p::Ptr{UInt8}, n::Int)
    while n > 0
        space = s.chunk_bytes - length(s.buf)
        space == 0 && (_emit!(s); space = s.chunk_bytes)
        take = min(space, n)
        old = length(s.buf)
        resize!(s.buf, old + take)
        GC.@preserve s unsafe_copyto!(pointer(s.buf, old + 1), p, take)
        p += take
        n -= take
        length(s.buf) >= s.chunk_bytes && _emit!(s)
    end
    return nothing
end

function Base.unsafe_write(s::BoundedSink, p::Ptr{UInt8}, n::UInt)
    _append!(s, p, Int(n))
    return Int(n)
end

function Base.write(s::BoundedSink, b::UInt8)
    push!(s.buf, b)
    length(s.buf) >= s.chunk_bytes && _emit!(s)
    return 1
end

Base.flush(::BoundedSink) = nothing       # a chunk boundary is a size decision, not a flush
Base.close(::BoundedSink) = nothing
Base.isopen(::BoundedSink) = true
Base.iswritable(::BoundedSink) = true

# Run the (serialize → optional compress → chunk) pipeline to completion on the
# producer task, reporting the outcome through the channel. Always ends with
# exactly one `_End` or `_Fail`. A `put!` onto a closed channel (the consumer
# cancelled/disconnected) throws `InvalidStateException`, which aborts the pass.
function _produce(value, chan::Channel{_Msg}, opts::StreamOpts)
    sink = BoundedSink(chan, opts.chunk_bytes)
    try
        if opts.compression == _COMPRESSION_NONE
            BeveFormat.beve_value!(BeveFormat.BeveSerializer(sink), value)
        else
            zio = _zstd_compressor_stream(sink, opts.zstd_level)
            BeveFormat.beve_value!(BeveFormat.BeveSerializer(zio), value)
            close(zio)                     # flush the zstd epilogue into `sink`
        end
        _flush_remaining!(sink)
        put!(chan, _End())
    catch e
        # Best-effort failure marker; if the channel is already closed (consumer
        # gone) the marker is moot and the InvalidStateException is swallowed.
        try
            put!(chan, _Fail(sprint(showerror, e)))
        catch
        end
    end
    return nothing
end

# Copy every byte of `src` into `dst` in bounded blocks, never materializing the
# whole stream — the file-producer counterpart to the value producer's
# `beve_value!` walk. `dst` is the bounded chunk sink (or a codec wrapping it).
function _copy_io!(dst::IO, src::IO; blk::Int = 1 << 16)
    buf = Vector{UInt8}(undef, blk)
    while !eof(src)
        n = readbytes!(src, buf)
        n > 0 && GC.@preserve buf unsafe_write(dst, pointer(buf), UInt(n))
    end
    return nothing
end

# Decide how to reconcile a file's stored compression with the stream's wire
# compression. Equal ⇒ pass the bytes through untouched (no codec); otherwise
# transcode in the one direction the pair implies.
function _file_transcode(stored::UInt8, wire::UInt8)
    stored == wire && return :passthrough
    return wire == _COMPRESSION_ZSTD ? :compress : :decompress
end

# Stream an existing file's bytes through the bounded chunk sink, applying only the
# codec the `transcode` plan calls for (chosen by `_file_transcode`). No BEVE pass
# in any branch — the file already holds the serialized form. Mirrors `_produce`'s
# channel contract: always ends with exactly one `_End`/`_Fail`, and a `put!` onto
# a channel the consumer closed aborts the pass.
function _produce_file(path::AbstractString, transcode::Symbol, chan::Channel{_Msg}, opts::StreamOpts)
    sink = BoundedSink(chan, opts.chunk_bytes)
    try
        if transcode === :passthrough
            open(io -> _copy_io!(sink, io), path)              # raw file bytes, no codec
        elseif transcode === :compress
            zio = _zstd_compressor_stream(sink, opts.zstd_level)
            open(io -> _copy_io!(zio, io), path)               # compress the stored bytes on the fly
            close(zio)                                         # flush the zstd epilogue into `sink`
        else # :decompress
            open(path) do io
                _copy_io!(sink, _zstd_decompressor_stream(io)) # decompress the stored bytes on the fly
            end
        end
        _flush_remaining!(sink)
        put!(chan, _End())
    catch e
        try
            put!(chan, _Fail(sprint(showerror, e)))
        catch
        end
    end
    return nothing
end

# Per-stream consumer-facing state: the channel plus the one-chunk lookahead used
# to mark the final chunk with `last = true`. `pulling`/`last_touch` drive the idle
# reaper (see `_reap_idle_sessions!`): `last_touch` is the monotonic `time_ns()` of
# session creation or the most recent `next`, and `pulling` is held true while a
# `next` is in flight so a pull merely parked on a slow producer is never reaped.
mutable struct _Session
    chan::Channel{_Msg}
    task::Task
    lookahead::Union{Nothing,_Chunk}
    done::Bool
    pulling::Bool
    last_touch::UInt64
    conn_id::UInt64               # owning connection (0 = none); see `_remove_connection_sessions!`
    lock::ReentrantLock
end
_Session(chan::Channel{_Msg}, task::Task, conn_id::UInt64 = UInt64(0)) =
    _Session(chan, task, nothing, false, false, time_ns(), conn_id, ReentrantLock())

# Raised by `_pull!` when the producer reported a failure (`_Fail`) instead of a
# clean end. Carries the producer's already-formatted message so `_svs_next` can
# tag the response `EC_SVS_PRODUCER_ERROR` (a producer-side fault, distinct from a
# malformed/late client request) and forward the message without re-stringifying.
struct _SvsProducerFault <: Exception
    msg::String
end

function _recv(s::_Session)::_Msg
    try
        return take!(s.chan)
    catch e
        # The producer always sends `_End`/`_Fail` before its task ends, so a bare
        # close means the producer task vanished (it threw before reporting).
        e isa InvalidStateException && return _Fail("svs producer ended without completing")
        rethrow(e)
    end
end

# Return `(chunk, last)` or throw with the producer's failure message. Blocks
# until the producer has the *following* chunk (or a terminal marker) ready — that
# block is the backpressure. A truncated production throws rather than masquerading
# as a clean `last`.
function _pull!(s::_Session)
    current = s.lookahead
    if current === nothing
        m = _recv(s)
        m isa _End && return (UInt8[], true)
        m isa _Fail && throw(_SvsProducerFault(m.msg))
        current = m::_Chunk
    else
        s.lookahead = nothing
    end
    nxt = _recv(s)
    if nxt isa _Chunk
        s.lookahead = nxt
        return (current.bytes, false)
    elseif nxt isa _End
        return (current.bytes, true)
    else
        throw(_SvsProducerFault((nxt::_Fail).msg))
    end
end

# Table of live download sessions, shared by the open/next/cancel handlers.
# `hooked_conns` records which connections already have a teardown callback
# registered, so a connection that opens many streams registers exactly one
# (the callback reaps all of that connection's sessions by `conn_id`).
struct _SessionTable
    next_id::Threads.Atomic{UInt64}
    sessions::Dict{UInt64,_Session}
    hooked_conns::Set{UInt64}
    lock::ReentrantLock
end
_SessionTable() = _SessionTable(Threads.Atomic{UInt64}(1), Dict{UInt64,_Session}(),
                                Set{UInt64}(), ReentrantLock())

_new_id(t::_SessionTable) = Threads.atomic_add!(t.next_id, UInt64(1))
_get_session(t::_SessionTable, id) = lock(() -> get(t.sessions, id, nothing), t.lock)

# Drop and release a session. Closing the channel wakes a producer task parked on
# a full `put!` (it throws InvalidStateException, treated as cancellation) and lets
# the resolved value it referenced be collected.
function _remove_session!(t::_SessionTable, id; reason::AbstractString = "removed")
    removed = lock(t.lock) do
        s = pop!(t.sessions, id, nothing)
        s === nothing && return false
        try; close(s.chan); catch; end
        return true
    end
    removed && @debug "SVS session closed" stream_id = id reason
    return nothing
end

# Reclaim every session that has gone untouched longer than `timeout_ns`. Skips a
# session with a `next` in flight (`pulling`): such a pull is waiting on a slow
# producer, not idle. The `pulling`/`last_touch` reads cross the session lock
# without holding it — deliberately, so the reaper never blocks behind a parked
# `next` that owns that lock; the fields are plain word-sized reads and a stale
# value at worst defers a reap by one sweep. Returns the count reclaimed.
function _reap_idle_sessions!(t::_SessionTable, timeout_ns::UInt64)
    now = time_ns()
    n = lock(t.lock) do
        victims = UInt64[]
        for (id, s) in t.sessions
            s.pulling && continue
            (now - s.last_touch) > timeout_ns && push!(victims, id)
        end
        for id in victims
            s = pop!(t.sessions, id, nothing)
            s === nothing || (try; close(s.chan); catch; end)
        end
        length(victims)
    end
    n > 0 && @debug "SVS reaped idle sessions" count = n  # logged outside the lock
    return n
end

# Release all sessions (server shutdown). Each channel close ends its producer.
function _drain_all_sessions!(t::_SessionTable)
    lock(t.lock) do
        for (_, s) in t.sessions
            try; close(s.chan); catch; end
        end
        empty!(t.sessions)
    end
    return nothing
end

# Reclaim every session a now-closed connection still owns. Wired to the server's
# per-connection `on_close` hook (see `serve_value_stream`), this frees abandoned
# pulls the instant the connection drops, instead of waiting out `idle_timeout`.
# The snapshot of keys is taken before mutating, so the iteration is stable.
function _remove_connection_sessions!(t::_SessionTable, conn_id::UInt64)
    n = lock(t.lock) do
        delete!(t.hooked_conns, conn_id)
        c = 0
        for id in collect(keys(t.sessions))
            s = t.sessions[id]
            if s.conn_id == conn_id
                delete!(t.sessions, id)
                try; close(s.chan); catch; end
                c += 1
            end
        end
        c
    end
    n > 0 && @debug "SVS released sessions on connection close" conn_id count = n
    return nothing
end

function _svs_error(req::Message, ec::ErrorCode, msg::AbstractString)
    return Message(id = req.header.id, query = "", body = String(msg),
                   body_format = UInt16(BODY_UTF8), ec = UInt32(ec))
end

function _beve_response(req::Message, value)
    return Message(id = req.header.id, query = "", body = BeveFormat.to_beve(value),
                   body_format = UInt16(BODY_BEVE))
end

# Frame a `next` response: the `last` flag rides in a 1-byte raw-binary query so it
# never wraps the zero-copy chunk body, which stays raw binary (SVS §2.2).
function _chunk_response(req::Message, chunk::Vector{UInt8}, last::Bool)
    return Message(id = req.header.id,
                   query = String(UInt8[last ? 0x01 : 0x00]),
                   body = chunk,
                   query_format = UInt16(QUERY_RAW_BINARY),
                   body_format = UInt16(BODY_RAW_BINARY))
end

# A resolved producer plan: how to drive the session task and what `format` /
# `compression` to advertise in the `open` response. The value and file producers
# differ only in this plan; the session machinery below is shared.
struct _StreamPlan
    produce::Function       # (chan::Channel{_Msg}) -> runs to completion, ends with _End/_Fail
    format::UInt16
    compression::UInt8
end

# Pull the `resource` string out of an `open` body, or a ready-to-return error
# `Message` if it is missing/mistyped. Shared by the value and file open handlers.
function _svs_resource(params, req::Message)
    resource = params isa AbstractDict ? get(params, "resource", nothing) : nothing
    resource isa AbstractString && return String(resource)
    return _svs_error(req, EC_INVALID_BODY, "svs open: expected BEVE {resource::String}")
end

# Start a producer session for `plan` and answer the `open`. `plan` is `nothing`
# (the resource did not resolve ⇒ 404) or a ready-to-return error `Message`
# (e.g. a transcode the server cannot perform); both short-circuit. Otherwise the
# slot reservation, id minting, producer launch, and connection-teardown wiring
# are exactly what the value path used, now shared with the file path.
function _svs_start_session(table::_SessionTable, opts::StreamOpts, req::Message,
                            conn::Union{ServerConnection,Nothing},
                            plan::Union{_StreamPlan,Message,Nothing})
    plan === nothing &&
        return _svs_error(req, EC_METHOD_NOT_FOUND, "svs open: unknown resource")
    plan isa Message && return plan

    conn_id = conn === nothing ? UInt64(0) : conn.id
    # Reserve the slot, mint the id, and start the producer atomically under the
    # table lock, so a burst of concurrent `open`s (both transports may dispatch
    # them in parallel) cannot overshoot `max_sessions`. Creating the channel and
    # `@async`-ing the producer are non-blocking, so holding the lock stays cheap.
    # `hook_conn` records whether this is the first session on `conn`, so we
    # register the connection teardown exactly once (outside the lock, below).
    local hook_conn::Bool
    stream_id = lock(table.lock) do
        length(table.sessions) >= opts.max_sessions && return nothing
        id = _new_id(table)
        chan = Channel{_Msg}(opts.session_depth)
        task = @async plan.produce(chan)
        table.sessions[id] = _Session(chan, task, conn_id)
        hook_conn = conn !== nothing && conn_id ∉ table.hooked_conns
        hook_conn && push!(table.hooked_conns, conn_id)
        return id
    end
    if stream_id === nothing
        @warn "SVS open rejected: too many concurrent streams" max_sessions = opts.max_sessions
        return _svs_error(req, EC_APPLICATION_ERROR_BASE,
                          "svs open: too many concurrent streams (max $(opts.max_sessions))")
    end
    @debug "SVS session opened" stream_id conn_id

    # Tie the session(s) to the connection: when it closes, every session it still
    # owns is reclaimed at once. If the connection already closed (a drop raced
    # this open), `on_close` runs the callback now, freeing the session just made.
    hook_conn && on_close(conn, () -> _remove_connection_sessions!(table, conn_id))

    resp = _OpenResponse(SVS_VERSION, stream_id, plan.format, plan.compression)
    return _beve_response(req, resp)
end

function _svs_open(table::_SessionTable, resolve, opts::StreamOpts, params, req::Message,
                   conn::Union{ServerConnection,Nothing} = nothing)
    resource = _svs_resource(params, req)
    resource isa Message && return resource
    value = resolve(resource)
    plan = value === nothing ? nothing :
           _StreamPlan(chan -> _produce(value, chan, opts), UInt16(BODY_BEVE), opts.compression)
    return _svs_start_session(table, opts, req, conn, plan)
end

# Normalize a file resolver's return: `nothing`, a `FileResource`, or a bare path
# `String` (the convenience form, `FileResource(path)`).
_as_file_resource(::Nothing) = nothing
_as_file_resource(fr::FileResource) = fr
_as_file_resource(path::AbstractString) = FileResource(path)
_as_file_resource(x) = throw(ArgumentError(
    "serve_file_stream resolver must return nothing, a FileResource, or a path String; got $(typeof(x))"))

function _svs_open_file(table::_SessionTable, resolve, opts::StreamOpts, params, req::Message,
                        conn::Union{ServerConnection,Nothing} = nothing)
    resource = _svs_resource(params, req)
    resource isa Message && return resource
    fr = _as_file_resource(resolve(resource))
    plan = if fr === nothing
        nothing
    elseif !isfile(fr.path)
        _svs_error(req, EC_METHOD_NOT_FOUND, "svs open: no file for resource (path: $(fr.path))")
    else
        transcode = _file_transcode(fr.stored_compression, opts.compression)
        if transcode !== :passthrough && !_zstd_available()
            _svs_error(req, EC_APPLICATION_ERROR_BASE,
                       "svs open: $(transcode) requires the CodecZstd package on the server")
        else
            _StreamPlan(chan -> _produce_file(fr.path, transcode, chan, opts),
                        fr.format, opts.compression)
        end
    end
    return _svs_start_session(table, opts, req, conn, plan)
end

function _svs_next(table::_SessionTable, params, req::Message)
    sid = params isa AbstractDict ? get(params, "stream_id", nothing) : nothing
    # A non-integer / negative stream_id is a malformed body, not a server fault:
    # reject it cleanly rather than letting the `UInt64` conversion throw and tear
    # the connection down.
    (sid isa Integer && sid >= 0) ||
        return _svs_error(req, EC_INVALID_BODY, "svs next: expected BEVE {stream_id::UInt64}")
    stream_id = UInt64(sid)
    session = _get_session(table, stream_id)
    session === nothing &&
        return _svs_error(req, EC_INVALID_QUERY, "svs next: unknown stream_id $stream_id")

    local chunk, last
    try
        lock(session.lock) do
            session.done && error("svs next: stream already finished")
            # Mark the pull in flight and stamp on entry and exit, so the idle
            # reaper never reclaims a session that is actively waiting on a slow
            # producer (the entry stamp) nor one just serviced (the exit stamp).
            session.pulling = true
            session.last_touch = time_ns()
            try
                chunk, last = _pull!(session)
            finally
                session.pulling = false
                session.last_touch = time_ns()
            end
            (last) && (session.done = true)
        end
    catch e
        if e isa _SvsProducerFault
            # The producer task failed mid-stream: report it as a producer fault and
            # forward its message verbatim (no re-stringification).
            _remove_session!(table, stream_id; reason = "producer fault")
            return _svs_error(req, EC_SVS_PRODUCER_ERROR, e.msg)
        end
        # A client/query-level fault (e.g. `next` after the stream already finished).
        _remove_session!(table, stream_id; reason = "next error")
        return _svs_error(req, EC_INVALID_QUERY, sprint(showerror, e))
    end

    last && _remove_session!(table, stream_id; reason = "completed")
    return _chunk_response(req, chunk, last)
end

function _svs_cancel(table::_SessionTable, params, req::Message)
    # Lenient: a malformed cancel releases nothing but must not error a notify, so
    # the stream_id is type-checked rather than force-converted.
    if params isa AbstractDict
        sid = get(params, "stream_id", nothing)
        (sid isa Integer && sid >= 0) && _remove_session!(table, UInt64(sid); reason = "cancelled")
    end
    return _beve_response(req, _CancelAck(true))
end

# Background backstop for sessions that are never freed cooperatively: a consumer
# that holds its connection open while abandoning the pull (no `next`, no `cancel`)
# would otherwise leave its session (and the producer task pinning the resolved
# value) resident. The reaper sweeps on a cadence derived from `idle_timeout`,
# reclaiming idle sessions. Server shutdown is delivered as a definite signal via
# `on_server_stop` -- not inferred from a polled flag -- so the sweep task is torn
# down and the table drained exactly when the server stops, for every server
# lifetime including ones shorter than a sweep interval. `idle_timeout = Inf`
# keeps only the shutdown drain (no idle reaping, hence no sweep task at all).
function _start_session_reaper(server, table::_SessionTable, opts::StreamOpts)
    if !isfinite(opts.idle_timeout)
        on_server_stop(server, () -> _drain_all_sessions!(table))
        return nothing
    end
    timeout_ns = round(UInt64, opts.idle_timeout * 1e9)
    # Sweep often enough to honor the timeout without busy-spinning; clamp so a
    # tiny timeout does not flood and a huge one still bounds the sweep period.
    interval = clamp(opts.idle_timeout / 2, 0.5, 30.0)
    timer = Timer(interval; interval = interval)
    # Closing the timer wakes the sweep task (its `wait` throws); draining here
    # makes shutdown release sessions synchronously with `stop()`.
    on_server_stop(server, () -> (close(timer); _drain_all_sessions!(table)))
    @async begin
        try
            while true
                wait(timer)
                _reap_idle_sessions!(table, timeout_ns)
            end
        catch
            # Timer closed at server stop (or a sweep error): stop reaping.
        finally
            close(timer)
        end
    end
    return nothing
end

# A server carries exactly one SVS producer: `serve_value_stream` and
# `serve_file_stream` install the same fixed `/_svs/*` routes, so a second
# registration silently replaces the first. Warn at registration time so that
# replacement is visible here rather than surfacing later as the "wrong" producer
# answering pulls.
function _warn_if_svs_registered(server)
    if hasproperty(server, :handlers) && haskey(server.handlers, SVS_OPEN)
        @warn "REPE SVS: an SVS producer is already registered on this server; the new " *
              "registration replaces it (one SVS producer per server, last registration wins)."
    end
    return nothing
end

"""
    serve_value_stream(server, resolve; opts = StreamOpts()) -> server

Register an SVS download producer on `server` (a TCP [`Server`](@ref) or a
WebSocket `WSServer`). Installs the `/_svs/open`, `/_svs/next`, and `/_svs/cancel`
routes; a server carries one SVS producer (a second registration replaces the
routes).

`resolve(resource::AbstractString)` maps the opaque `resource` key from an `open`
request to the value to stream, or returns `nothing` if there is no such resource.
The value is BEVE-serialized and, per `opts.compression`, zstd-compressed as it is
pulled; it is never copied whole. Any BEVE-serializable value works, including a
numeric `Vector` (encoded as a BEVE typed array).

Each producer session is bound to the connection that opened it via the server's
[`on_close`](@ref) hook, so a dropped connection releases its sessions at once;
`opts.idle_timeout` and `cancel` are the secondary backstops (see [`StreamOpts`](@ref)).

```julia
server = RepeRPC.Server("127.0.0.1", 9000)
RepeRPC.serve_value_stream(server, resource -> resource == "telemetry" ? load_frame() : nothing)
RepeRPC.listen(server; async = true)
```
"""
function serve_value_stream(server::AbstractServer, resolve; opts::StreamOpts = StreamOpts())
    opts.compression == _COMPRESSION_ZSTD && _require_zstd()
    _warn_if_svs_registered(server)
    table = _SessionTable()
    # `open` takes the optional connection context (3-arg form): the transport
    # passes it, and the default keeps a context-less caller working. `next` and
    # `cancel` do not need it. The idle reaper (below) remains the backstop for
    # transports that supply no connection context.
    register(server, SVS_OPEN, (params, req, conn = nothing) -> _svs_open(table, resolve, opts, params, req, conn))
    register(server, SVS_NEXT, (params, req) -> _svs_next(table, params, req))
    register(server, SVS_CANCEL, (params, req) -> _svs_cancel(table, params, req))
    _start_session_reaper(server, table, opts)
    return server
end

"""
    serve_file_stream(server, resolve; opts = StreamOpts()) -> server

Register an SVS download producer that streams **existing files verbatim**,
without the deserialize→reserialize round trip of [`serve_value_stream`](@ref).
Use this when the value already lives on disk as a `.beve` / `.beve.zst` file (for
example a finished collection): the producer reads the file bytes straight into
the chunk stream rather than rebuilding a struct only to serialize it again.

Installs the same `/_svs/open`, `/_svs/next`, `/_svs/cancel` routes as
[`serve_value_stream`](@ref); a server carries one SVS producer, so registering a
file stream replaces a value stream and vice versa (last registration wins).

`resolve(resource::AbstractString)` maps the opaque `resource` key to a
[`FileResource`](@ref) (or a bare path `String`, treated as `FileResource(path)`),
or `nothing` if there is no such resource. The producer reconciles the file's
stored compression with `opts.compression`:

- **stored == wire** (e.g. a `.beve.zst` file on a `:zstd` stream, or a `.beve`
  file on a `:none` stream): the bytes are passed through untouched — no codec, no
  BEVE — the cheapest mode and the reason this entry point exists.
- **stored `:none`, wire `:zstd`**: the file is zstd-compressed on the fly (still
  no BEVE pass), so a plain `.beve` file is delivered compressed on the wire.
- **stored `:zstd`, wire `:none`**: the file is decompressed on the fly, so a
  `.beve.zst` file can be served to a consumer without `CodecZstd`.

The two transcoding directions need `CodecZstd` loaded on the server (`using
CodecZstd`); an `open` that would require it without it returns an error. The
consumer side is identical to [`serve_value_stream`](@ref): pull with
[`pull_value`](@ref), [`pull_to_beve_file`](@ref), [`pull_to_beve_zst_file`](@ref),
or [`pull_to_file`](@ref). Sessions are connection-bound and idle-reaped exactly as
for the value producer.

```julia
server = RepeRPC.Server("127.0.0.1", 9000)
RepeRPC.serve_file_stream(server, resource -> isfile(resource) ? RepeRPC.FileResource(resource) : nothing)
RepeRPC.listen(server; async = true)
```
"""
function serve_file_stream(server::AbstractServer, resolve; opts::StreamOpts = StreamOpts())
    opts.compression == _COMPRESSION_ZSTD && _require_zstd()
    _warn_if_svs_registered(server)
    table = _SessionTable()
    register(server, SVS_OPEN,
             (params, req, conn = nothing) -> _svs_open_file(table, resolve, opts, params, req, conn))
    register(server, SVS_NEXT, (params, req) -> _svs_next(table, params, req))
    register(server, SVS_CANCEL, (params, req) -> _svs_cancel(table, params, req))
    _start_session_reaper(server, table, opts)
    return server
end

#==============================================================================#
# Client — pull driver
#==============================================================================#

struct _OpenInfo
    stream_id::UInt64
    format::UInt16
    compression::UInt8
end

function _open_stream(client::AbstractClient, resource::AbstractString, timeout)
    resp = send_request(_OpenResponse, client, SVS_OPEN, _OpenRequest(String(resource));
                        query_format = QUERY_JSON_POINTER, body_format = BODY_BEVE, timeout = timeout)
    resp isa _OpenResponse ||
        error("svs open: malformed response of type $(typeof(resp))")
    # The server opens its session before answering, so when we reject what it
    # returned we `cancel` the stream it just gave us (SVS §5 / §2.3) rather than
    # leaving it to the server's idle/disconnect backstop.
    if resp.version != SVS_VERSION
        _cancel_stream(client, resp.stream_id, "unsupported svs version $(resp.version)")
        error("svs open: server contract version $(resp.version) != supported $(SVS_VERSION)")
    end
    if !(resp.compression == _COMPRESSION_NONE || resp.compression == _COMPRESSION_ZSTD)
        _cancel_stream(client, resp.stream_id, "unknown compression tag $(resp.compression)")
        error("svs open: unknown compression tag $(resp.compression)")
    end
    return _OpenInfo(resp.stream_id, resp.format, resp.compression)
end

function _cancel_stream(client::AbstractClient, stream_id::UInt64, reason::AbstractString)
    # Notify form: best-effort, no reply awaited (SVS §2.3).
    try
        send_notify(client, SVS_CANCEL, _CancelRequest(stream_id, String(reason));
                    query_format = QUERY_JSON_POINTER, body_format = BODY_BEVE)
    catch
    end
    return nothing
end

"""
    ChunkReader <: IO

A read-side `IO` that issues `next` requests on demand and surfaces the terminal
`last = 1` chunk as EOF. Decoders (BEVE's `BeveDeserializer`, a wrapping zstd
decompressor) read from it without knowing chunks exist underneath.
"""
mutable struct ChunkReader <: IO
    client::AbstractClient
    stream_id::UInt64
    timeout::Any
    deadline::Union{Nothing,Float64}      # `time()` past which the whole pull aborts (nothing = unbounded)
    total_timeout::Union{Nothing,Float64} # the configured budget, retained for the timeout message
    cancel::Union{Nothing,Function}       # caller predicate polled once per chunk; `true` aborts the pull
    resource::Union{Nothing,String}       # retained for `CancelledError`/`TimeoutError` observability
    buf::Vector{UInt8}
    pos::Int            # 1-based index of the next unread byte
    finished::Bool      # last chunk consumed; no more fetches
    last_seen::Bool
end
ChunkReader(client::AbstractClient, stream_id::UInt64, timeout;
            deadline::Union{Nothing,Float64} = nothing,
            total_timeout::Union{Nothing,Float64} = nothing,
            cancel::Union{Nothing,Function} = nothing,
            resource::Union{Nothing,String} = nothing) =
    ChunkReader(client, stream_id, timeout, deadline, total_timeout, cancel, resource,
                UInt8[], 1, false, false)

# Poll the caller's cancel predicate and throw if it fires. `stream_id = 0` marks a
# cancel observed before the stream opened (no session to release). `nothing`
# short-circuits, so the unused path pays nothing on the per-chunk hot check.
@inline function _throw_if_cancelled(cancel, resource, stream_id::UInt64 = UInt64(0))
    if cancel !== nothing && cancel()
        throw(CancelledError(stream_id; resource = resource))
    end
    return nothing
end

# No-op close so a wrapping zstd decompressor's `close` (which cascades here) frees
# its native `ZSTD_DCtx` without erroring on the underlying reader. The ChunkReader
# holds no OS resource of its own; the producer session is released via `_cancel_stream`.
Base.close(::ChunkReader) = nothing

# Release the decoder's native zstd context. TranscodingStreams installs no finalizer,
# so an unclosed decompressor leaks its `ZSTD_DCtx` until process exit (on the success
# path too, not just on abort). `close` frees it and cascades to the ChunkReader no-op
# above. Guarded so a close never masks the in-flight exception when run from a `finally`.
_release_reader(reader) = (try; close(reader); catch; end; nothing)

function _fetch!(r::ChunkReader)
    # Cooperative cancel: polled once per chunk, before the round trip, so an
    # already-cancelled pull never issues another `next`. Checked before the deadline
    # so an explicit cancel wins over a coincidentally-expired budget. The abort
    # unwinds the caller's `try`/`finally` exactly as the deadline throw below does.
    _throw_if_cancelled(r.cancel, r.resource, r.stream_id)
    # Whole-transfer guard: `timeout` bounds each individual `next` round trip, but a
    # peer trickling bytes just under that per-call timeout could stall a pull
    # indefinitely. `deadline` (set from `total_timeout`) bounds the pull as a whole.
    # Checked before the round trip so an already-expired pull never issues another
    # `next`; on expiry the caller's atomic temp-then-rename leaves no final file.
    if r.deadline !== nothing && time() >= r.deadline
        throw(TimeoutError("svs: transfer exceeded total_timeout of $(r.total_timeout)s",
                           something(r.total_timeout, 0.0)))
    end
    resp = send_request(Message, r.client, SVS_NEXT, _NextRequest(r.stream_id);
                        query_format = QUERY_JSON_POINTER, body_format = BODY_BEVE, timeout = r.timeout)
    last = !isempty(resp.query) && codeunit(resp.query, 1) == 0x01
    r.buf = resp.body
    r.pos = 1
    if last
        r.last_seen = true
        r.finished = true
    end
    return nothing
end

# Ensure the current buffer has an unread byte, or that the stream is finished.
function _ensure!(r::ChunkReader)
    while r.pos > length(r.buf) && !r.finished
        _fetch!(r)
    end
    return nothing
end

Base.eof(r::ChunkReader) = (_ensure!(r); r.pos > length(r.buf))
Base.bytesavailable(r::ChunkReader) = length(r.buf) - r.pos + 1
Base.isopen(r::ChunkReader) = !r.finished || r.pos <= length(r.buf)
Base.isreadable(::ChunkReader) = true

function Base.read(r::ChunkReader, ::Type{UInt8})::UInt8
    _ensure!(r)
    r.pos > length(r.buf) && throw(EOFError())
    b = @inbounds r.buf[r.pos]
    r.pos += 1
    return b
end

function Base.unsafe_read(r::ChunkReader, p::Ptr{UInt8}, n::UInt)
    nleft = Int(n)
    while nleft > 0
        _ensure!(r)
        avail = length(r.buf) - r.pos + 1
        avail <= 0 && throw(EOFError())
        take = min(avail, nleft)
        GC.@preserve r unsafe_copyto!(p, pointer(r.buf, r.pos), take)
        p += take
        r.pos += take
        nleft -= take
    end
    return nothing
end

function Base.readavailable(r::ChunkReader)
    _ensure!(r)
    out = r.buf[r.pos:end]
    r.pos = length(r.buf) + 1
    return out
end

# Open `info`'s stream as a read-side `IO` of the *decompressed* serialized bytes.
# Returns `(reader, raw)` where `raw` is the underlying `ChunkReader` (carrying the
# `last_seen` flag) and `reader` is `raw` wrapped in a zstd decompressor when the
# stream is compressed.
function _stream_reader(client::AbstractClient, info::_OpenInfo, timeout; total_timeout = nothing,
                        cancel = nothing, resource = nothing)
    deadline = total_timeout === nothing ? nothing : time() + Float64(total_timeout)
    tt = total_timeout === nothing ? nothing : Float64(total_timeout)
    raw = ChunkReader(client, info.stream_id, timeout; deadline = deadline, total_timeout = tt,
                      cancel = cancel, resource = resource === nothing ? nothing : String(resource))
    reader = info.compression == _COMPRESSION_ZSTD ? _zstd_decompressor_stream(raw) : raw
    return reader, raw
end

function _check_compression_available(info::_OpenInfo)
    info.compression == _COMPRESSION_ZSTD && _require_zstd()
    return nothing
end

"""
    pull_value(client, resource; timeout = nothing, total_timeout = nothing, cancel = nothing) -> value

Pull `resource` from an SVS producer reachable through `client` (a TCP
[`Client`](@ref) or a WebSocket `WSClient`) and stream-decode it into a generic
value (the `Dict`/`Vector`/scalar tree BEVE deserializes to). The compressed and
decompressed buffers are never materialized; peak memory is the result tree plus
one chunk. `timeout` is the per-`next`-round-trip timeout (`nothing` uses the
client default); `total_timeout`, when set, bounds the whole transfer in seconds
(a `TimeoutError` is thrown if exceeded), guarding against a peer that trickles
bytes just under the per-`next` timeout indefinitely.

`cancel` is a caller predicate (`() -> Bool`) polled once per chunk, immediately
before each `next` round trip; returning `true` aborts the pull with a
[`CancelledError`](@ref). See the "Cooperative cancellation" note below for the
latency and thread-safety contract.

    pull_value(T::Type, client, resource; timeout = nothing, total_timeout = nothing, cancel = nothing) -> T

Decode into the struct type `T` instead. Like the generic form, this streams
token-by-token straight into `T` (`deser_beve(T, ::IO)`): neither the compressed
nor the decompressed payload is materialized, so peak memory is the constructed
`T` plus one chunk. Memory-bounded regardless of payload size.

## Cooperative cancellation

`cancel` is polled once per chunk, immediately before each `next` round trip.
Setting the flag aborts the pull at the next chunk boundary: any `next` already in
flight first runs to completion (or to the per-`next` `timeout`), so the abort lands
within one chunk round trip — not instantly, and not only at end-of-transfer.
Worst-case latency from setting the flag to the throw is the remaining time of the
in-flight `next`, bounded above by the per-`next` `timeout` (reached only if the
peer stalls or trickles bytes); `total_timeout` remains the backstop for a peer
that stalls *inside* a `next`, which cancellation cannot interrupt. In normal
operation the window is the wire time of a single chunk. The predicate runs on the
pull's own task, so back it with a `Threads.Atomic{Bool}` (not a bare `Ref{Bool}`)
for reliable cross-task visibility, and keep it cheap, non-blocking, and
non-throwing — it is polled on the transfer hot path.
"""
function pull_value(client::AbstractClient, resource::AbstractString; timeout = nothing,
                    total_timeout = nothing, cancel = nothing)
    _throw_if_cancelled(cancel, resource)   # skip the open round trip if already cancelled
    info = _open_stream(client, resource, timeout)
    if info.format != UInt16(BODY_BEVE)
        _cancel_stream(client, info.stream_id, "value decode needs format=BEVE")
        error("svs: value decode needs format=BEVE, got format $(info.format)")
    end
    _check_compression_available(info)
    reader, _ = _stream_reader(client, info, timeout; total_timeout = total_timeout,
                               cancel = cancel, resource = resource)
    value = try
        BeveFormat.parse_value(BeveFormat.BeveDeserializer(reader))
    finally
        # Free the native zstd decoder (no finalizer; see `_release_reader`), then
        # release the session — the decoder may not have pulled the terminal empty
        # chunk. Both best-effort (SVS §2.3).
        _release_reader(reader)
        _cancel_stream(client, info.stream_id, "value fully decoded")
    end
    return value
end

function pull_value(::Type{T}, client::AbstractClient, resource::AbstractString; timeout = nothing,
                    total_timeout = nothing, cancel = nothing) where {T}
    _throw_if_cancelled(cancel, resource)   # skip the open round trip if already cancelled
    info = _open_stream(client, resource, timeout)
    if info.format != UInt16(BODY_BEVE)
        _cancel_stream(client, info.stream_id, "typed decode needs format=BEVE")
        error("svs: typed decode needs format=BEVE, got format $(info.format)")
    end
    _check_compression_available(info)
    reader, _ = _stream_reader(client, info, timeout; total_timeout = total_timeout,
                               cancel = cancel, resource = resource)
    value = try
        BeveFormat.deser_beve(T, reader)   # stream straight into `T`; never materializes the payload
    finally
        # Free the native zstd decoder (no finalizer; see `_release_reader`), then
        # release the session — the decoder may stop before the terminal empty chunk.
        # Both best-effort (SVS §2.3).
        _release_reader(reader)
        _cancel_stream(client, info.stream_id, "typed decode complete")
    end
    return value
end

# Drive a pull into a temp file via `fill`, then commit atomically: rename onto
# `path` only after the terminating `last` chunk arrived. The temp file is removed
# if anything fails before the rename, so a dropped connection never leaves a
# truncated file that looks complete (SVS §3).
function _pull_to_file(client::AbstractClient, info::_OpenInfo, path::AbstractString,
                       timeout, decompress::Bool; total_timeout = nothing, cancel = nothing,
                       resource = nothing)
    reader, raw = _stream_reader(client, info, timeout; total_timeout = total_timeout,
                                 cancel = cancel, resource = resource)
    decompress || (reader = raw)
    tmp = string(path, ".svspart")
    committed = false
    try
        open(tmp, "w") do f
            while !eof(reader)
                write(f, readavailable(reader))
            end
        end
        raw.last_seen ||
            error("svs: stream ended without a final chunk (truncated transfer)")
        Base.Filesystem.rename(tmp, path)
        committed = true
    finally
        _release_reader(reader)   # free the native zstd decoder (no finalizer; leaks otherwise)
        if !committed
            _cancel_stream(client, info.stream_id, "receiver aborted")
            try; isfile(tmp) && rm(tmp); catch; end
        end
    end
    return nothing
end

"""
    pull_to_beve_zst_file(client, resource, path; timeout = nothing, total_timeout = nothing, cancel = nothing)

Pull `resource` and write the compressed bytes verbatim to `path` as a `.beve.zst`
file. Requires the producer to be compressed. Fully memory-bounded regardless of
payload size; written atomically (temp-then-rename on the terminal chunk). On a
mid-stream abort — a [`CancelledError`](@ref) from `cancel`, a `total_timeout`, or a
dropped connection — the `.svspart` temp is removed and `path` is never created.
`cancel` is polled once per chunk; see [`pull_value`](@ref) for its latency and
thread-safety contract.
"""
function pull_to_beve_zst_file(client::AbstractClient, resource::AbstractString,
                               path::AbstractString; timeout = nothing, total_timeout = nothing,
                               cancel = nothing)
    _throw_if_cancelled(cancel, resource)   # skip the open round trip if already cancelled
    info = _open_stream(client, resource, timeout)
    if info.compression != _COMPRESSION_ZSTD
        _cancel_stream(client, info.stream_id, ".beve.zst output needs a compressed stream")
        error("svs: .beve.zst output needs a zstd-compressed stream")
    end
    _pull_to_file(client, info, path, timeout, false; total_timeout = total_timeout,
                  cancel = cancel, resource = resource)
    return nothing
end

"""
    pull_to_beve_file(client, resource, path; timeout = nothing, total_timeout = nothing, cancel = nothing)

Pull `resource`, decompress it, and write a `.beve` file at `path`. Requires the
producer to be compressed and BEVE-formatted. Fully memory-bounded; written
atomically. Needs `CodecZstd` loaded. On a mid-stream abort — a
[`CancelledError`](@ref) from `cancel`, a `total_timeout`, or a dropped connection —
the `.svspart` temp is removed and `path` is never created. `cancel` is polled once
per chunk; see [`pull_value`](@ref) for its latency and thread-safety contract.
"""
function pull_to_beve_file(client::AbstractClient, resource::AbstractString,
                           path::AbstractString; timeout = nothing, total_timeout = nothing,
                           cancel = nothing)
    _throw_if_cancelled(cancel, resource)   # skip the open round trip if already cancelled
    info = _open_stream(client, resource, timeout)
    if info.compression != _COMPRESSION_ZSTD
        _cancel_stream(client, info.stream_id, ".beve output needs a compressed stream")
        error("svs: .beve output needs a zstd-compressed stream")
    end
    if info.format != UInt16(BODY_BEVE)
        _cancel_stream(client, info.stream_id, ".beve output needs format=BEVE")
        error("svs: .beve output needs format=BEVE, got format $(info.format)")
    end
    _check_compression_available(info)
    _pull_to_file(client, info, path, timeout, true; total_timeout = total_timeout,
                  cancel = cancel, resource = resource)
    return nothing
end

"""
    pull_to_file(client, resource, path; timeout = nothing, total_timeout = nothing, cancel = nothing)

Pull `resource` and write its **logical** bytes to `path`, decompressing when the
stream is zstd-compressed and writing the bytes as-is when it is not. Format-
agnostic (unlike [`pull_to_beve_file`](@ref), it does not require `format=BEVE`),
so it is the general "download this stream to a file" helper — most useful with an
uncompressed [`serve_file_stream`](@ref) producer, where neither
[`pull_to_beve_file`](@ref) (needs a compressed stream) nor
[`pull_to_beve_zst_file`](@ref) (writes verbatim compressed) applies. The verbatim-
compressed counterpart is [`pull_to_beve_zst_file`](@ref). Fully memory-bounded
regardless of payload size; written atomically (temp-then-rename on the terminal
chunk). Decompression needs `CodecZstd` loaded. `cancel` is polled once per chunk
and aborts with a [`CancelledError`](@ref), removing the `.svspart` temp; see
[`pull_value`](@ref) for its latency and thread-safety contract.
"""
function pull_to_file(client::AbstractClient, resource::AbstractString,
                      path::AbstractString; timeout = nothing, total_timeout = nothing,
                      cancel = nothing)
    _throw_if_cancelled(cancel, resource)   # skip the open round trip if already cancelled
    info = _open_stream(client, resource, timeout)
    _check_compression_available(info)
    _pull_to_file(client, info, path, timeout, info.compression == _COMPRESSION_ZSTD;
                  total_timeout = total_timeout, cancel = cancel, resource = resource)
    return nothing
end
