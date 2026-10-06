# Streaming large bodies - shared vocabulary and chunk-pump primitives.
#
# REPE moves a body socket <-> IO in fixed-size chunks so that peak memory is
# bounded by the chunk size regardless of payload size. The wire format is
# unchanged: a streamed message is byte-for-byte identical to the equivalent
# fully-materialized one (same 48-byte header, same body_length, same body
# bytes), so streamed and non-streamed peers interoperate freely in either
# direction.
#
# This file holds only the pieces both the client and server paths share: the
# returned/handler types and the low-level pump + body-length guard. The
# transport-facing entry points live with their transport: client-side
# `stream_response`/`download`/`stream_request` in client.jl, the server-side
# `_write_streamed_response` in server.jl, and Fleet `stream`/`download` in
# fleet.jl.

"""
    StreamBody(io::IO, length::Integer; body_format = BODY_RAW_BINARY, own = true)

Returned from a server handler to stream `length` bytes from `io` as the
response body instead of materializing an in-memory result. The server writes
`length` into the header's `body_length`, then pumps `io` to the socket in
chunks, bypassing the single-buffer `serialize_message`.

`length` must be known in advance. REPE is a prefix-length protocol (the
header's `body_length` precedes the body), so there is no chunked transfer
encoding: a source whose size is not known until it is fully produced (a pipe, a
live socket, on-the-fly compression) cannot be streamed directly. Size it first
or stage it.

`own` controls whether the server closes `io` when the transfer finishes (or
fails). It defaults to `true`, which is what [`FileBody`](@ref) wants. Pass
`own = false` when the handler owns the lifetime of a caller-supplied `IO`
(e.g. a reused `IOBuffer`) and must not have it closed.

Returning a `StreamBody` is only supported over the TCP [`Server`](@ref). The
WebSocket server rejects it with a clear error: streaming buys nothing there
because HTTP.jl reassembles every frame into one in-memory buffer on receive (it
exposes no per-frame read), so a receiver materializes the whole body no matter
how the sender frames it, and the memory bound `StreamBody` exists to provide is
unreachable end-to-end.
"""
struct StreamBody
    io::IO
    length::Int
    body_format::BodyFormat
    own::Bool
end

function StreamBody(io::IO, length::Integer;
                    body_format::BodyFormat = BODY_RAW_BINARY,
                    own::Bool = true)
    length < 0 && throw(ArgumentError("StreamBody length must be non-negative, got $length"))
    return StreamBody(io, Int(length), body_format, own)
end

"""
    FileBody(path::AbstractString; body_format = BODY_RAW_BINARY) -> StreamBody

Convenience constructor: opens `path` for reading, sets `length = filesize(path)`,
and `own = true` so the opened handle is closed after the transfer (or on
failure). Use it from a handler to stream a file straight from disk:

```julia
register(server, "/artifacts/get", (params, request) -> FileBody(resolve(params["id"])))
```
"""
function FileBody(path::AbstractString; body_format::BodyFormat = BODY_RAW_BINARY)
    io = open(path, "r")
    try
        # Stat the open handle (not the path) so the declared length and the
        # type check come from the same fstat -- no TOCTOU window where the file
        # is swapped between sizing and reading. Reject anything that is not a
        # regular file (a directory opens fine on POSIX but has no streamable
        # body), failing cleanly here instead of as a mid-transfer short read.
        st = stat(io)
        isfile(st) || throw(ArgumentError(
            "FileBody requires a regular file, but $(repr(path)) is not one"))
        return StreamBody(io, st.size; body_format = body_format, own = true)
    catch e
        close(io)
        rethrow(e)
    end
end

"""
    StreamedResponse

Metadata returned by [`stream_response`](@ref)/`download`; the body bytes are in
the caller-supplied sink, not in this value. A `StreamedResponse` is only
returned on success: a server error or a truncated transfer is *raised*, never
encoded here, so there is no error-code field to check.

# Fields
- `bytes::Int` - number of body bytes written to the sink
- `body_format::BodyFormat` - format the server tagged the body with
"""
struct StreamedResponse
    bytes::Int
    body_format::BodyFormat
end

function Base.show(io::IO, r::StreamedResponse)
    print(io, "StreamedResponse($(r.bytes) bytes, $(r.body_format))")
end

"""
    _to_body_format(x::Integer) -> BodyFormat

Convert a wire `body_format` value to the [`BodyFormat`](@ref) enum. Custom
formats outside the defined enum values (>= `BODY_CUSTOM_BASE` and not exactly a
named instance) fall back to `BODY_RAW_BINARY`, since to a streaming sink the
bytes are raw regardless of the tag. This keeps an otherwise-successful transfer
from failing only because the format tag is application-defined.
"""
function _to_body_format(x::Integer)
    try
        return BodyFormat(x)
    catch
        return BODY_RAW_BINARY
    end
end

"""
    _within_length_limit(limit, len) -> Bool

`true` when a single wire-controlled length `len` (`query_length` or
`body_length`) is safe to allocate. Checked *before* allocating from the length,
on every read path, so a garbled or malicious header cannot force an unbounded
allocation.

A set `limit` (`max_body_length`) bounds `len` to at most `limit`. A `nothing`
limit ("unlimited", the default) still rejects a `len` above `typemax(Int)`: the
bytes are read into an `Int`-indexed `Vector`, so an oversized `UInt64` length
would otherwise throw `InexactError` deep in the read path instead of being
refused cleanly up front. Both `query_length` and `body_length` are guarded,
since either is peer-controlled and `deserialize_header` only checks their sum
against the header's `length`.
"""
@inline _within_length_limit(::Nothing, len::Integer) = len <= typemax(Int)
@inline _within_length_limit(limit::Integer, len::Integer) = len <= limit

"""
    _pump_exact(src::IO, dest::IO, nbytes::Integer, buffer_size::Integer) -> Int

Copy exactly `nbytes` bytes from `src` to `dest`, moving at most `buffer_size`
bytes per iteration, so peak memory is bounded by `buffer_size` regardless of
`nbytes`. Returns `nbytes`.

Throws `EOFError` if `src` reaches EOF before `nbytes` have been read. A short
read on either direction of a REPE stream is a truncated transfer: because
`body_length` is committed in the header before the first body byte, the
protocol cannot signal a mid-body failure, so the only correct outcome is to
surface it as a transport error (and let the caller tear the connection down)
rather than treat the bytes already moved as a complete body.
"""
function _pump_exact(src::IO, dest::IO, nbytes::Integer, buffer_size::Integer)
    total = Int(nbytes)
    total < 0 && throw(ArgumentError("nbytes must be non-negative, got $total"))
    buffer_size < 1 && throw(ArgumentError("buffer_size must be positive, got $buffer_size"))
    total == 0 && return 0          # nothing to move; skip the buffer allocation
    remaining = total
    bufcap = max(1, min(Int(buffer_size), total))
    buf = Vector{UInt8}(undef, bufcap)
    while remaining > 0
        want = remaining < bufcap ? remaining : bufcap
        n = readbytes!(src, buf, want)
        n == 0 && throw(EOFError())
        write(dest, view(buf, 1:n))
        remaining -= n
    end
    return total
end

"""
    _read_message(socket::IO, header::Header)::Message

Read the query and body that follow an already-parsed `header` directly into
right-sized buffers and build the `Message`, without recombining into one buffer
or re-parsing the header. This is the non-streaming receive path shared by the
TCP client and server; it brings the body cost down to a single allocation +
read (`String(query_bytes)` takes ownership of the query buffer). Throws
`EOFError` if the socket ends before the full query/body has arrived.
"""
function _read_message(socket::IO, header::Header)::Message
    qlen = Int(header.query_length)
    blen = Int(header.body_length)

    query_bytes = Vector{UInt8}(undef, qlen)
    if qlen > 0
        readbytes!(socket, query_bytes, qlen) == qlen || throw(EOFError())
    end

    body_bytes = Vector{UInt8}(undef, blen)
    if blen > 0
        readbytes!(socket, body_bytes, blen) == blen || throw(EOFError())
    end

    return Message(header, String(query_bytes), body_bytes)
end
