struct PendingRequest
    channel::Channel
    result_type::Union{Nothing, Type}
    # When non-`nothing`, the response body is streamed into this sink in
    # `buffer_size` chunks instead of being materialized; the channel then
    # receives a `StreamedResponse`. See `stream_response`.
    sink::Union{Nothing, IO}
    buffer_size::Int
end

# Backward-compatible constructor for the non-streaming path (and the WebSocket
# client, which ignores the sink). Keeps existing `PendingRequest(ch, T)` callers
# working unchanged.
PendingRequest(channel::Channel, result_type::Union{Nothing, Type}) =
    PendingRequest(channel, result_type, nothing, DEFAULT_BUFFER_SIZE)

mutable struct Client <: AbstractClient
    socket::TCPSocket
    host::String
    port::Int
    connected::Bool
    timeout::Float64
    next_id::Threads.Atomic{UInt64}
    pending_requests::Dict{UInt64, PendingRequest}
    nodelay::Bool
    # Seconds a connection may sit idle before TCP keepalive probing starts;
    # `nothing` disables keepalive. See `_enable_keepalive!`.
    keepalive::Union{Nothing, Float64}
    # Reject a response whose header declares a body larger than this many bytes
    # before allocating/reading it. `nothing` means unlimited (default).
    max_body_length::Union{Nothing, Int}
    # Default chunk size for streamed transfers; overridable per `stream_response`.
    buffer_size::Int

    # Synchronization primitives
    state_lock::ReentrantLock        # Protects connected and socket
    requests_lock::ReentrantLock     # Protects pending_requests
    write_lock::ReentrantLock        # Protects socket writes

    function Client(host::String = "localhost", port::Int = 8080; timeout::Float64 = 30.0,
                    nodelay::Bool = true,
                    keepalive::Union{Nothing, Real} = DEFAULT_KEEPALIVE_IDLE,
                    max_body_length::Union{Nothing, Integer} = nothing,
                    buffer_size::Integer = DEFAULT_BUFFER_SIZE)
        buffer_size < 1 && throw(ArgumentError("buffer_size must be positive, got $buffer_size"))
        keepalive !== nothing && !(isfinite(keepalive) && 0 < keepalive <= typemax(Cuint)) &&
            throw(ArgumentError("keepalive must be a finite number of seconds in (0, $(typemax(Cuint))] (use nothing to disable), got $keepalive"))
        max_body_length !== nothing && max_body_length < 0 &&
            throw(ArgumentError("max_body_length must be non-negative or nothing, got $max_body_length"))
        new(TCPSocket(), host, port, false, timeout,
            Threads.Atomic{UInt64}(1),
            Dict{UInt64, PendingRequest}(),
            nodelay,
            keepalive === nothing ? nothing : Float64(keepalive),
            max_body_length === nothing ? nothing : Int(max_body_length),
            Int(buffer_size),
            ReentrantLock(),
            ReentrantLock(),
            ReentrantLock())
    end
end

function _resolve_addresses(host::String)::Vector{Sockets.IPAddr}
    addresses = Sockets.IPAddr[]
    for family in (Sockets.IPv6, Sockets.IPv4)
        try
            addr = Sockets.getaddrinfo(host, family)
            push!(addresses, addr)
        catch
            # Ignore resolution failures for this address family
        end
    end

    if isempty(addresses)
        try
            push!(addresses, Sockets.getaddrinfo(host))
        catch e
            error(ErrorException("Failed to resolve host $host: $(e)"))
        end
    end

    unique_addrs = Sockets.IPAddr[]
    for addr in addresses
        if !any(existing -> existing == addr, unique_addrs)
            push!(unique_addrs, addr)
        end
    end

    return unique_addrs
end

function _connect_socket(host::String, port::Int)
    last_error = nothing

    for addr in _resolve_addresses(host)
        try
            return Sockets.connect(addr, port)
        catch e
            last_error = e
        end
    end

    if last_error !== nothing
        throw(last_error)
    else
        error(ErrorException("No addresses available for host $host"))
    end
end

# Enable TCP keepalive so a peer that disappears *without* closing the socket --
# host power loss, a pulled cable, a firewall silently dropping the flow -- is
# eventually detected. A clean peer close arrives as EOF and is already handled
# (`_handle_responses` wakes every waiter with a `ConnectionError`), but a half-open
# connection delivers nothing: the reader sits in `eof(sock)` indefinitely, so a
# request with a timeout reports a truthful but unhelpful `TimeoutError` and a
# streamed wait with `timeout = nothing` never returns at all. Keepalive turns both
# into the `ConnectionError` they actually are.
#
# `idle` goes on the wire in whole seconds, and libuv rejects a zero delay *after*
# it has already flipped `SO_KEEPALIVE` on -- which would leave keepalive nominally
# enabled with the OS default two-hour idle -- so a sub-second request rounds up
# rather than degrading silently.
function _enable_keepalive!(sock::TCPSocket, idle::Union{Nothing, Float64})
    idle === nothing && return sock
    # Mutating a libuv handle has to be serialized against the event loop that owns
    # it, exactly as `Sockets.nagle` does at the adjacent call site.
    Base.iolock_begin()
    try
        Base.check_open(sock)
        ccall(:uv_tcp_keepalive, Cint, (Ptr{Cvoid}, Cint, Cuint),
              sock.handle, Cint(1), max(Cuint(1), round(Cuint, idle)))
        _set_keepalive_probes!(sock)
    finally
        Base.iolock_end()
    end
    return sock
end

# Pin the probe cadence rather than inheriting it. The libuv bundled with Julia 1.11
# sets these itself, to exactly the values used here, but the one in Julia 1.9/1.10 --
# inside this package's compat range -- sets only `SO_KEEPALIVE` and the idle time and
# leaves the OS defaults of 75 s x 8. That is a 615 s detection window instead of 25 s,
# which puts a dead peer far outside the request timeout and reinstates the misleading
# `TimeoutError` this whole feature exists to remove. Setting them explicitly makes
# detection identical on every supported Julia.
#
# The option numbers live under `IPPROTO_TCP` and differ per platform, so this is
# best-effort: on an unrecognized platform, or if `setsockopt` refuses, keepalive stays
# enabled with whatever cadence libuv left rather than failing the connection.
# Callers must already hold the iolock.
function _set_keepalive_probes!(sock::TCPSocket)
    # Option numbers under IPPROTO_TCP: (TCP_KEEPINTVL, TCP_KEEPCNT).
    opts = Sys.islinux() ? (Cint(5), Cint(6)) :
           Sys.isapple() ? (Cint(0x101), Cint(0x102)) : nothing
    opts === nothing && return sock
    try
        fd = Base._fd(sock)
        for (opt, value) in zip(opts, (_KEEPALIVE_PROBE_INTERVAL, _KEEPALIVE_PROBE_COUNT))
            ref = Ref{Cint}(value)
            ccall(:setsockopt, Cint, (Cint, Cint, Cint, Ptr{Cvoid}, Cuint),
                  fd, _IPPROTO_TCP, opt, ref, sizeof(Cint))
        end
    catch
        # `Base._fd` is internal and the option numbers are platform-specific; a
        # failure here costs detection latency, not correctness.
    end
    return sock
end

function connect(client::Client)
    lock(client.state_lock) do
        if client.connected
            return client
        end

        try
            client.socket = _connect_socket(client.host, client.port)
            Sockets.nagle(client.socket, !client.nodelay)
            _enable_keepalive!(client.socket, client.keepalive)
            client.connected = true
            
            # Start the response handler
            @async _handle_responses(client)
            
            return client
        catch e
            client.connected = false
            rethrow(e)
        end
    end
end

function set_nodelay!(client::Client, enabled::Bool)
    lock(client.state_lock) do
        client.nodelay = enabled
        if client.connected
            Sockets.nagle(client.socket, !enabled)
        end
    end
    return client
end

function disconnect(client::Client)
    lock(client.state_lock) do
        if !client.connected
            return
        end
        
        try
            close(client.socket)
        finally
            client.connected = false
            
            # Clean up pending requests
            lock(client.requests_lock) do
                for pending in values(client.pending_requests)
                    close(pending.channel)
                end
                empty!(client.pending_requests)
            end
        end
    end
end

function _get_next_id(client::Client)::UInt64
    # atomic_add! returns the old value and adds, so we get unique sequential IDs
    old_val = Threads.atomic_add!(client.next_id, UInt64(1))
    return old_val
end

function _send_message(client::Client, msg::Message)
    # Check connection without lock for fast path
    if !client.connected
        throw(ErrorException("Client not connected"))
    end
    
    data = serialize_message(msg)
    
    # Lock for socket write to prevent message interleaving
    lock(client.write_lock) do
        write(client.socket, data)
        flush(client.socket)
    end
end

function send_notify(client::Client, method::String, params = nothing; 
                    query_format::QueryFormat = QUERY_JSON_POINTER,
                    body_format::BodyFormat = BODY_JSON)
    
    body_bytes = params === nothing ? UInt8[] : encode_body(params, body_format)
    
    msg = Message(
        id = _get_next_id(client),
        query = method,
        body = body_bytes,
        query_format = UInt16(query_format),
        body_format = UInt16(body_format),
        notify = true
    )
    
    _send_message(client, msg)
end

# Async version that returns a Task
function send_request_async(client::Client, method::String, params = nothing; 
                           query_format::QueryFormat = QUERY_JSON_POINTER,
                           body_format::BodyFormat = BODY_JSON,
                           timeout::Union{Float64, Nothing} = nothing,
                           result_type::Union{Nothing, Type} = nothing)
    
    return @async begin
        _send_request_sync(client, method, params; 
                         query_format=query_format, 
                         body_format=body_format, 
                         timeout=timeout,
                         result_type=result_type)
    end
end

# Deliver a response value (a parsed result, a `StreamedResponse`, or an
# Exception) to the waiter registered under `id`. The pending entry is *popped*
# here so that a duplicate or late response with the same id finds no entry (and
# cannot block on an already-satisfied channel), and so a timeout `Timer` that
# checks `haskey` under the same lock will not close a channel we just filled.
function _deliver(client::AbstractClient, id::UInt64, value)
    lock(client.requests_lock) do
        pending = pop!(client.pending_requests, id, nothing)
        pending === nothing && return
        try
            put!(pending.channel, value)
        catch e
            # The only expected failure is a closed channel: a timeout or
            # disconnect abandoned this waiter (under this same lock). Anything
            # else is a genuine surprise -- surface it rather than swallow it.
            e isa InvalidStateException || rethrow(e)
        end
    end
end

"""
    _response_value(msg, pending) -> Any

Turn a decoded response `Message` into the value handed to its waiter, shared by the
TCP reader and the WebSocket listener. A non-OK error code becomes a structured
`REPEError` so callers can switch on `.code` (e.g. `EC_METHOD_NOT_FOUND` vs
`EC_SVS_PRODUCER_ERROR`) instead of substring-matching a message; unknown application
codes collapse to the app base via `_error_code`, and the raw number is kept when the
server supplied no body. A decode failure is *returned*, not thrown, so it reaches the
waiter that asked for it rather than killing the reader.

Pure and lock-free by design: both callers run it after releasing `requests_lock`, so a
large `parse_body` cannot stall registration or timeout firing for the other requests
in flight on that connection.
"""
function _response_value(msg::Message, pending::PendingRequest)
    if msg.header.ec != UInt32(EC_OK)
        error_msg = isempty(msg.body) ?
            "RPC error (code $(msg.header.ec))" : String(msg.body)
        return REPEError(_error_code(msg.header.ec), error_msg)
    end
    return try
        # `msg` is discarded immediately after delivery, so the untyped decode can
        # consume/alias `msg.body` rather than copy it -- this is the ~1xN receive
        # cost the copy-reduction work targets (see parse_body's `copy`).
        pending.result_type === nothing ?
            parse_body(msg; copy=false) :
            parse_body(msg, pending.result_type)
    catch parse_err
        parse_err
    end
end

"""
    _oversize_response_error(header, limit) -> REPEError

The `EC_INVALID_HEADER` error delivered when a *response* declares a
`query_length`/`body_length` past `max_body_length`. Client-side counterpart of
[`_oversize_error_message`](@ref), which builds the server's reply to an oversized
request; shared by both transports so they reject in identical words.
"""
function _oversize_response_error(header::Header, limit)
    cap = limit === nothing ? "the addressable maximum" : string(limit)
    return REPEError(EC_INVALID_HEADER,
        "Response too large: query_length=$(header.query_length) body_length=$(header.body_length) exceeds max_body_length $(cap)")
end

# Wait for `channel` to receive a response, blocking (no polling) so request-heavy
# patterns are not floored by a poll interval. Timeout is enforced by a `Timer`
# that removes the pending entry and closes the channel on expiry; that wakes the
# blocking `take!` with an `InvalidStateException`, which we map to `TimeoutError`.
# A `nothing` timeout waits indefinitely (used for long streamed transfers); the
# wait then ends only when a response arrives or the connection drops (which
# closes the channel -> `ConnectionError`).
#
# The single-putter invariant the reader relies on is preserved: this waiter is
# never a putter, and the reader checks `haskey`/`pop!` under `requests_lock`,
# the same lock the Timer uses, so the reader never puts into a channel this
# Timer has abandoned.
function _await_response(client::AbstractClient, request_id::UInt64, channel::Channel,
                         timeout_val::Union{Nothing, Float64})
    timed_out = Threads.Atomic{Bool}(false)
    timer = nothing
    if timeout_val !== nothing
        timer = Timer(timeout_val) do _
            lock(client.requests_lock) do
                if haskey(client.pending_requests, request_id)
                    timed_out[] = true
                    delete!(client.pending_requests, request_id)
                    close(channel)
                end
            end
        end
    end

    result = try
        take!(channel)
    catch e
        e isa InvalidStateException || rethrow(e)
        # `timed_out` is written only by the Timer above, which exists only when
        # `timeout_val` is a real duration.
        timed_out[] ? TimeoutError(timeout_val::Float64) :
                      ConnectionError("Connection closed before response was received")
    finally
        timer === nothing || close(timer)
        lock(client.requests_lock) do
            delete!(client.pending_requests, request_id)
        end
    end

    # Raise outside the `try`. A `throw` from within the catch above would leave the
    # `InvalidStateException` on the exception stack, so the error reaching the
    # caller carries a `caused by: InvalidStateException: Channel is closed` chain --
    # REPE's own wakeup mechanism, presented as if it were the root cause. That
    # points whoever reads the stacktrace at internal plumbing instead of at the
    # connection.
    result isa Exception && throw(result)
    return result
end

# Register `pending` under `request_id`, then run the `send!` closure that writes
# the request to the wire. If the send throws, the request never reached the
# server (or the socket itself broke), so no response will arrive to wake a
# waiter: unregister and close the channel before rethrowing. Shared by the
# non-streaming request and the streamed *response* request, whose register /
# send / failure-cleanup lifecycles are identical. `stream_request` deliberately
# does NOT use this -- a failed streamed *upload* leaves a partial frame on the
# wire, so it must tear the whole connection down rather than just drop this one
# registration (see there).
function _register_and_send(client::AbstractClient, request_id::UInt64,
                            pending::PendingRequest, send!)
    lock(client.requests_lock) do
        client.pending_requests[request_id] = pending
    end
    try
        send!()
    catch e
        lock(client.requests_lock) do
            delete!(client.pending_requests, request_id)
        end
        try; close(pending.channel); catch; end
        rethrow(e)
    end
end

# Internal synchronous implementation
function _send_request_sync(client::Client, method::String, params = nothing;
                          query_format::QueryFormat = QUERY_JSON_POINTER,
                          body_format::BodyFormat = BODY_JSON,
                          timeout::Union{Float64, Nothing} = nothing,
                          result_type::Union{Nothing, Type} = nothing)

    if !client.connected
        connect(client)
    end

    request_id = _get_next_id(client)
    response_channel = Channel(1)

    body_bytes = params === nothing ? UInt8[] : encode_body(params, body_format)

    msg = Message(
        id = request_id,
        query = method,
        body = body_bytes,
        query_format = UInt16(query_format),
        body_format = UInt16(body_format),
        notify = false
    )

    _register_and_send(client, request_id, PendingRequest(response_channel, result_type),
                       () -> _send_message(client, msg))

    timeout_val = something(timeout, client.timeout)
    return _await_response(client, request_id, response_channel, timeout_val)
end

# Backward compatible synchronous version
function send_request(client::Client, method::String, params = nothing; 
                     query_format::QueryFormat = QUERY_JSON_POINTER,
                     body_format::BodyFormat = BODY_JSON,
                     timeout::Union{Float64, Nothing} = nothing,
                     result_type::Union{Nothing, Type} = nothing)
    
    return _send_request_sync(client, method, params; 
                            query_format=query_format, 
                            body_format=body_format, 
                            timeout=timeout,
                            result_type=result_type)
end

function send_request(::Type{T}, client::Client, method::String, params = nothing; 
                     query_format::QueryFormat = QUERY_JSON_POINTER,
                     body_format::BodyFormat = BODY_JSON,
                     timeout::Union{Float64, Nothing} = nothing) where {T}
    
    return _send_request_sync(client, method, params; 
                            query_format=query_format, 
                            body_format=body_format, 
                            timeout=timeout,
                            result_type=T)
end

function send_request_async(::Type{T}, client::Client, method::String, params = nothing; 
                           query_format::QueryFormat = QUERY_JSON_POINTER,
                           body_format::BodyFormat = BODY_JSON,
                           timeout::Union{Float64, Nothing} = nothing) where {T}
    
    return send_request_async(client, method, params; 
                             query_format=query_format, 
                             body_format=body_format, 
                             timeout=timeout,
                             result_type=T)
end

function _handle_responses(client::Client)
    # Bind this reader to the socket it was started for. A later reconnect swaps
    # in a fresh socket (and a fresh reader); capturing `sock` lets this reader
    # tell, on exit, whether it is still the current one before it tears down
    # shared state (see the guarded cleanup at the bottom).
    sock = client.socket

    while true
        # Check if still connected (with lock)
        connected = lock(client.state_lock) do
            client.connected
        end

        if !connected
            break
        end

        try
            if eof(sock)
                break
            end

            # Read header
            header_bytes = read(sock, HEADER_SIZE)
            if length(header_bytes) < HEADER_SIZE
                break
            end

            header = deserialize_header(header_bytes)

            # Guard the wire-controlled query/body lengths before reading or
            # allocating from them. Both are peer-controlled, and an unlimited cap
            # still rejects a length past typemax(Int). The oversized message
            # cannot be drained without defeating the guard, so the connection is
            # unrecoverable: surface the error to the waiter and tear it down.
            if !_within_length_limit(client.max_body_length, header.query_length) ||
               !_within_length_limit(client.max_body_length, header.body_length)
                _deliver(client, header.id,
                         _oversize_response_error(header, client.max_body_length))
                break
            end

            # Look up the pending request once, right after the header, and copy
            # the sink/buffer_size out so the (potentially very long) pump runs
            # without holding requests_lock and stalling registration/timeouts.
            pending = lock(client.requests_lock) do
                get(client.pending_requests, header.id, nothing)
            end

            if pending !== nothing && pending.sink !== nothing && header.ec == UInt32(EC_OK)
                # STREAM path: consume the echoed query, then pump the body into
                # the caller's sink in bounded chunks. This occupies the reader
                # for the whole transfer, blocking demux of any other response on
                # this connection.
                local written
                try
                    _pump_exact(sock, devnull, header.query_length, pending.buffer_size)
                    written = _pump_exact(sock, pending.sink, header.body_length, pending.buffer_size)
                catch pump_err
                    # Truncated/interrupted mid-body: the bytes already in the
                    # sink are NOT a complete body, and the connection is now
                    # desynced regardless of the error type. A short read on the
                    # socket surfaces from `_pump_exact` as `EOFError`; report it
                    # as the documented `ConnectionError` (the connection dropped
                    # mid-stream) with the original kept as the cause. A sink-side
                    # write failure (any other type) passes through unchanged.
                    # Either way, stop reading so the loop tears the connection
                    # down (the trailing cleanup wakes other waiters).
                    delivered = pump_err isa EOFError ?
                        ConnectionError("Connection closed mid-stream", pump_err) : pump_err
                    _deliver(client, header.id, delivered)
                    break
                end
                _deliver(client, header.id,
                    StreamedResponse(written, _to_body_format(header.body_format)))
            else
                # NORMAL path. Also covers: a sink-carrying request whose response
                # came back as an error (ec != OK, small error body), and a late
                # response with no matching pending request (bytes still consumed
                # to keep the stream framed, just not delivered).
                msg = _read_message(sock, header)

                pending === nothing ||
                    _deliver(client, header.id, _response_value(msg, pending))
            end

        catch e
            # Any error that reaches here is terminal for this connection. A
            # socket read failure (EOF, ECONNRESET, EPIPE, ...) means the peer is
            # gone; a header that will not parse means the stream is desynced.
            # Either way the socket does not heal, so we must stop -- retrying the
            # read just re-throws in a tight loop (a reset peer spins forever and,
            # worse, the spin can starve the timeout Timer so waiters never wake).
            # Per-message decode errors do not reach here: they are caught around
            # parse_body and delivered to their own waiter. The cleanup below
            # wakes every remaining waiter with a ConnectionError.
            connected = lock(client.state_lock) do
                client.connected
            end
            # A clean EOF (peer hung up), or any error once we are already tearing
            # the connection down, is expected and not worth a warning.
            if connected && !(e isa EOFError)
                @warn "Error handling response" exception=e
            end
            break
        end
    end

    # Connection is going down. Mark disconnected and wake every remaining waiter
    # so none blocks forever (matters especially for no-timeout streamed waits).
    #
    # Guard against a stale reader: if a reconnect has already swapped in a fresh
    # socket, that new connection owns `connected` and `pending_requests`, so this
    # reader must not clobber them. The check + teardown run under state_lock (with
    # the nested requests_lock matching disconnect's lock order) so a concurrent
    # connect() -- which also holds state_lock -- cannot interleave.
    lock(client.state_lock) do
        if client.socket === sock
            client.connected = false
            lock(client.requests_lock) do
                for pending in values(client.pending_requests)
                    try; close(pending.channel); catch; end
                end
                empty!(client.pending_requests)
            end
        end
    end
end

#==============================================================================#
# Streaming (TCP)
#==============================================================================#

"""
    stream_response(client::Client, method, params = nothing; into::IO,
                    query_format = QUERY_JSON_POINTER,
                    body_format  = BODY_JSON,
                    timeout      = nothing,
                    buffer_size  = client.buffer_size) -> StreamedResponse

Send a request and stream the response body directly into `into`, copying at most
`buffer_size` bytes at a time. The body is never fully materialized on the client;
peak memory is bounded by `buffer_size` regardless of body size. Returns a
[`StreamedResponse`](@ref) (metadata only) on success; the bytes are in `into`.

`body_format` is the encoding of the request `params`, **not** the response: the
server tags the response body independently, and that tag is reported back as
`StreamedResponse.body_format`. (For a param-less download the request body is
empty, so `body_format` has no effect at all.)

`timeout` defaults to `nothing` (wait indefinitely), since a large transfer can
legitimately run far longer than the per-request `client.timeout`. Pass an
explicit timeout to bound it. **A timed-out stream tears the connection down**:
the body is read incrementally, so a timeout mid-body leaves unread bytes on the
wire that cannot be resynced, and the only safe outcome is to drop the
connection (the next call reconnects). If the server responds with an error
instead of a body, this raises but the connection stays usable (the small error
body is read normally). On a truncated transfer or a timeout, whatever bytes
already reached `into` are partial, not a complete body.

The single background reader pumps the whole body before it can demux any other
response on this connection; route bulk transfers over a dedicated `Client` when
they must coexist with latency-sensitive traffic.
"""
function stream_response(client::Client, method::String, params = nothing; into::IO,
                         query_format::QueryFormat = QUERY_JSON_POINTER,
                         body_format::BodyFormat = BODY_JSON,
                         timeout::Union{Float64, Nothing} = nothing,
                         buffer_size::Integer = client.buffer_size)::StreamedResponse
    buffer_size < 1 && throw(ArgumentError("buffer_size must be positive, got $buffer_size"))

    if !client.connected
        connect(client)
    end

    request_id = _get_next_id(client)
    response_channel = Channel(1)

    body_bytes = params === nothing ? UInt8[] : encode_body(params, body_format)

    msg = Message(
        id = request_id,
        query = method,
        body = body_bytes,
        query_format = UInt16(query_format),
        body_format = UInt16(body_format),
        notify = false
    )

    _register_and_send(client, request_id,
                       PendingRequest(response_channel, nothing, into, Int(buffer_size)),
                       () -> _send_message(client, msg))

    try
        return _await_response(client, request_id, response_channel, timeout)
    catch e
        # A timed-out streamed receive cannot be recovered: the response body is
        # read incrementally by the background reader, so on timeout there are
        # unread body bytes still on the wire (the reader is mid-pump or about to
        # be) and the stream is desynced. Tear the connection down. This aborts
        # the reader's pump -- otherwise it would keep draining the entire body
        # into the now-abandoned `into` sink in the background, breaking the
        # bounded-memory guarantee and monopolizing the connection -- and forces
        # the next call to reconnect onto a clean stream. Only a timeout needs
        # this: a server error response (ec != OK) travels the normal receive
        # path and leaves the connection framed, and a connection drop has
        # already torn it down.
        e isa TimeoutError && disconnect(client)
        rethrow(e)
    end
end

"""
    download(client::Client, method, dest_path::AbstractString; params = nothing, kwargs...) -> StreamedResponse

Open `dest_path` for writing (truncating it) and stream the response body into it
via [`stream_response`](@ref). `params` and any other keywords are forwarded.

If the transfer fails partway, the partially written file is left in place and the
error is rethrown; callers needing atomicity should stream to a temp path and
rename on success.

Not exported (it would shadow `Downloads.download`); call it as `RepeRPC.download`.
"""
function download(client::Client, method::String, dest_path::AbstractString;
                  params = nothing, kwargs...)::StreamedResponse
    return open(dest_path, "w") do io
        stream_response(client, method, params; into = io, kwargs...)
    end
end

# Write a streamed request: header + query, then pump `nbytes` from `source` to
# the socket in chunks, under write_lock so it cannot interleave with another
# request on the same connection.
function _send_streamed_request(client::Client, request_id::UInt64, method::String,
                                source::IO, nbytes::Int, query_format::QueryFormat,
                                body_format::BodyFormat, buffer_size::Int)
    client.connected || throw(ErrorException("Client not connected"))

    query_bytes = Vector{UInt8}(method)
    header = Header(
        length = HEADER_SIZE + length(query_bytes) + nbytes,
        id = request_id,
        query_length = length(query_bytes),
        body_length = nbytes,
        query_format = UInt16(query_format),
        body_format = UInt16(body_format),
        notify = 0,
        ec = UInt32(EC_OK),
    )

    lock(client.write_lock) do
        write(client.socket, serialize_header(header))
        isempty(query_bytes) || write(client.socket, query_bytes)
        _pump_exact(source, client.socket, nbytes, buffer_size)
        flush(client.socket)
    end
end

"""
    stream_request(client::Client, method, source::IO, length::Integer;
                   query_format = QUERY_JSON_POINTER,
                   body_format  = BODY_RAW_BINARY,
                   timeout      = nothing,
                   buffer_size  = client.buffer_size,
                   result_type  = nothing) -> <parsed response>

Stream `length` bytes from `source` as the request body, in `buffer_size` chunks,
so the upload is never fully materialized on the client. `length` must be known
up front (it is carried in the header). Returns the parsed response exactly like
[`send_request`](@ref).

`timeout` bounds only the wait for the server's *response*; the upload pump
itself runs synchronously under the write lock and is not bounded by it. Like
[`send_request`](@ref), `timeout = nothing` (the default) maps to
`client.timeout`. This differs from [`stream_response`](@ref), where `nothing`
means wait indefinitely, because there the body transfer is part of the wait.

The server receives the body normally (materialized, bounded by its
`max_body_length`); a server-side zero-copy streaming *receive* handler is out of
scope here.
"""
function stream_request(client::Client, method::String, source::IO, length::Integer;
                        query_format::QueryFormat = QUERY_JSON_POINTER,
                        body_format::BodyFormat = BODY_RAW_BINARY,
                        timeout::Union{Float64, Nothing} = nothing,
                        buffer_size::Integer = client.buffer_size,
                        result_type::Union{Nothing, Type} = nothing)
    length < 0 && throw(ArgumentError("stream_request length must be non-negative, got $length"))
    buffer_size < 1 && throw(ArgumentError("buffer_size must be positive, got $buffer_size"))

    if !client.connected
        connect(client)
    end

    request_id = _get_next_id(client)
    response_channel = Channel(1)

    lock(client.requests_lock) do
        client.pending_requests[request_id] = PendingRequest(response_channel, result_type)
    end

    try
        _send_streamed_request(client, request_id, method, source, Int(length),
                               query_format, body_format, Int(buffer_size))
    catch e
        # A streamed send frames the header (committing body_length) and then
        # pumps the source under write_lock, so a mid-pump failure leaves a
        # partial frame on the wire with the socket otherwise healthy and the
        # server blocked reading the rest of the declared body. Unlike a normal
        # _send_message failure -- where the socket itself broke and the reader
        # tears the connection down -- here the *source* failed, so nothing else
        # heals the desync. Tear the connection down so the next call reconnects
        # onto a clean stream instead of writing a new header into a corrupted
        # frame. disconnect() also drains pending_requests (closing
        # response_channel) and is a no-op for a pre-write "not connected" error.
        disconnect(client)
        rethrow(e)
    end

    timeout_val = something(timeout, client.timeout)
    return _await_response(client, request_id, response_channel, timeout_val)
end

# Convenience function for concurrent batch requests
function batch(client::Client, requests::AbstractVector{<:Tuple{String, Any}}; kwargs...)
    tasks = Task[]
    for (method, params) in requests
        task = send_request_async(client, method, params; kwargs...)
        push!(tasks, task)
    end
    return tasks
end

# Wait for all tasks and collect results
function await_batch(tasks::Vector{Task})
    return [fetch(task) for task in tasks]
end

# Utility function to check connection status
function isconnected(client::Client)
    return client.connected
end

# Pretty printing for REPL
function Base.show(io::IO, client::Client)
    status = client.connected ? "connected" : "disconnected"
    print(io, "Client(\"$(client.host):$(client.port)\", $status)")
end
