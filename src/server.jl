"""
    print_stacktrace(error)

Print out a stacktrace from a try-catch error
"""
function print_stacktrace(error)
    Base.printstyled("ERROR: "; color=:red, bold=true)
    Base.showerror(stdout, error)
    Base.show_backtrace(stdout, Base.catch_backtrace())
end

mutable struct Server <: AbstractServer
    host::String
    port::Int
    server::Union{Sockets.TCPServer,Nothing}
    running::Bool
    handlers::Dict{String,Function}
    middleware::Vector{Function}
    print_stacktrace::Bool
    nodelay::Bool
    # Seconds an accepted connection may sit idle before TCP keepalive probing
    # starts; `nothing` disables keepalive. See `_enable_keepalive!`.
    keepalive::Union{Nothing,Float64}
    # Reject a request whose header declares a body larger than this many bytes
    # without reading it (replying with EC_INVALID_HEADER and closing). `nothing`
    # means unlimited (default).
    max_body_length::Union{Nothing,Int}
    # Chunk size used when streaming a `StreamBody` response to the socket.
    buffer_size::Int

    function Server(host::String="localhost", port::Int=8080; print_stacktrace::Bool=false,
                    nodelay::Bool=true,
                    keepalive::Union{Nothing,Real}=DEFAULT_KEEPALIVE_IDLE,
                    max_body_length::Union{Nothing,Integer}=nothing,
                    buffer_size::Integer=DEFAULT_BUFFER_SIZE)
        buffer_size < 1 && throw(ArgumentError("buffer_size must be positive, got $buffer_size"))
        keepalive !== nothing && !(isfinite(keepalive) && 0 < keepalive <= typemax(Cuint)) &&
            throw(ArgumentError("keepalive must be a finite number of seconds in (0, $(typemax(Cuint))] (use nothing to disable), got $keepalive"))
        max_body_length !== nothing && max_body_length < 0 &&
            throw(ArgumentError("max_body_length must be non-negative or nothing, got $max_body_length"))
        new(host, port, nothing, false, Dict{String,Function}(), Function[], print_stacktrace, nodelay,
            keepalive === nothing ? nothing : Float64(keepalive),
            max_body_length === nothing ? nothing : Int(max_body_length),
            Int(buffer_size))
    end
end

function _resolve_listen_address(host::String)
    normalized = strip(host)

    if isempty(normalized) || normalized == "*" || normalized == "0.0.0.0"
        return Sockets.IPv4(0)
    elseif normalized == "::"
        return Sockets.IPv6(0)
    end

    for family in (Sockets.IPv4, Sockets.IPv6)
        try
            return Sockets.getaddrinfo(normalized, family)
        catch
            # Try the next family
        end
    end

    error(ErrorException("Failed to resolve listen address for host $host"))
end

function register(server::Server, method::String, handler::Function)
    server.handlers[method] = handler
end

function use(server::Server, middleware::Function)
    push!(server.middleware, middleware)
end

function _start_server(server::Server)
    if server.running
        return
    end

    listen_addr = _resolve_listen_address(server.host)
    server.server = Sockets.listen(listen_addr, server.port)
    server.running = true

    @info "REPE Server listening on $(server.host):$(server.port)"

    # Yield to allow other tasks to run
    yield()

    while server.running
        try
            client = accept(server.server)
            Sockets.nagle(client, !server.nodelay)
            # A client that vanishes without a FIN leaves this connection's task
            # parked in `eof(client)` forever, holding a socket and whatever the
            # `ServerConnection` close hook would have released. Exhausting file
            # descriptors that way stops the server accepting anyone. Keepalive is
            # the same fix as on the client, in the other direction.
            _enable_keepalive!(client, server.keepalive)
            @async _handle_client(server, client)
        catch e
            if server.running
                @error "Error accepting connection" exception = e
            end
        end
    end
end

function stop(server::Server)
    if !server.running
        return
    end

    server.running = false

    if server.server !== nothing
        close(server.server)
        server.server = nothing
    end

    _run_server_stop_hooks(server)
    @info "REPE Server stopped"
end

function _handle_client(server::Server, client::TCPSocket)
    @info "Client connected"

    conn = ServerConnection()
    try
        while isopen(client) && server.running
            if eof(client)
                break
            end

            header_bytes = read(client, HEADER_SIZE)
            if length(header_bytes) < HEADER_SIZE
                break
            end

            header = deserialize_header(header_bytes)

            # Guard the wire-controlled query/body lengths before reading or
            # allocating from them. Both are peer-controlled (deserialize_header
            # only checks their sum against `length`), so both must be bounded,
            # and an unlimited cap still rejects a length past typemax(Int). We
            # cannot drain an oversized message without defeating the guard, so we
            # reply EC_INVALID_HEADER (echoing the id) and close the connection.
            if !_within_length_limit(server.max_body_length, header.query_length) ||
               !_within_length_limit(server.max_body_length, header.body_length)
                if header.notify == 0
                    try
                        write(client, serialize_message(_oversize_error_message(header, server.max_body_length)))
                        flush(client)
                    catch
                        # Peer may already be gone; nothing more we can do.
                    end
                end
                @warn "Rejecting oversized request" query_length=header.query_length body_length=header.body_length max_body_length=server.max_body_length
                break
            end

            request = _read_message(client, header)

            response = _process_request(server, request, conn)

            if response isa StreamBody
                if request.header.notify == 0
                    _write_streamed_response(client, request, response; buffer_size=server.buffer_size)
                else
                    # No response is sent for a notify; just release the source.
                    response.own && (try; close(response.io); catch; end)
                end
            elseif request.header.notify == 0
                response_data = serialize_message(response)
                write(client, response_data)
                flush(client)
            end
        end
    catch e
        @error "Error handling client" exception = e
        if server.print_stacktrace
            print_stacktrace(e)
        end
    finally
        _close_connection!(conn)
        close(client)
        @info "Client disconnected"
    end
end

# ---- per-connection teardown hook -------------------------------------------
#
# A request handler runs with no inherent knowledge of the connection it arrived
# on, yet some resources (an SVS producer session, a subscription, a temp file)
# must live exactly as long as that connection and be released the instant it
# drops -- whether the peer sent a clean teardown or simply vanished.
# `ServerConnection` carries that lifetime: a handler that accepts a third
# argument `(params, request, conn)` receives it and registers `on_close`
# callbacks, which the transport runs once when the connection's read loop ends.

const _CONNECTION_ID = Threads.Atomic{UInt64}(1)
_next_connection_id() = Threads.atomic_add!(_CONNECTION_ID, UInt64(1))

"""
    ServerConnection

The lifetime of one client connection, handed to request handlers that opt in by
accepting a third argument `(params, request, conn)`. Register cleanup with
[`on_close`](@ref); each callback runs exactly once when the connection closes
(or immediately, if it is already closed when registered). `id` is a
process-unique connection identifier. Handlers that take only `(params, request)`
never see this and are unaffected.
"""
mutable struct ServerConnection
    id::UInt64
    callbacks::Vector{Any}
    closed::Bool
    lock::ReentrantLock
end
ServerConnection() = ServerConnection(_next_connection_id(), Any[], false, ReentrantLock())

_run_close_callback(cb) =
    try
        cb()
    catch err
        @error "ServerConnection close callback failed" exception = (err, catch_backtrace())
    end

"""
    on_close(conn::ServerConnection, callback)

Register `callback` (a zero-argument callable) to run when `conn` closes. If
`conn` has already closed, `callback` runs immediately, so a teardown racing the
registration never orphans the resource. Callbacks run once, in registration
order; a throwing callback is logged and does not block the others.
"""
function on_close(conn::ServerConnection, callback)
    run_now = lock(conn.lock) do
        conn.closed && return true
        push!(conn.callbacks, callback)
        return false
    end
    run_now && _run_close_callback(callback)
    return nothing
end

# Run every registered callback once and mark the connection closed. Idempotent,
# since a transport may reach teardown via both an error and its `finally`.
# Callbacks run outside the lock so one may re-enter `on_close` without deadlock.
function _close_connection!(conn::ServerConnection)
    cbs = lock(conn.lock) do
        conn.closed && return nothing
        conn.closed = true
        cs = conn.callbacks
        conn.callbacks = Any[]
        return cs
    end
    cbs === nothing && return nothing
    for cb in cbs
        _run_close_callback(cb)
    end
    return nothing
end

# ---- server-lifetime teardown hook ------------------------------------------
#
# Per-*server* teardown, distinct from the per-connection `ServerConnection`
# above. A feature that spawns a server-lifetime background task (e.g. the SVS
# session reaper) registers here so `stop()` tears it down deterministically,
# rather than the task having to infer shutdown from a polled flag (which cannot
# distinguish "not started yet" from "stopped" and so leaks on short lifecycles).
# Keyed by server identity in a side table, so neither server struct needs a new
# field. Hooks run once, on the first `stop()`.
const _SERVER_STOP_HOOKS = IdDict{Any,Vector{Any}}()
const _SERVER_STOP_HOOKS_LOCK = ReentrantLock()

"""
    on_server_stop(server, callback)

Register `callback` (a zero-argument callable) to run when `server` stops. Used
by server-lifetime background machinery to release itself deterministically.
Callbacks run once, in registration order, on the first [`stop`](@ref).
"""
function on_server_stop(server, callback)
    lock(_SERVER_STOP_HOOKS_LOCK) do
        push!(get!(() -> Any[], _SERVER_STOP_HOOKS, server), callback)
    end
    return nothing
end

# Run and clear a server's stop hooks. Idempotent: the second call finds no
# entry. `stop()` guards on `running`, so this fires on the first real stop only.
function _run_server_stop_hooks(server)
    cbs = lock(_SERVER_STOP_HOOKS_LOCK) do
        pop!(_SERVER_STOP_HOOKS, server, nothing)
    end
    cbs === nothing && return nothing
    for cb in cbs
        try
            cb()
        catch err
            @error "server stop hook failed" exception = (err, catch_backtrace())
        end
    end
    return nothing
end

# True iff `handler` has a non-vararg method taking exactly three positional
# arguments. Used to decide whether to pass the connection context. Excludes
# vararg handlers (`m.isva`), so a `(params, req, rest...)` handler is not
# silently handed `conn` as `rest[1]` -- `applicable` cannot make that
# distinction. `m.nargs` counts the function itself, so three params => 4.
_accepts_connection(handler) = any(m -> m.nargs == 4 && !m.isva, methods(handler))

# Dispatch to a handler, passing the connection context only to handlers that
# accept it (a 3-arg `(params, request, conn)` method). Plain 2-arg handlers are
# called as before, so the hook is fully backward compatible.
function _invoke_handler(handler, params, request, conn)
    if conn !== nothing && _accepts_connection(handler)
        return handler(params, request, conn)
    end
    return handler(params, request)
end

function _process_request_pipeline(handlers::Dict{String,Function},
                                   middleware::Vector{Function},
                                   request::Message,
                                   do_print_stacktrace::Bool,
                                   conn::Union{ServerConnection,Nothing} = nothing)::Union{Message,StreamBody}
    try
        for mw in middleware
            result = mw(request)
            if result !== nothing
                if result isa Message
                    return result
                elseif result isa ErrorCode
                    return _create_error_response(request, result)
                end
            end
        end

        method = parse_query(request)

        if !haskey(handlers, method)
            return _create_error_response(request, EC_METHOD_NOT_FOUND,
                "Method not found: $method")
        end

        handler = handlers[method]

        params = parse_body(request)

        result = _invoke_handler(handler, params, request, conn)

        if result isa Message || result isa StreamBody
            # A StreamBody asks the transport to stream a source as the body; the
            # TCP server handles it in _handle_client; the WebSocket server
            # rejects it (HTTP.jl reassembles each frame whole on receive, so
            # streaming there bounds nothing -- see the StreamBody docstring).
            return result
        else
            return create_response(request, result)
        end

    catch e
        @error "Error processing request" exception = e
        if do_print_stacktrace
            print_stacktrace(e)
        end
        return _create_error_response(request, EC_PARSE_ERROR, string(e))
    end
end

function _process_request(server::Server, request::Message,
                          conn::Union{ServerConnection,Nothing} = nothing)::Union{Message,StreamBody}
    return _process_request_pipeline(server.handlers, server.middleware, request,
                                     server.print_stacktrace, conn)
end

"""
    _write_streamed_response(socket, request, body::StreamBody; buffer_size)

Stream a `StreamBody` to `socket` as the response to `request`: write a header
that echoes the request `id`/`query` with `body_length = body.length` and
`ec = EC_OK`, then pump `body.io` to the socket in `buffer_size` chunks, bypassing
`serialize_message`'s single buffer. Closes `body.io` afterwards when `body.own`.

Because `body_length` is committed in the header before the first body byte, a
source that fails or runs short mid-stream cannot be signalled to the peer: the
error propagates (the caller, `_handle_client`, closes the socket) so the peer
sees a truncated transfer. The underlying cause is logged.
"""
function _write_streamed_response(socket::IO, request::Message, body::StreamBody;
                                  buffer_size::Integer=DEFAULT_BUFFER_SIZE)
    query_bytes = Vector{UInt8}(request.query)
    header = Header(
        length = HEADER_SIZE + length(query_bytes) + body.length,
        id = request.header.id,
        query_length = length(query_bytes),
        body_length = body.length,
        query_format = request.header.query_format,
        body_format = UInt16(body.body_format),
        notify = 0,
        ec = UInt32(EC_OK),
    )

    try
        write(socket, serialize_header(header))
        isempty(query_bytes) || write(socket, query_bytes)
        _pump_exact(body.io, socket, body.length, buffer_size)
        flush(socket)
    catch e
        @error "Streamed response failed mid-transfer; closing connection" exception=e id=request.header.id
        rethrow(e)
    finally
        body.own && (try; close(body.io); catch; end)
    end
end

"""
    _oversize_error_message(header, limit) -> Message

Build the `EC_INVALID_HEADER` response sent when an incoming request's declared
`query_length`/`body_length` exceeds `max_body_length` (or, under an unlimited
cap, exceeds the addressable maximum). Echoes the request `id` so the client can
correlate it; carries no query (we reject before reading it).
"""
function _oversize_error_message(header::Header, limit)
    cap = limit === nothing ? "the addressable maximum" : string(limit)
    msg = "Request too large: query_length=$(header.query_length) body_length=$(header.body_length) exceeds max_body_length $(cap)"
    return Message(
        id = header.id,
        query = "",
        body = msg,
        body_format = UInt16(BODY_UTF8),
        ec = UInt32(EC_INVALID_HEADER),
    )
end

function set_nodelay!(server::Server, enabled::Bool)
    server.nodelay = enabled
    return server
end

function _create_error_response(request::Message, ec::ErrorCode, msg::String="")::Message
    error_msg = isempty(msg) ? get(ERROR_MESSAGES, ec, "Unknown error") : msg

    return Message(
        id=request.header.id,
        query=request.query,
        body=error_msg,
        query_format=request.header.query_format,
        body_format=UInt16(BODY_UTF8),
        ec=UInt32(ec)
    )
end

function listen(server::Server; async::Bool=false)
    if async
        @async _start_server(server)
    else
        _start_server(server)
    end
end

function wait_for_server(host::String, port::Int; attempts::Int=50, delay::Float64=0.1)
    for _ in 1:attempts
        try
            sock = _connect_socket(host, port)
            close(sock)
            return
        catch e
            if isa(e, Base.IOError) || isa(e, Base.UVError)
                sleep(delay)
            else
                rethrow(e)
            end
        end
    end

    error(ErrorException("Server failed to start on $host:$port"))
end

# Pretty printing for REPL
function Base.show(io::IO, server::Server)
    status = server.running ? "running" : "stopped"
    n_handlers = length(server.handlers)
    handler_text = n_handlers == 1 ? "1 handler" : "$n_handlers handlers"
    print(io, "Server(\"$(server.host):$(server.port)\", $status, $handler_text)")
end
