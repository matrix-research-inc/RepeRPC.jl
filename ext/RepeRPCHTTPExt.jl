module RepeRPCHTTPExt

import HTTP
import HTTP.WebSockets
using RepeRPC
using Sockets: getsockname

# ========== WSServer ==========

"""
    _WSServer

Internal struct backing [`RepeRPC.WSServer`](@ref). Carries optional TLS
configuration (`sslconfig`) and bearer-token / pluggable auth state
(`auth_token`, `auth_verifier`).
"""
mutable struct _WSServer <: RepeRPC.AbstractServer
    host::String
    port::Int
    server_handle::Any
    running::Bool
    handlers::Dict{String,Function}
    middleware::Vector{Function}
    print_stacktrace::Bool
    html::String
    http_handler::Union{Function, Nothing}
    # Security-relevant configuration is fixed at construction. `const`
    # makes that intent enforceable so an unsafe mutation cannot land
    # after listen() with no compile-time warning.
    const sslconfig::Any
    const auth_token::Union{Nothing,String}
    const auth_verifier::Union{Nothing,Function}
end

"""
    RepeRPC.WSServer(host="localhost", port=8080; kwargs...) -> WSServer

Construct a REPE WebSocket server. The transport is plain `ws://` unless
`sslconfig` is supplied, in which case the server speaks `wss://` natively.

# Keyword Arguments
- `print_stacktrace`: log full stack traces on handler errors. Default `false`.
- `html`: optional HTML body served on non-upgrade HTTP requests (e.g., a
  status page). Default `""` (responds with `426 Upgrade Required`).
- `http_handler`: optional `(http) -> Bool` callback for handling non-upgrade
  HTTP requests. Returning `true` means the handler wrote a response; `false`
  falls through to the default 426/HTML response.
- `sslconfig`: an `MbedTLS.SSLConfig` (or compatible) to terminate TLS inside
  the server. Required for `wss://`. When `nothing` (default) the server
  serves plain `ws://`.
- `auth_token`: shared secret. When set, the server requires every incoming
  request to carry `Authorization: Bearer <token>`. Mismatched or missing
  tokens get `401 Unauthorized` and the connection is closed before the
  WebSocket upgrade. Comparison is constant-time to avoid token-byte timing
  leaks.
- `auth_verifier`: `(http::HTTP.Stream) -> Bool` callback for custom auth
  (e.g., scope/claims checks, IP allow-lists). When set it takes precedence
  over `auth_token`. Throwing or returning anything other than `true` is
  treated as failure.

# Example

```julia
server = RepeRPC.WSServer("0.0.0.0", 8443;
                      sslconfig  = MbedTLS.SSLConfig(certfile, keyfile),
                      auth_token = ENV["REPE_TOKEN"])
```
"""
function RepeRPC.WSServer(host::String="localhost", port::Int=8080;
                       print_stacktrace::Bool=false, html::String="",
                       http_handler::Union{Function, Nothing}=nothing,
                       sslconfig = nothing,
                       auth_token::Union{Nothing,AbstractString} = nothing,
                       auth_verifier::Union{Nothing,Function} = nothing)
    # Reject empty token at construction so a misconfigured ENV var cannot
    # collapse the auth check to "matches `Authorization: Bearer ` with
    # empty bytes after the prefix."
    auth_token !== nothing && isempty(auth_token) &&
        throw(ArgumentError("auth_token must be non-empty; pass nothing to disable authentication"))

    return _WSServer(host, port, nothing, false,
                     Dict{String,Function}(), Function[],
                     print_stacktrace, html, http_handler,
                     sslconfig,
                     auth_token === nothing ? nothing : String(auth_token),
                     auth_verifier)
end

# Cap on the Authorization header we will examine. A malicious or buggy
# client could otherwise send a multi-MB header that we pay an allocation
# and a constant-time loop over. Real bearer tokens are well under 1 KB.
const _MAX_AUTH_HEADER_BYTES = 8192

# Textbook constant-time compare: equal-length only, with no early exit
# during the byte loop. Length itself is not the secret (an attacker
# already knows or can guess token length); the bytes are. This is the
# same pattern as OpenSSL `CRYPTO_memcmp` and Python `hmac.compare_digest`.
function _constant_time_eq(a::AbstractString, b::AbstractString)
    cu_a = codeunits(a)
    cu_b = codeunits(b)
    length(cu_a) == length(cu_b) || return false
    diff = UInt8(0)
    @inbounds for i in eachindex(cu_a)
        diff |= xor(cu_a[i], cu_b[i])
    end
    return diff == 0x00
end

function _extract_bearer_token(http)
    val = HTTP.header(http.message, "Authorization", "")
    isempty(val) && return nothing
    # Defense in depth: do not even try to parse oversized headers.
    sizeof(val) > _MAX_AUTH_HEADER_BYTES && return nothing
    startswith(val, "Bearer ") || return nothing
    return SubString(val, length("Bearer ") + 1)
end

function _authorize_request(server::_WSServer, http)
    if server.auth_verifier !== nothing
        try
            return server.auth_verifier(http) === true
        catch e
            if server.print_stacktrace
                # Pair the exception with its backtrace so print_stacktrace
                # actually surfaces a stacktrace.
                @warn "auth_verifier threw" exception=(e, catch_backtrace())
            end
            return false
        end
    end
    if server.auth_token !== nothing
        presented = _extract_bearer_token(http)
        presented === nothing && return false
        return _constant_time_eq(presented, server.auth_token)
    end
    return true
end

function _reject_unauthorized(http)
    HTTP.setstatus(http, 401)
    HTTP.setheader(http, "WWW-Authenticate" => "Bearer realm=\"REPE\"")
    HTTP.setheader(http, "Content-Type" => "text/plain; charset=utf-8")
    HTTP.setheader(http, "Connection" => "close")
    HTTP.startwrite(http)
    Base.write(http, "401 Unauthorized")
end

function RepeRPC.register(server::_WSServer, method::String, handler::Function)
    server.handlers[method] = handler
end

function RepeRPC.use(server::_WSServer, middleware::Function)
    push!(server.middleware, middleware)
end

function RepeRPC.listen(server::_WSServer; async::Bool=false)
    server.running = true
    scheme = server.sslconfig === nothing ? "ws" : "wss"
    @info "REPE WebSocket Server listening on $scheme://$(server.host):$(server.port)"

    ws_handler = function(http)
        # Auth runs before any other dispatch so unauthorized callers cannot
        # reach the HTTP handler, the HTML body, or trigger a WebSocket
        # upgrade. The same gate applies to upgrade and non-upgrade requests.
        if !_authorize_request(server, http)
            try
                _reject_unauthorized(http)
            catch
                # Client may already have torn down the socket; ignore.
            end
            return
        end

        if HTTP.WebSockets.isupgrade(http.message)
            HTTP.WebSockets.upgrade(http) do ws
                _handle_ws_connection(server, ws)
            end
        else
            read(http)  # consume request body before responding
            # Try custom HTTP handler first
            if server.http_handler !== nothing && server.http_handler(http)
                return
            end
            if !isempty(server.html)
                HTTP.setstatus(http, 200)
                HTTP.setheader(http, "Content-Type" => "text/html; charset=utf-8")
            else
                HTTP.setstatus(http, 426)
                HTTP.setheader(http, "Upgrade" => "websocket")
            end
            HTTP.setheader(http, "Connection" => "close")
            HTTP.startwrite(http)
            Base.write(http, !isempty(server.html) ? server.html :
                "426 Upgrade Required: this is a REPE WebSocket server")
        end
    end

    # Build kwargs for HTTP.listen!: sslconfig only when set, so the plaintext
    # path stays untouched when no TLS is configured.
    listen_kwargs = server.sslconfig === nothing ?
        NamedTuple() : (sslconfig = server.sslconfig,)

    if async
        server.server_handle = HTTP.listen!(ws_handler, server.host, server.port;
                                            listen_kwargs...)
        # When port 0 is requested, update with the actual bound port
        if server.port == 0
            addr = getsockname(server.server_handle.listener.server)
            server.port = Int(addr[2])
        end
        yield()
    else
        HTTP.listen(ws_handler, server.host, server.port; listen_kwargs...)
    end
end

function RepeRPC.stop(server::_WSServer)
    if !server.running
        return
    end
    server.running = false
    if server.server_handle !== nothing
        close(server.server_handle)
        server.server_handle = nothing
    end
    RepeRPC._run_server_stop_hooks(server)
    @info "REPE WebSocket Server stopped"
end

function _handle_ws_connection(server::_WSServer, ws)
    write_lock = ReentrantLock()
    # One context for the whole connection, shared by every per-request task it
    # spawns. Closed once below when the read loop ends (clean close or drop), so
    # connection-scoped resources (e.g. SVS sessions) are released immediately
    # rather than waiting on an idle timeout.
    conn = RepeRPC.ServerConnection()
    try
        for data in ws
            if !server.running
                break
            end

            msg_bytes = data isa Vector{UInt8} ? data : Vector{UInt8}(data)
            request = RepeRPC.deserialize_message(msg_bytes)

            # Process each request asynchronously so that slow handlers
            # don't block the read loop or starve other connections.
            @async try
                response = RepeRPC._process_request_pipeline(
                    server.handlers, server.middleware, request, server.print_stacktrace, conn
                )

                if response isa RepeRPC.StreamBody
                    # Streaming buys nothing over WebSocket: HTTP.jl's receive
                    # reassembles every frame into one in-memory buffer before
                    # yielding (no per-frame read), so a receiver materializes the
                    # whole body no matter how the sender frames it -- the memory
                    # bound StreamBody exists for is unreachable end-to-end here.
                    # Reject with a clear error instead of pretending to stream,
                    # and release the source.
                    response.own && (try; close(response.io); catch; end)
                    if request.header.notify == 0
                        err = RepeRPC._create_error_response(request, RepeRPC.EC_INVALID_HEADER,
                            "StreamBody responses are not supported over the WebSocket transport; use a TCP Server for streamed bodies")
                        lock(write_lock) do
                            HTTP.WebSockets.send(ws, RepeRPC.serialize_message(err))
                        end
                    end
                elseif request.header.notify == 0
                    response_data = RepeRPC.serialize_message(response)
                    lock(write_lock) do
                        HTTP.WebSockets.send(ws, response_data)
                    end
                end
            catch e
                if server.running
                    @error "Error processing WebSocket request" exception=e
                    if server.print_stacktrace
                        RepeRPC.print_stacktrace(e)
                    end
                end
            end
        end
    catch e
        if server.running
            @error "Error handling WebSocket client" exception=e
            if server.print_stacktrace
                RepeRPC.print_stacktrace(e)
            end
        end
    finally
        # The read loop has ended (clean close, drop, or error): release every
        # resource tied to this connection. A request task still in flight may
        # register a callback after this runs; `on_close` then runs it at once.
        RepeRPC._close_connection!(conn)
    end
end

function Base.show(io::IO, server::_WSServer)
    status = server.running ? "running" : "stopped"
    n_handlers = length(server.handlers)
    handler_text = n_handlers == 1 ? "1 handler" : "$n_handlers handlers"
    scheme = server.sslconfig === nothing ? "ws" : "wss"
    auth = server.auth_verifier !== nothing ? ", auth=verifier" :
           server.auth_token !== nothing ? ", auth=token" : ""
    print(io, "WSServer(\"$scheme://$(server.host):$(server.port)\", $status, $handler_text$auth)")
end

# ========== WSClient ==========

"""
    _WSClient

Internal struct backing [`RepeRPC.WSClient`](@ref). Subtypes [`RepeRPC.AbstractClient`](@ref)
so it can be carried by a [`RepeRPC.Fleet`](@ref) `Node` alongside TCP clients.
"""
mutable struct _WSClient <: RepeRPC.AbstractClient
    const url::String
    ws::Any
    connected::Bool
    const timeout::Float64
    # Cap on an accepted response's query/body length, mirroring the TCP
    # `Client`. `nothing` is unlimited. Enforced in `_ws_response_loop`; see the
    # note there on why this is a post-receive ceiling rather than the TCP
    # client's pre-allocation bound.
    const max_body_length::Union{Nothing,Int}
    # Security-relevant configuration is fixed at construction; see the
    # matching note on `_WSServer`.
    const auth_token::Union{Nothing,String}
    const require_ssl_verification::Bool
    const sslconfig::Any
    const extra_headers::Vector{Pair{String,String}}
    next_id::Threads.Atomic{UInt64}
    pending_requests::Dict{UInt64, RepeRPC.PendingRequest}
    listener_task::Union{Task, Nothing}
    state_lock::ReentrantLock
    requests_lock::ReentrantLock
    write_lock::ReentrantLock
end

"""
    RepeRPC.WSClient(url; kwargs...) -> WSClient
    RepeRPC.WSClient(host, port; kwargs...) -> WSClient

Construct a REPE WebSocket client. Use a `wss://` URL to negotiate TLS via
HTTP.jl's built-in stack.

# Keyword Arguments
- `timeout`: per-request timeout in seconds. Default `30.0`.
- `max_body_length`: cap on an accepted response's `query_length`/`body_length`.
  A response declaring a larger size is refused with an error rather than parsed
  or delivered. Default `nothing` (unlimited). Unlike the TCP [`Client`](@ref),
  where this is checked before the body is read off the socket, HTTP.jl has
  already buffered the whole WebSocket frame by the time REPE sees the header, so
  here it is a post-receive ceiling: it bounds what REPE accepts and delivers
  (and avoids the extra parse/copy), not the underlying frame buffering. Use a
  TCP node when you need a hard pre-allocation bound.
- `auth_token`: bearer token, sent as `Authorization: Bearer <token>` on the
  WebSocket upgrade request. Pair this with a matching `auth_token` on
  [`RepeRPC.WSServer`](@ref).
- `require_ssl_verification`: when `false`, the client accepts self-signed
  certificates. Default `true`. Only meaningful for `wss://`.
- `sslconfig`: an `MbedTLS.SSLConfig` (or compatible) for custom CA bundles or
  client certificates. Passed through to HTTP.jl.
- `extra_headers`: additional HTTP headers to send during the upgrade. Useful
  for custom auth schemes when `auth_verifier` on the server inspects headers
  other than `Authorization`.

# Example

```julia
client = RepeRPC.WSClient("wss://data-1.example.com/repe";
                      auth_token = ENV["REPE_TOKEN"])
RepeRPC.connect(client)
```
"""
function RepeRPC.WSClient(url::AbstractString;
                       timeout::Float64 = 30.0,
                       max_body_length::Union{Nothing,Integer} = nothing,
                       auth_token::Union{Nothing,AbstractString} = nothing,
                       require_ssl_verification::Bool = true,
                       sslconfig = nothing,
                       extra_headers = nothing)
    # Same construction-time empty-token check as the server side.
    auth_token !== nothing && isempty(auth_token) &&
        throw(ArgumentError("auth_token must be non-empty; pass nothing to disable authentication"))

    # Same non-negative check as the TCP `Client`/`Server`.
    max_body_length !== nothing && max_body_length < 0 &&
        throw(ArgumentError("max_body_length must be non-negative or nothing, got $max_body_length"))

    headers = if extra_headers === nothing
        Pair{String,String}[]
    else
        [String(k) => String(v) for (k, v) in extra_headers]
    end

    # Refuse ambiguous configurations: if the caller wants a bearer token
    # AND has supplied an Authorization entry in extra_headers, the wire
    # would carry two Authorization headers and server-side dedup behavior
    # is undefined. Force the caller to pick one.
    if auth_token !== nothing && any(p -> lowercase(p.first) == "authorization", headers)
        throw(ArgumentError("Cannot pass both auth_token and an `Authorization` entry in extra_headers; choose one"))
    end

    return _WSClient(String(url), nothing, false, timeout,
                     max_body_length === nothing ? nothing : Int(max_body_length),
                     auth_token === nothing ? nothing : String(auth_token),
                     require_ssl_verification,
                     sslconfig,
                     headers,
                     Threads.Atomic{UInt64}(1),
                     Dict{UInt64, RepeRPC.PendingRequest}(),
                     nothing,
                     ReentrantLock(), ReentrantLock(), ReentrantLock())
end

function RepeRPC.WSClient(host::String, port::Int; kwargs...)
    return RepeRPC.WSClient("ws://$host:$port"; kwargs...)
end

function RepeRPC.connect(client::_WSClient)
    should_connect = lock(client.state_lock) do
        if client.connected
            return false
        end
        if client.listener_task !== nothing && !istaskdone(client.listener_task)
            return false
        end
        return true
    end

    if !should_connect
        return client
    end

    ready = Channel{Any}(1)

    # Auth header and TLS kwargs are assembled per-connection so that token or
    # cert rotation between connect attempts is honored without rebuilding the
    # client struct.
    headers = copy(client.extra_headers)
    if client.auth_token !== nothing
        push!(headers, "Authorization" => "Bearer $(client.auth_token)")
    end

    open_kwargs = Dict{Symbol,Any}(:headers => headers)
    # require_ssl_verification only matters for wss://; HTTP.jl ignores it for ws://
    open_kwargs[:require_ssl_verification] = client.require_ssl_verification
    if client.sslconfig !== nothing
        open_kwargs[:sslconfig] = client.sslconfig
    end

    client.listener_task = @async begin
        try
            HTTP.WebSockets.open(client.url; open_kwargs...) do ws
                lock(client.state_lock) do
                    client.ws = ws
                    client.connected = true
                end
                put!(ready, true)
                _ws_response_loop(client, ws)
            end
        catch e
            if !isready(ready)
                put!(ready, e)
            end
        finally
            # This cleanup may race with disconnect(), which performs the same
            # operations. Both paths are safe: close() is guarded by try/catch
            # and empty!() is idempotent. The duplication ensures cleanup
            # happens regardless of which path runs first.
            lock(client.state_lock) do
                client.connected = false
                client.ws = nothing
            end
            lock(client.requests_lock) do
                for pending in values(client.pending_requests)
                    try; close(pending.channel); catch; end
                end
                empty!(client.pending_requests)
            end
        end
    end

    result = take!(ready)
    if result !== true
        throw(result isa Exception ? result : ErrorException("Failed to connect to $(client.url)"))
    end

    return client
end

function RepeRPC.disconnect(client::_WSClient)
    ws_to_close = nothing
    lock(client.state_lock) do
        if !client.connected
            return
        end
        client.connected = false
        ws_to_close = client.ws
        client.ws = nothing
    end

    if ws_to_close !== nothing
        try; close(ws_to_close); catch; end
    end

    # May race with the listener task's finally block — see comment there.
    lock(client.requests_lock) do
        for pending in values(client.pending_requests)
            try; close(pending.channel); catch; end
        end
        empty!(client.pending_requests)
    end

    if client.listener_task !== nothing && !istaskdone(client.listener_task)
        try; wait(client.listener_task); catch; end
    end
    client.listener_task = nothing
end

function RepeRPC.isconnected(client::_WSClient)
    return client.connected
end

function _ws_get_next_id(client::_WSClient)::UInt64
    return Threads.atomic_add!(client.next_id, UInt64(1))
end

function _ws_send_message(client::_WSClient, msg::RepeRPC.Message)
    if !client.connected || client.ws === nothing
        throw(ErrorException("Client not connected"))
    end

    data = RepeRPC.serialize_message(msg)
    lock(client.write_lock) do
        HTTP.WebSockets.send(client.ws, data)
    end
end

function _ws_response_loop(client::_WSClient, ws)
    try
        for data in ws
            connected = lock(client.state_lock) do
                client.connected
            end
            !connected && break

            msg_bytes = data isa Vector{UInt8} ? data : Vector{UInt8}(data)

            # Reject an oversized response before deserialize_message copies its
            # (possibly huge) body out of msg_bytes and before parse_body decodes
            # it: parse only the 48-byte header and check the wire-declared
            # lengths against max_body_length. This is a POST-RECEIVE ceiling --
            # HTTP.jl's `receive` has already buffered the whole frame in memory
            # (it exposes no per-frame read), so unlike the TCP client, which
            # checks before reading the body off the socket, it cannot prevent
            # that initial buffering; it bounds what REPE parses and delivers. A
            # WebSocket frame is self-delimiting, so one oversize response does
            # not desync the connection: deliver the error to its waiter (if any)
            # and keep the loop running for the other pending requests.
            if length(msg_bytes) >= RepeRPC.HEADER_SIZE
                header = RepeRPC.deserialize_header(msg_bytes[1:RepeRPC.HEADER_SIZE])
                if !RepeRPC._within_length_limit(client.max_body_length, header.query_length) ||
                   !RepeRPC._within_length_limit(client.max_body_length, header.body_length)
                    RepeRPC._deliver(client, header.id,
                        RepeRPC._oversize_response_error(header, client.max_body_length))
                    continue
                end
            end

            msg = RepeRPC.deserialize_message(msg_bytes)

            # Copy the entry out and release the lock before decoding, exactly as
            # the TCP reader does: `parse_body` on a large body would otherwise hold
            # `requests_lock` for its whole duration, stalling registration and
            # timeout firing for every other in-flight request on this connection.
            # `RepeRPC._deliver` re-looks-up and pops under the lock, and tolerates the
            # entry having gone away while we were decoding.
            pending = lock(client.requests_lock) do
                get(client.pending_requests, msg.header.id, nothing)
            end
            pending === nothing && continue

            # `_response_value`'s `copy = false` decode is safe here for the same
            # reason it is on TCP: `deserialize_message` has already copied the body
            # out of HTTP.jl's frame buffer, so nothing aliases a buffer HTTP.jl reuses.
            RepeRPC._deliver(client, msg.header.id, RepeRPC._response_value(msg, pending))
        end
    catch e
        connected = lock(client.state_lock) do
            client.connected
        end
        if connected
            @warn "WebSocket response handler error" exception=e
        end
    end
end

function RepeRPC.send_notify(client::_WSClient, method::String, params=nothing;
                          query_format::RepeRPC.QueryFormat=RepeRPC.QUERY_JSON_POINTER,
                          body_format::RepeRPC.BodyFormat=RepeRPC.BODY_JSON)
    body_bytes = params === nothing ? UInt8[] : RepeRPC.encode_body(params, body_format)

    msg = RepeRPC.Message(
        id=_ws_get_next_id(client),
        query=method,
        body=body_bytes,
        query_format=UInt16(query_format),
        body_format=UInt16(body_format),
        notify=true
    )

    _ws_send_message(client, msg)
end

function _ws_send_request_sync(client::_WSClient, method::String, params=nothing;
                               query_format::RepeRPC.QueryFormat=RepeRPC.QUERY_JSON_POINTER,
                               body_format::RepeRPC.BodyFormat=RepeRPC.BODY_JSON,
                               timeout::Union{Float64,Nothing}=nothing,
                               result_type::Union{Nothing,Type}=nothing)
    if !client.connected
        RepeRPC.connect(client)
    end

    request_id = _ws_get_next_id(client)
    response_channel = Channel(1)

    body_bytes = params === nothing ? UInt8[] : RepeRPC.encode_body(params, body_format)

    msg = RepeRPC.Message(
        id=request_id,
        query=method,
        body=body_bytes,
        query_format=UInt16(query_format),
        body_format=UInt16(body_format),
        notify=false
    )

    RepeRPC._register_and_send(client, request_id,
                            RepeRPC.PendingRequest(response_channel, result_type),
                            () -> _ws_send_message(client, msg))

    # Blocking wait shared with the TCP client: it wakes the instant the listener's
    # teardown closes this channel, so a peer close is reported as a
    # `ConnectionError` rather than sitting out the full timeout.
    return RepeRPC._await_response(client, request_id, response_channel,
                                something(timeout, client.timeout))
end

function RepeRPC.send_request(client::_WSClient, method::String, params=nothing;
                           query_format::RepeRPC.QueryFormat=RepeRPC.QUERY_JSON_POINTER,
                           body_format::RepeRPC.BodyFormat=RepeRPC.BODY_JSON,
                           timeout::Union{Float64,Nothing}=nothing,
                           result_type::Union{Nothing,Type}=nothing)
    return _ws_send_request_sync(client, method, params;
                                 query_format=query_format,
                                 body_format=body_format,
                                 timeout=timeout,
                                 result_type=result_type)
end

function RepeRPC.send_request(::Type{T}, client::_WSClient, method::String, params=nothing;
                           query_format::RepeRPC.QueryFormat=RepeRPC.QUERY_JSON_POINTER,
                           body_format::RepeRPC.BodyFormat=RepeRPC.BODY_JSON,
                           timeout::Union{Float64,Nothing}=nothing) where {T}
    return _ws_send_request_sync(client, method, params;
                                 query_format=query_format,
                                 body_format=body_format,
                                 timeout=timeout,
                                 result_type=T)
end

function RepeRPC.send_request_async(client::_WSClient, method::String, params=nothing;
                                 query_format::RepeRPC.QueryFormat=RepeRPC.QUERY_JSON_POINTER,
                                 body_format::RepeRPC.BodyFormat=RepeRPC.BODY_JSON,
                                 timeout::Union{Float64,Nothing}=nothing,
                                 result_type::Union{Nothing,Type}=nothing)
    return @async begin
        _ws_send_request_sync(client, method, params;
                              query_format=query_format,
                              body_format=body_format,
                              timeout=timeout,
                              result_type=result_type)
    end
end

function RepeRPC.send_request_async(::Type{T}, client::_WSClient, method::String, params=nothing;
                                 query_format::RepeRPC.QueryFormat=RepeRPC.QUERY_JSON_POINTER,
                                 body_format::RepeRPC.BodyFormat=RepeRPC.BODY_JSON,
                                 timeout::Union{Float64,Nothing}=nothing) where {T}
    return RepeRPC.send_request_async(client, method, params;
                                   query_format=query_format,
                                   body_format=body_format,
                                   timeout=timeout,
                                   result_type=T)
end

function RepeRPC.batch(client::_WSClient, requests::AbstractVector{<:Tuple{String, Any}}; kwargs...)
    tasks = Task[]
    for (method, params) in requests
        task = RepeRPC.send_request_async(client, method, params; kwargs...)
        push!(tasks, task)
    end
    return tasks
end

function Base.show(io::IO, client::_WSClient)
    status = client.connected ? "connected" : "disconnected"
    auth = client.auth_token !== nothing ? ", auth=token" : ""
    print(io, "WSClient(\"$(client.url)\", $status$auth)")
end

end # module RepeRPCHTTPExt
