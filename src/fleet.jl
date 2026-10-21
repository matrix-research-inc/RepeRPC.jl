# Fleet API - Multi-server control for RepeRPC.jl
# Provides unified interface for managing and communicating with multiple REPE servers

using Base.Threads: ReentrantLock

#==============================================================================#
# Types
#==============================================================================#

"""
    NodeConfig

Configuration for a remote REPE server node. The `name` defaults to `host` for simplicity.

# TCP transport (default)
```julia
NodeConfig("compute-1.local", 8080)
NodeConfig("compute-1.local", 8080; tags=["compute", "gpu"])
NodeConfig("192.168.1.10", 8080; name="compute-1")
```

# WebSocket transport
Pass a `ws://` or `wss://` URL to use the WebSocket client (requires
`using HTTP`). For secure deployments combine `wss://` with `auth_token` to
require a bearer token on the upgrade request.

```julia
NodeConfig("wss://data-1.example.com/repe"; auth_token=ENV["REPE_TOKEN"])
NodeConfig("ws://10.0.0.5:8080")
```

# Resource limits
- `max_body_length`: cap on an accepted response's `query_length`/`body_length`,
  forwarded to the node's transport client. Default `nothing` (unlimited). On a
  TCP node it is a pre-allocation bound (checked before the body is read off the
  socket). On a WebSocket node HTTP.jl has already buffered the whole frame by
  the time REPE sees the header, so it is a post-receive ceiling: it still
  refuses to parse or deliver an oversized body (and bounds the materialized
  [`stream`](@ref)/[`download`](@ref) fallback), but cannot prevent that initial
  frame buffering. Use a TCP node when a hard memory bound matters.

# Auth and TLS keywords
- `auth_token`: shared secret sent as `Authorization: Bearer <token>` on the
  WebSocket upgrade request. Ignored for TCP transport.
- `require_ssl_verification`: when `false`, the client accepts self-signed
  certificates. Defaults to `true`. Only meaningful for `wss://` URLs.
- `tls_config`: optional `Reseau.TLS.Config` for custom CA trust stores or
  client certificates. Requires a `wss://` URL.
- `max_frame_size`: largest inbound WebSocket frame, in bytes. Default
  `nothing`, which keeps the WebSocket client's 16 MiB default. Requires a
  `ws://` or `wss://` URL.
"""
struct NodeConfig
    name::String
    host::String
    port::Int
    url::Union{Nothing,String}
    auth_token::Union{Nothing,String}
    require_ssl_verification::Bool
    tls_config::Any
    tags::Vector{String}
    timeout::Float64
    max_body_length::Union{Nothing,Int}
    max_frame_size::Union{Nothing,Int}

    function NodeConfig(host::String, port::Int;
                        name::String = host,
                        url::Union{Nothing,AbstractString} = nothing,
                        auth_token::Union{Nothing,AbstractString} = nothing,
                        require_ssl_verification::Bool = true,
                        tls_config = nothing,
                        tags::Vector{String} = String[],
                        timeout::Float64 = 30.0,
                        max_body_length::Union{Nothing,Integer} = nothing,
                        max_frame_size::Union{Nothing,Integer} = nothing)
        (port < 1 || port > 65535) && throw(ArgumentError("port must be between 1 and 65535"))
        timeout <= 0 && throw(ArgumentError("timeout must be positive"))
        max_body_length !== nothing && max_body_length < 0 &&
            throw(ArgumentError("max_body_length must be non-negative or nothing, got $max_body_length"))
        max_frame_size !== nothing && max_frame_size <= 0 &&
            throw(ArgumentError("max_frame_size must be positive or nothing, got $max_frame_size"))

        # An unset env var collapsing to "" would otherwise authenticate any
        # client sending `Authorization: Bearer ` against a server configured
        # with the same empty token. Reject at construction so the
        # misconfiguration cannot reach the wire.
        auth_token !== nothing && isempty(auth_token) &&
            throw(ArgumentError("auth_token must be non-empty; pass nothing to disable authentication"))

        # Security-relevant keywords only have meaning for WebSocket
        # transport. A caller who supplies them without a ws:// or wss:// URL
        # is almost certainly misconfigured: copy-pasting a wss example,
        # then switching the host/port and forgetting the scheme should
        # error loudly rather than silently drop auth.
        # The WebSocket client reads verification from a TLS config, so a
        # per-node override alongside one would be silently contradictory.
        tls_config !== nothing && !require_ssl_verification &&
            throw(ArgumentError("require_ssl_verification = false cannot be combined with tls_config; disable verification on the config instead"))

        if url === nothing
            auth_token !== nothing &&
                throw(ArgumentError("auth_token requires a ws:// or wss:// URL; TCP transport does not authenticate at the REPE layer"))
            tls_config !== nothing &&
                throw(ArgumentError("tls_config requires a wss:// URL"))
            max_frame_size !== nothing &&
                throw(ArgumentError("max_frame_size requires a ws:// or wss:// URL"))
            require_ssl_verification ||
                throw(ArgumentError("require_ssl_verification = false has no effect for TCP transport; use a wss:// URL"))
        end

        new(name, host, port,
            url === nothing ? nothing : String(url),
            auth_token === nothing ? nothing : String(auth_token),
            require_ssl_verification,
            tls_config,
            tags, timeout,
            max_body_length === nothing ? nothing : Int(max_body_length),
            max_frame_size === nothing ? nothing : Int(max_frame_size))
    end
end

"""
    NodeConfig(url::AbstractString; kwargs...) -> NodeConfig

URL-style constructor for WebSocket nodes. Accepts `ws://` or `wss://` URLs.
Host and port are parsed from the URL for display purposes; the URL itself is
authoritative for connection. Default port is 443 for `wss://` and 80 for
`ws://` when not specified in the URL.

# Example
```julia
NodeConfig("wss://data-1.example.com/repe";
           auth_token = ENV["REPE_TOKEN"],
           tags       = ["team"])
```
"""
function NodeConfig(url::AbstractString;
                    name::Union{Nothing,AbstractString} = nothing,
                    auth_token::Union{Nothing,AbstractString} = nothing,
                    require_ssl_verification::Bool = true,
                    tls_config = nothing,
                    tags::Vector{String} = String[],
                    timeout::Float64 = 30.0,
                    max_body_length::Union{Nothing,Integer} = nothing,
                    max_frame_size::Union{Nothing,Integer} = nothing)
    parsed = _parse_ws_url(url)
    return NodeConfig(parsed.host, parsed.port;
        name = name === nothing ? String(url) : String(name),
        url = String(url),
        auth_token = auth_token,
        require_ssl_verification = require_ssl_verification,
        tls_config = tls_config,
        tags = tags,
        timeout = timeout,
        max_body_length = max_body_length,
        max_frame_size = max_frame_size)
end

"""
    _parse_ws_url(url) -> (; host, port, scheme)

Pull host and port out of a `ws://` / `wss://` URL for display and validation.
Defaults port to 443 for `wss` and 80 for `ws` when absent.
"""
function _parse_ws_url(url::AbstractString)
    scheme = ""
    rest = ""
    if startswith(url, "wss://")
        scheme = "wss"
        rest = SubString(url, 7)
    elseif startswith(url, "ws://")
        scheme = "ws"
        rest = SubString(url, 6)
    else
        throw(ArgumentError("WebSocket URL must start with ws:// or wss://, got: $url"))
    end
    path_idx = findfirst(c -> c == '/' || c == '?' || c == '#', rest)
    hostport = path_idx === nothing ? rest : SubString(rest, 1, path_idx - 1)
    isempty(hostport) && throw(ArgumentError("WebSocket URL missing host: $url"))
    # IPv6 literal URLs use bracketed hosts (e.g. wss://[::1]:8443). The
    # naive split-on-colon used below would corrupt the host string. Since
    # the LAN-fleet use case rarely needs v6 literals, reject explicitly
    # rather than silently mis-parse.
    startswith(hostport, '[') &&
        throw(ArgumentError("IPv6 literal URLs (bracketed host) are not supported; use a hostname or IPv4 literal: $url"))
    if occursin(':', hostport)
        parts = split(hostport, ':'; limit = 2)
        host = String(parts[1])
        port = try
            parse(Int, parts[2])
        catch
            throw(ArgumentError("WebSocket URL has invalid port: $url"))
        end
    else
        host = String(hostport)
        port = scheme == "wss" ? 443 : 80
    end
    return (host = host, port = port, scheme = scheme)
end

"""
    Node

Internal representation of a connected server. Created automatically from `NodeConfig`.

The underlying `client` is typed as [`AbstractClient`](@ref) so a Fleet can hold
a mix of TCP `Client` and WebSocket `WSClient` nodes. The concrete type is
selected by [`_make_client`](@ref) based on whether `NodeConfig.url` is set.
"""
mutable struct Node
    name::String
    host::String
    port::Int
    tags::Set{String}
    timeout::Float64
    client::AbstractClient

    function Node(config::NodeConfig)
        client = _make_client(config)
        new(config.name, config.host, config.port, Set(config.tags), config.timeout, client)
    end
end

"""
    _make_client(config::NodeConfig) -> AbstractClient

Build the transport client for a node. Returns a TCP [`Client`](@ref) when
`config.url` is `nothing`, otherwise a `WSClient` over WebSocket (requires the
`RepeRPCHTTPExt` extension; `using HTTP` to load it). `timeout` and
`max_body_length` are forwarded to both transports; the auth and TLS keywords
are forwarded to the WebSocket client and silently ignored for TCP.
"""
function _make_client(config::NodeConfig)
    if config.url === nothing
        return Client(config.host, config.port;
            timeout = config.timeout,
            max_body_length = config.max_body_length)
    end
    return WSClient(config.url;
        timeout = config.timeout,
        max_body_length = config.max_body_length,
        auth_token = config.auth_token,
        require_ssl_verification = config.require_ssl_verification,
        tls_config = config.tls_config,
        max_frame_size = something(config.max_frame_size, _WS_DEFAULT_MAX_FRAME_SIZE))
end

"""
    RemoteResult{T}

Structured response from remote calls.

# Fields
- `node::String` - Node that processed the request
- `value::Union{T, Nothing}` - Result value (nothing on failure)
- `error::Union{Exception, Nothing}` - Exception (nothing on success)
- `elapsed::Float64` - Time taken in seconds
"""
struct RemoteResult{T}
    node::String
    value::Union{T, Nothing}
    error::Union{Exception, Nothing}
    elapsed::Float64
end

"""
    succeeded(r::RemoteResult) -> Bool

Check if the remote call succeeded (no error).
"""
succeeded(r::RemoteResult) = r.error === nothing

"""
    failed(r::RemoteResult) -> Bool

Check if the remote call failed (has error). A caller-cancelled SVS pull also
reports `failed(r) == true`; use [`cancelled`](@ref) to tell a caller-initiated
abort apart from a genuine node fault (`failed(r) && !cancelled(r)`).
"""
failed(r::RemoteResult) = r.error !== nothing

"""
    cancelled(r::RemoteResult) -> Bool

Check whether an SVS pull was aborted by the caller's `cancel` predicate (its error
is a [`CancelledError`](@ref)). Because a cancellation is caller-initiated rather
than a producer fault, a cancelled result also reports [`failed`](@ref)`(r) == true`;
to find only genuine node faults, filter with `failed(r) && !cancelled(r)`. This
keeps deliberate cancellations from being mistaken for producer faults.
"""
cancelled(r::RemoteResult) = r.error isa CancelledError

"""
    Base.getindex(r::RemoteResult)

Get the value from a RemoteResult, or throw the error if it failed.
"""
function Base.getindex(r::RemoteResult)
    r.error !== nothing && throw(r.error)
    return r.value
end

"""
    HealthStatus

Result of a health check for a single node.
"""
const HealthStatus = NamedTuple{(:healthy, :latency, :error), Tuple{Bool, Float64, Union{Exception, Nothing}}}

"""
    Fleet

The main interface for managing multiple REPE server nodes.

# Example
```julia
config = [
    NodeConfig("compute-1.local", 8080; tags=["compute"]),
    NodeConfig("compute-2.local", 8080; tags=["compute"]),
    NodeConfig("storage.local", 8080; tags=["storage"]),
]

fleet = Fleet(config)
connect!(fleet)

# Broadcast to all nodes
results = broadcast(fleet, "/status")

# Filter by tag
results = broadcast(fleet, "/compute", Dict("value" => 42); tags=["compute"])

disconnect!(fleet)
```
"""
mutable struct Fleet
    nodes::Dict{String, Node}
    nodes_lock::ReentrantLock
    default_timeout::Float64
    retry_policy::NamedTuple{(:max_attempts, :delay), Tuple{Int, Float64}}

    function Fleet(configs::Vector{NodeConfig};
                   timeout::Float64 = 30.0,
                   max_retry_attempts::Int = 3,
                   retry_delay::Float64 = 1.0)
        timeout <= 0 && throw(ArgumentError("timeout must be positive"))
        max_retry_attempts < 1 && throw(ArgumentError("max_retry_attempts must be at least 1"))
        retry_delay < 0 && throw(ArgumentError("retry_delay must be non-negative"))

        # Check for duplicate names BEFORE creating any nodes
        # (to provide clear error messages before network errors)
        seen_names = Set{String}()
        for config in configs
            if config.name in seen_names
                throw(ArgumentError("Duplicate node name: \"$(config.name)\". Multiple servers on the same host require explicit unique names."))
            end
            push!(seen_names, config.name)
        end

        # Now create nodes
        nodes = Dict{String, Node}()
        for config in configs
            nodes[config.name] = Node(config)
        end

        new(nodes, ReentrantLock(), timeout, (max_attempts=max_retry_attempts, delay=retry_delay))
    end

    function Fleet(nodes_vec::Vector{Node};
                   timeout::Float64 = 30.0,
                   max_retry_attempts::Int = 3,
                   retry_delay::Float64 = 1.0)
        timeout <= 0 && throw(ArgumentError("timeout must be positive"))
        max_retry_attempts < 1 && throw(ArgumentError("max_retry_attempts must be at least 1"))
        retry_delay < 0 && throw(ArgumentError("retry_delay must be non-negative"))

        # Check for duplicate names first
        seen_names = Set{String}()
        for node in nodes_vec
            if node.name in seen_names
                throw(ArgumentError("Duplicate node name: \"$(node.name)\". Multiple servers on the same host require explicit unique names."))
            end
            push!(seen_names, node.name)
        end

        # Add nodes to dict
        nodes = Dict{String, Node}()
        for node in nodes_vec
            nodes[node.name] = node
        end

        new(nodes, ReentrantLock(), timeout, (max_attempts=max_retry_attempts, delay=retry_delay))
    end
end

# Empty fleet constructor
Fleet() = Fleet(NodeConfig[])

#==============================================================================#
# Connection Management
#==============================================================================#

"""
    connect!(fleet::Fleet) -> (connected=Vector{String}, failed=Vector{String})

Connect to all nodes in the fleet. Returns a named tuple with lists of
successfully connected and failed node names.
"""
function connect!(fleet::Fleet)
    node_list = lock(fleet.nodes_lock) do
        collect(values(fleet.nodes))
    end

    connected = String[]
    failed = String[]

    # Connect in parallel using tasks
    tasks = Dict{String, Task}()
    for node in node_list
        tasks[node.name] = @async begin
            try
                if !isconnected(node.client)
                    connect(node.client)
                end
                return true
            catch e
                return e
            end
        end
    end

    # Collect results
    for (name, task) in tasks
        result = fetch(task)
        if result === true
            push!(connected, name)
        else
            push!(failed, name)
        end
    end

    return (connected=connected, failed=failed)
end

"""
    disconnect!(fleet::Fleet) -> (disconnected=Vector{String}, failed=Vector{String})

Disconnect from all nodes in the fleet. Returns a named tuple with lists of
successfully disconnected and failed node names.
"""
function disconnect!(fleet::Fleet)
    node_list = lock(fleet.nodes_lock) do
        collect(values(fleet.nodes))
    end

    disconnected = String[]
    failed = String[]

    for node in node_list
        try
            if isconnected(node.client)
                disconnect(node.client)
            end
            push!(disconnected, node.name)
        catch e
            push!(failed, node.name)
        end
    end

    return (disconnected=disconnected, failed=failed)
end

"""
    reconnect!(fleet::Fleet) -> (reconnected=Vector{String}, failed=Vector{String})

Reconnect disconnected nodes. Returns a named tuple with lists of
successfully reconnected and failed node names.
"""
function reconnect!(fleet::Fleet)
    node_list = lock(fleet.nodes_lock) do
        collect(values(fleet.nodes))
    end

    reconnected = String[]
    failed = String[]

    # Reconnect in parallel
    tasks = Dict{String, Task}()
    for node in node_list
        if !isconnected(node.client)
            tasks[node.name] = @async begin
                try
                    # Disconnect first to clean up any stale state
                    try
                        disconnect(node.client)
                    catch
                    end
                    connect(node.client)
                    return true
                catch e
                    return e
                end
            end
        end
    end

    # Collect results
    for (name, task) in tasks
        result = fetch(task)
        if result === true
            push!(reconnected, name)
        else
            push!(failed, name)
        end
    end

    return (reconnected=reconnected, failed=failed)
end

"""
    isconnected(fleet::Fleet) -> Bool

Check if all nodes in the fleet are connected.
"""
function isconnected(fleet::Fleet)
    lock(fleet.nodes_lock) do
        for node in values(fleet.nodes)
            if !isconnected(node.client)
                return false
            end
        end
        return true
    end
end

"""
    isconnected(fleet::Fleet, name::String) -> Bool

Check if a specific node is connected.
"""
function isconnected(fleet::Fleet, name::String)
    lock(fleet.nodes_lock) do
        if !haskey(fleet.nodes, name)
            throw(KeyError(name))
        end
        return isconnected(fleet.nodes[name].client)
    end
end

"""
    isconnected(node::Node) -> Bool

Check if a node is connected.
"""
isconnected(node::Node) = isconnected(node.client)

#==============================================================================#
# Node Access
#==============================================================================#

"""
    nodes(fleet::Fleet) -> Vector{Node}

Get all nodes in the fleet.
"""
function nodes(fleet::Fleet)
    lock(fleet.nodes_lock) do
        collect(values(fleet.nodes))
    end
end

"""
    connected_nodes(fleet::Fleet) -> Vector{Node}

Get all connected nodes in the fleet.
"""
function connected_nodes(fleet::Fleet)
    lock(fleet.nodes_lock) do
        [node for node in values(fleet.nodes) if isconnected(node.client)]
    end
end

"""
    filter_nodes(fleet::Fleet; tags::Vector{String}) -> Vector{Node}

Get nodes matching all specified tags.
"""
function filter_nodes(fleet::Fleet; tags::Vector{String})
    tag_set = Set(tags)
    lock(fleet.nodes_lock) do
        [node for node in values(fleet.nodes) if tag_set ⊆ node.tags]
    end
end

"""
    Base.getindex(fleet::Fleet, name::String) -> Node

Get a node by name.
"""
function Base.getindex(fleet::Fleet, name::String)
    lock(fleet.nodes_lock) do
        if !haskey(fleet.nodes, name)
            throw(KeyError(name))
        end
        return fleet.nodes[name]
    end
end

"""
    Base.length(fleet::Fleet) -> Int

Get the number of nodes in the fleet.
"""
function Base.length(fleet::Fleet)
    lock(fleet.nodes_lock) do
        length(fleet.nodes)
    end
end

"""
    Base.keys(fleet::Fleet) -> Vector{String}

Get all node names in the fleet.
"""
function Base.keys(fleet::Fleet)
    lock(fleet.nodes_lock) do
        collect(keys(fleet.nodes))
    end
end

#==============================================================================#
# Dynamic Node Management
#==============================================================================#

"""
    add_node!(fleet::Fleet, config::NodeConfig) -> Fleet

Add a new node to the fleet. Does not automatically connect.
Throws `ArgumentError` if a node with the same name already exists.
"""
function add_node!(fleet::Fleet, config::NodeConfig)
    lock(fleet.nodes_lock) do
        if haskey(fleet.nodes, config.name)
            throw(ArgumentError("Node \"$(config.name)\" already exists in fleet"))
        end
        fleet.nodes[config.name] = Node(config)
    end
    return fleet
end

"""
    remove_node!(fleet::Fleet, name::String) -> Fleet

Remove a node from the fleet. Disconnects the node before removal.
"""
function remove_node!(fleet::Fleet, name::String)
    lock(fleet.nodes_lock) do
        if haskey(fleet.nodes, name)
            node = fleet.nodes[name]
            try
                disconnect(node.client)
            catch
                # Ignore disconnect errors during removal
            end
            delete!(fleet.nodes, name)
        end
    end
    return fleet
end

#==============================================================================#
# Remote Invocation
#==============================================================================#

"""
    call(fleet::Fleet, node_name::String, method::String, params=nothing; kwargs...) -> RemoteResult

Call a method on a specific node.

An unknown or removed `node_name` is captured as a failed `RemoteResult` (a
`KeyError`) rather than thrown, consistent with any other per-node failure;
`r[]` rethrows it if you want the miss loud (see [`_fleet_lookup`](@ref)).

# Arguments
- `fleet`: The fleet
- `node_name`: Name of the target node
- `method`: The RPC method name
- `params`: Optional parameters

# Keyword Arguments
- `query_format`: Format for the method name (default: QUERY_JSON_POINTER)
- `body_format`: Format for serializing params (default: BODY_JSON)
"""
function call(fleet::Fleet, node_name::String, method::String, params=nothing;
              query_format::QueryFormat = QUERY_JSON_POINTER,
              body_format::BodyFormat = BODY_JSON)

    node = _fleet_lookup(Any, fleet, node_name)
    node isa RemoteResult && return node

    _call_with_retry(fleet, node, method, params; query_format=query_format, body_format=body_format)
end

"""
    _is_retryable_error(e) -> Bool

Decide whether a fleet RPC failure justifies another attempt against the same
node.

Retry is opt-in via an explicit allowlist of transport-level exceptions, so
that any future or unexpected exception type defaults to *not* retried. This
protects stateful RPCs (e.g., a hardware capture that mutates state) from
being triggered twice because some new code path threw a class the predicate
hadn't been taught about.

Allowed (retried):
- [`ConnectionError`](@ref): REPE's wrapper around connect-phase failures
  (e.g., refused connection, host unreachable). Strictly before-send.
- `Base.IOError`: raw socket errors that bypass `ConnectionError`, typically
  during `connect()` or the initial `write()`.
- `EOFError`: peer closed the stream. Most often the server crashed before
  the request landed; technically ambiguous if it happens mid-read after the
  server processed the request, so callers with strict idempotency concerns
  should set `max_retry_attempts = 1` or treat the endpoint as one-shot.

Everything else is treated as non-retryable. In particular this includes
[`TimeoutError`](@ref) (response was slow; the server may have already acted
on the request) and [`REPEError`](@ref) (the server returned a structured
error response, so the request reached and was processed by the server).

This predicate is internal. A future per-call `idempotent` keyword on
`call`/`broadcast` would be the right place to widen the allowlist on a
case-by-case basis without mutating this default.
"""
function _is_retryable_error(e)
    e isa ConnectionError && return true
    e isa Base.IOError && return true
    e isa EOFError && return true
    return false
end

"""
Internal function to call a node with retry logic.
"""
function _call_with_retry(fleet::Fleet, node::Node, method::String, params;
                          query_format::QueryFormat = QUERY_JSON_POINTER,
                          body_format::BodyFormat = BODY_JSON)
    start_time = time()
    last_error = nothing

    for attempt in 1:fleet.retry_policy.max_attempts
        try
            # Ensure connected
            if !isconnected(node.client)
                connect(node.client)
            end

            # Determine timeout: node-specific > fleet default
            timeout = node.timeout > 0 ? node.timeout : fleet.default_timeout

            result = send_request(node.client, method, params;
                                  query_format=query_format,
                                  body_format=body_format,
                                  timeout=timeout)

            elapsed = time() - start_time
            return RemoteResult{Any}(node.name, result, nothing, elapsed)

        catch e
            last_error = e
            # Stop early when retrying could double-trigger a side effect or
            # when the same call would deterministically fail again.
            _is_retryable_error(e) || break
            if attempt < fleet.retry_policy.max_attempts
                sleep(fleet.retry_policy.delay)
            end
        end
    end

    elapsed = time() - start_time
    return RemoteResult{Any}(node.name, nothing, last_error, elapsed)
end

"""
    broadcast(fleet::Fleet, method::String, params=nothing; tags=String[], kwargs...) -> Dict{String, RemoteResult}

Call a method on all matching nodes in parallel.

# Arguments
- `fleet`: The fleet
- `method`: The RPC method name
- `params`: Optional parameters (same params sent to all nodes)

# Keyword Arguments
- `tags`: Filter nodes by tags (empty means all nodes)
- `query_format`: Format for the method name (default: QUERY_JSON_POINTER)
- `body_format`: Format for serializing params (default: BODY_JSON)
"""
function broadcast(fleet::Fleet, method::String, params=nothing;
                   tags::Vector{String} = String[],
                   query_format::QueryFormat = QUERY_JSON_POINTER,
                   body_format::BodyFormat = BODY_JSON)

    # Snapshot nodes under lock
    target_nodes = lock(fleet.nodes_lock) do
        if isempty(tags)
            collect(values(fleet.nodes))
        else
            tag_set = Set(tags)
            [node for node in values(fleet.nodes) if tag_set ⊆ node.tags]
        end
    end

    # Execute in parallel
    results = Dict{String, RemoteResult}()
    tasks = Dict{String, Task}()

    for node in target_nodes
        tasks[node.name] = @async _call_with_retry(fleet, node, method, params;
                                                    query_format=query_format,
                                                    body_format=body_format)
    end

    # Collect results
    for (name, task) in tasks
        results[name] = fetch(task)
    end

    return results
end

"""
    map_reduce(reduce_fn, fleet::Fleet, method::String, params=nothing; tags=String[], kwargs...) -> Any

Broadcast a method call and reduce the results.

# Arguments
- `reduce_fn`: Function to reduce results, receives Vector{RemoteResult}
- `fleet`: The fleet
- `method`: The RPC method name
- `params`: Optional parameters

# Keyword Arguments
- `tags`: Filter nodes by tags (empty means all nodes)
- `query_format`: Format for the method name
- `body_format`: Format for serializing params

# Example
```julia
total = map_reduce(fleet, "/compute", Dict("value" => 10); tags=["compute"]) do results
    sum(r.value["result"] for r in results if succeeded(r))
end
```
"""
function map_reduce(reduce_fn::Function, fleet::Fleet, method::String, params=nothing;
                    tags::Vector{String} = String[],
                    query_format::QueryFormat = QUERY_JSON_POINTER,
                    body_format::BodyFormat = BODY_JSON)

    results_dict = broadcast(fleet, method, params;
                             tags=tags,
                             query_format=query_format,
                             body_format=body_format)

    results_vec = collect(values(results_dict))
    return reduce_fn(results_vec)
end

#==============================================================================#
# Streaming
#==============================================================================#

"""
    _stream_response_dispatch(client, method, params, sink; kwargs...) -> StreamedResponse

Transport dispatch for a streamed fleet transfer. A TCP [`Client`](@ref) uses the
chunked [`stream_response`](@ref) path (peak memory bounded by `buffer_size`). Any
other `AbstractClient` (e.g. a WebSocket client, which has no chunked receive
path -- HTTP.jl reassembles each frame whole) falls back to a fully materialized
[`send_request`](@ref) whose result is written into `sink`; the memory bound then
does not hold, only raw-binary or string bodies can be written this way, and a
response larger than the node's `max_body_length` is refused before delivery.
"""
function _stream_response_dispatch(client::Client, method::String, params, sink::IO;
                                   query_format::QueryFormat,
                                   body_format::BodyFormat,
                                   buffer_size::Union{Nothing,Integer},
                                   timeout::Union{Float64,Nothing})
    bs = buffer_size === nothing ? client.buffer_size : Int(buffer_size)
    return stream_response(client, method, params;
                           into = sink,
                           query_format = query_format,
                           body_format = body_format,
                           timeout = timeout,
                           buffer_size = bs)
end

function _stream_response_dispatch(client::AbstractClient, method::String, params, sink::IO;
                                   query_format::QueryFormat,
                                   body_format::BodyFormat,
                                   buffer_size::Union{Nothing,Integer},
                                   timeout::Union{Float64,Nothing})
    # Materialized fallback for transports without a chunked path (WebSocket).
    result = send_request(client, method, params;
                          query_format = query_format,
                          body_format = body_format,
                          timeout = timeout)
    bytes, fmt = _write_materialized_to_sink(sink, result)
    return StreamedResponse(bytes, fmt)
end

"""
    _write_materialized_to_sink(sink::IO, result) -> (bytes::Int, body_format::BodyFormat)

Write a materialized response body into `sink` for the WebSocket streaming
fallback. An empty body (`parse_body` returns `nothing`) writes nothing, matching
the TCP path which streams 0 bytes for an empty response. Raw-binary
(`Vector{UInt8}`) and string bodies are written as-is. A structured body
(Dict/array from JSON/BEVE) cannot be written byte-for-byte and raises, directing
the caller to use a TCP node for structured streamed bodies.
"""
function _write_materialized_to_sink(sink::IO, result)
    if result === nothing
        # Empty (zero-length) body. The TCP path streams 0 bytes without error,
        # so the fallback must agree rather than reject a valid empty response.
        # The format tag is moot for 0 bytes; report raw-binary.
        return 0, BODY_RAW_BINARY
    elseif result isa AbstractVector{UInt8}
        write(sink, result)
        return length(result), BODY_RAW_BINARY
    elseif result isa AbstractString
        n = write(sink, result)
        return n, BODY_UTF8
    else
        throw(ArgumentError(
            "Cannot stream a non-binary response body over a WebSocket node " *
            "(got $(typeof(result))); the materialized fallback only handles " *
            "raw-binary or string bodies. Route structured streamed transfers " *
            "to a TCP node."))
    end
end

# Single-attempt (no-retry) streamed transfer into `sink`, wrapped in a
# RemoteResult. Streamed calls are deliberately NOT routed through
# `_call_with_retry`: a transfer that has already written partial bytes into the
# sink must not be silently retried on top of them. Errors are captured into the
# RemoteResult, consistent with `call`.
function _fleet_stream(node::Node, method::String, params, sink::IO;
                       query_format::QueryFormat,
                       body_format::BodyFormat,
                       buffer_size::Union{Nothing,Integer},
                       timeout::Union{Float64,Nothing})
    start_time = time()
    try
        if !isconnected(node.client)
            connect(node.client)
        end
        result = _stream_response_dispatch(node.client, method, params, sink;
                                           query_format = query_format,
                                           body_format = body_format,
                                           buffer_size = buffer_size,
                                           timeout = timeout)
        return RemoteResult{StreamedResponse}(node.name, result, nothing, time() - start_time)
    catch e
        return RemoteResult{StreamedResponse}(node.name, nothing, e, time() - start_time)
    end
end

"""
    _fleet_lookup(::Type{R}, fleet::Fleet, node_name) -> Union{Node, RemoteResult{R}}

Resolve `node_name` to its [`Node`](@ref), or, on a miss, a **failed**
`RemoteResult{R}` carrying the `KeyError`. Every fleet entry point that returns a
`RemoteResult` ([`call`](@ref), [`stream`](@ref), [`download`](@ref), and the SVS
`pull_*` wrappers) resolves through this, so an unknown or removed node never
throws past the `RemoteResult` envelope: because a fleet's node set is dynamic
([`add_node!`](@ref)/[`remove_node!`](@ref)), a missing name is a runtime
condition, not only a typo, and belongs in the result like any other per-node
failure. This also means one bad name cannot abort a batch of calls or pulls. A
caller who wants the miss thrown has [`getindex`](@ref)`(r)`, which rethrows the
stored `KeyError`. (`fleet[name]` still throws, matching `Dict` indexing: it
returns a `Node`, not a `RemoteResult`, so it is outside this contract.)
"""
function _fleet_lookup(::Type{R}, fleet::Fleet, node_name::AbstractString) where {R}
    lock(fleet.nodes_lock) do
        haskey(fleet.nodes, node_name) ? fleet.nodes[node_name] :
            RemoteResult{R}(String(node_name), nothing, KeyError(node_name), 0.0)
    end
end

"""
    stream(fleet, node_name, method, params=nothing; into::IO, kwargs...) -> RemoteResult{StreamedResponse}

Stream a response body from `node_name` into `into`. On a TCP node this uses the
chunked path, so peak memory is bounded by `buffer_size`. On a WebSocket node,
which has no chunked receive path (HTTP.jl reassembles each frame whole), it
falls back to a materialized request written into `into`
(see [`_stream_response_dispatch`](@ref)); `stream`/`download` keep working for
every node, but the memory bound holds only on TCP nodes. A response exceeding
the node's `max_body_length` is refused rather than written into `into`.

`timeout` defaults to `nothing` (no timeout), since a bulk transfer can run far
longer than the per-request node timeout. Streamed calls are **not retried** (a
partial sink must not be re-run on top of), so the fleet retry policy does not
apply here. Errors are captured into the returned `RemoteResult`, including an
unknown or removed `node_name` (a `KeyError`; `getindex(r)` rethrows it), so a
bad name never throws past the envelope. Unexported; reach it as `RepeRPC.stream`.
"""
function stream(fleet::Fleet, node_name::String, method::String, params = nothing; into::IO,
                query_format::QueryFormat = QUERY_JSON_POINTER,
                body_format::BodyFormat = BODY_JSON,
                buffer_size::Union{Nothing,Integer} = nothing,
                timeout::Union{Float64,Nothing} = nothing)
    node = _fleet_lookup(StreamedResponse, fleet, node_name)
    node isa RemoteResult && return node
    return _fleet_stream(node, method, params, into;
                         query_format = query_format,
                         body_format = body_format,
                         buffer_size = buffer_size,
                         timeout = timeout)
end

"""
    download(fleet, node_name, method, dest_path::AbstractString; params=nothing, kwargs...) -> RemoteResult{StreamedResponse}

Open `dest_path` for writing (truncating it) and stream the response body from
`node_name` into it. Same transport, no-retry, and timeout semantics as
[`stream`](@ref). A failed transfer leaves a partial file in place (truncated at
open, not deleted on failure); for atomicity, download to a temp path and rename
on success. An unknown or removed `node_name` is captured as a failed
`RemoteResult` (a `KeyError`) **before** `dest_path` is opened, so a bad name
leaves any existing file untouched; `getindex(r)` rethrows it. Unexported; reach
it as `RepeRPC.download`.
"""
function download(fleet::Fleet, node_name::String, method::String, dest_path::AbstractString;
                  params = nothing,
                  query_format::QueryFormat = QUERY_JSON_POINTER,
                  body_format::BodyFormat = BODY_JSON,
                  buffer_size::Union{Nothing,Integer} = nothing,
                  timeout::Union{Float64,Nothing} = nothing)
    node = _fleet_lookup(StreamedResponse, fleet, node_name)
    node isa RemoteResult && return node
    return open(dest_path, "w") do io
        _fleet_stream(node, method, params, io;
                      query_format = query_format,
                      body_format = body_format,
                      buffer_size = buffer_size,
                      timeout = timeout)
    end
end

#==============================================================================#
# SVS (Serialized Value Stream) pull
#==============================================================================#

# Fleet wrappers over the single-client SVS pull API (`pull_value`,
# `pull_to_beve_file`, `pull_to_beve_zst_file`). An SVS pull is a stateful,
# multi-round-trip session that may already have advanced a server-side session
# or written partial bytes into a file, so — like `stream`/`download` — it is
# deliberately NOT routed through `_call_with_retry`: a mid-stream failure must
# not be silently re-driven on top of a partial sink or a half-consumed session.
# Errors are captured into the returned `RemoteResult`, consistent with `call`.
#
# `run` receives the node's `AbstractClient` and performs the actual pull; this
# helper only owns the connect-if-needed, timing, and error-capture envelope so
# the four public methods stay one line of intent each.
function _fleet_svs_pull(::Type{R}, node::Node, run) where {R}
    start_time = time()
    try
        if !isconnected(node.client)
            connect(node.client)
        end
        value = run(node.client)
        return RemoteResult{R}(node.name, value, nothing, time() - start_time)
    catch e
        return RemoteResult{R}(node.name, nothing, e, time() - start_time)
    end
end

"""
    pull_value(fleet, node_name, resource; timeout = nothing, total_timeout = nothing, cancel = nothing) -> RemoteResult{Any}

Fleet wrapper over the single-client [`pull_value`](@ref): open an SVS stream for
`resource` on `node_name`, stream-decode the BEVE value into a generic
`Dict`/`Vector`/scalar tree, and capture it in a `RemoteResult`. Peak memory is
the decoded tree plus one chunk; the compressed and decompressed buffers are
never materialized. `timeout` is the per-`next` round-trip timeout (`nothing`
uses the node client's default); `total_timeout` bounds the whole transfer; `cancel`
is a caller predicate polled once per chunk that aborts the pull with a
[`CancelledError`](@ref) (see the single-client [`pull_value`](@ref) for both).

    pull_value(T::Type, fleet, node_name, resource; timeout = nothing, total_timeout = nothing, cancel = nothing) -> RemoteResult{T}

Decode into the struct type `T` instead (small-payload path; see the single-client
[`pull_value`](@ref) for the memory caveat).

Like [`stream`](@ref)/[`download`](@ref), an SVS pull is **not retried**: the
fleet retry policy does not apply, because a partially consumed session must not
be silently re-driven. Errors are captured into the returned `RemoteResult`
rather than thrown, including an unknown or removed `node_name` (a `KeyError`), so
one bad node cannot abort a batch of pulls; use [`getindex`](@ref)`(r)` to rethrow
the miss. A caller-initiated cancellation is captured as a `CancelledError`;
distinguish it from a producer fault with [`cancelled`](@ref)
(`failed(r) && !cancelled(r)` selects genuine faults).
"""
function pull_value(fleet::Fleet, node_name::String, resource::AbstractString; timeout = nothing,
                    total_timeout = nothing, cancel = nothing)
    node = _fleet_lookup(Any, fleet, node_name)
    node isa RemoteResult && return node
    return _fleet_svs_pull(Any, node,
                           client -> pull_value(client, resource; timeout = timeout,
                                                total_timeout = total_timeout, cancel = cancel))
end

function pull_value(::Type{T}, fleet::Fleet, node_name::String, resource::AbstractString;
                    timeout = nothing, total_timeout = nothing, cancel = nothing) where {T}
    node = _fleet_lookup(T, fleet, node_name)
    node isa RemoteResult && return node
    return _fleet_svs_pull(T, node,
                           client -> pull_value(T, client, resource; timeout = timeout,
                                                total_timeout = total_timeout, cancel = cancel))
end

"""
    pull_to_beve_file(fleet, node_name, resource, path; timeout = nothing, total_timeout = nothing, cancel = nothing) -> RemoteResult{Nothing}

Fleet wrapper over the single-client [`pull_to_beve_file`](@ref): pull `resource`
from `node_name`, decompress it, and write a `.beve` file at `path`. Requires the
producer to be zstd-compressed and BEVE-formatted, and `CodecZstd` loaded. Fully
memory-bounded and written atomically (temp-then-rename on the terminal chunk).
`total_timeout` bounds the whole transfer; `cancel` is polled once per chunk and
aborts with a [`CancelledError`](@ref) (see the single-client [`pull_value`](@ref)).
No-retry; errors are captured into the returned `RemoteResult` (a successful pull
carries `value === nothing`, so use [`succeeded`](@ref)/[`failed`](@ref)/[`cancelled`](@ref)
rather than the value to test the outcome).
"""
function pull_to_beve_file(fleet::Fleet, node_name::String, resource::AbstractString,
                           path::AbstractString; timeout = nothing, total_timeout = nothing,
                           cancel = nothing)
    node = _fleet_lookup(Nothing, fleet, node_name)
    node isa RemoteResult && return node
    return _fleet_svs_pull(Nothing, node,
                           client -> pull_to_beve_file(client, resource, path; timeout = timeout,
                                                       total_timeout = total_timeout, cancel = cancel))
end

"""
    pull_to_beve_zst_file(fleet, node_name, resource, path; timeout = nothing, total_timeout = nothing, cancel = nothing) -> RemoteResult{Nothing}

Fleet wrapper over the single-client [`pull_to_beve_zst_file`](@ref): pull
`resource` from `node_name` and write the compressed bytes verbatim to `path` as a
`.beve.zst` file. Requires the producer to be zstd-compressed. Fully
memory-bounded and written atomically. `total_timeout` bounds the whole transfer;
`cancel` is polled once per chunk and aborts with a [`CancelledError`](@ref) (see the
single-client [`pull_value`](@ref)). No-retry; errors are captured into the returned
`RemoteResult` (a successful pull carries `value === nothing`).
"""
function pull_to_beve_zst_file(fleet::Fleet, node_name::String, resource::AbstractString,
                               path::AbstractString; timeout = nothing, total_timeout = nothing,
                               cancel = nothing)
    node = _fleet_lookup(Nothing, fleet, node_name)
    node isa RemoteResult && return node
    return _fleet_svs_pull(Nothing, node,
                           client -> pull_to_beve_zst_file(client, resource, path; timeout = timeout,
                                                           total_timeout = total_timeout, cancel = cancel))
end

"""
    pull_to_file(fleet, node_name, resource, path; timeout = nothing, total_timeout = nothing, cancel = nothing) -> RemoteResult{Nothing}

Fleet wrapper over the single-client [`pull_to_file`](@ref): pull `resource` from
`node_name` and write its logical bytes to `path`, decompressing if the stream is
zstd-compressed. The general "download to file" envelope (format-agnostic),
typically paired with a [`serve_file_stream`](@ref) producer. Fully memory-bounded
and written atomically. `total_timeout` bounds the whole transfer; `cancel` is polled
once per chunk and aborts with a [`CancelledError`](@ref) (see the single-client
[`pull_value`](@ref)). No-retry; errors are captured into the returned `RemoteResult`
(a successful pull carries `value === nothing`).
"""
function pull_to_file(fleet::Fleet, node_name::String, resource::AbstractString,
                      path::AbstractString; timeout = nothing, total_timeout = nothing,
                      cancel = nothing)
    node = _fleet_lookup(Nothing, fleet, node_name)
    node isa RemoteResult && return node
    return _fleet_svs_pull(Nothing, node,
                           client -> pull_to_file(client, resource, path; timeout = timeout,
                                                  total_timeout = total_timeout, cancel = cancel))
end

#==============================================================================#
# Callable Syntax
#==============================================================================#

"""
    (fleet::Fleet)(method::String, params=nothing; tags=String[], kwargs...)

Callable broadcast syntax. Equivalent to `broadcast(fleet, method, params; tags=tags, kwargs...)`.
"""
function (fleet::Fleet)(method::String, params=nothing;
                        tags::Vector{String} = String[],
                        query_format::QueryFormat = QUERY_JSON_POINTER,
                        body_format::BodyFormat = BODY_JSON)
    broadcast(fleet, method, params; tags=tags, query_format=query_format, body_format=body_format)
end

"""
    (fleet::Fleet)(node_name::String, method::String, params=nothing; kwargs...)

Callable single-node syntax. Equivalent to `call(fleet, node_name, method, params; kwargs...)`.
"""
function (fleet::Fleet)(node_name::String, method::String, params;
                        query_format::QueryFormat = QUERY_JSON_POINTER,
                        body_format::BodyFormat = BODY_JSON)
    call(fleet, node_name, method, params; query_format=query_format, body_format=body_format)
end

#==============================================================================#
# Health Monitoring
#==============================================================================#

"""
    health_check(fleet::Fleet; health_endpoint::String="/status") -> Dict{String, HealthStatus}

Check health of all nodes by calling a health endpoint.

Returns a Dict mapping node names to HealthStatus named tuples with fields:
- `healthy::Bool` - Whether the node responded successfully
- `latency::Float64` - Response time in seconds
- `error::Union{Exception, Nothing}` - Error if unhealthy
"""
function health_check(fleet::Fleet; health_endpoint::String = "/status")
    results = Dict{String, HealthStatus}()

    # Snapshot nodes
    node_list = lock(fleet.nodes_lock) do
        collect(values(fleet.nodes))
    end

    # Check in parallel
    tasks = Dict{String, Task}()
    for node in node_list
        tasks[node.name] = @async begin
            start_time = time()
            try
                if !isconnected(node.client)
                    connect(node.client)
                end
                send_request(node.client, health_endpoint, nothing; timeout=5.0)
                latency = time() - start_time
                return (healthy=true, latency=latency, error=nothing)
            catch e
                latency = time() - start_time
                return (healthy=false, latency=latency, error=e)
            end
        end
    end

    # Collect results
    for (name, task) in tasks
        results[name] = fetch(task)
    end

    return results
end

#==============================================================================#
# Pretty Printing
#==============================================================================#

function Base.show(io::IO, config::NodeConfig)
    print(io, "NodeConfig(\"$(config.host)\", $(config.port)")
    if config.name != config.host
        print(io, "; name=\"$(config.name)\"")
    end
    if !isempty(config.tags)
        print(io, ", tags=$(config.tags)")
    end
    print(io, ")")
end

function Base.show(io::IO, node::Node)
    status = isconnected(node.client) ? "connected" : "disconnected"
    print(io, "Node(\"$(node.name)\", $(node.host):$(node.port), $status)")
end

function Base.show(io::IO, fleet::Fleet)
    n = length(fleet)
    connected = length(connected_nodes(fleet))
    print(io, "Fleet($connected/$n nodes connected)")
end

function Base.show(io::IO, r::RemoteResult)
    if succeeded(r)
        print(io, "RemoteResult($(r.node), success, $(round(r.elapsed * 1000, digits=1))ms)")
    elseif cancelled(r)
        print(io, "RemoteResult($(r.node), cancelled, $(round(r.elapsed * 1000, digits=1))ms)")
    else
        print(io, "RemoteResult($(r.node), failed: $(typeof(r.error)), $(round(r.elapsed * 1000, digits=1))ms)")
    end
end
