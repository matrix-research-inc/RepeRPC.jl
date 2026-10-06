# Fleet API - Multi-Server Control

The Fleet API provides a unified interface for managing and communicating with multiple REPE servers. It supports both TCP (`Fleet`) for bidirectional communication and UDP (`UniUDPFleet`) for fire-and-forget broadcasting.

## Features

- Unified interface to treat multiple servers as a single logical unit
- Tag-based node filtering for targeted operations
- Parallel execution with structured results per node
- Bulk transfer per node: response streaming and SVS value pulls (no-retry)
- Automatic retry with configurable policy (TCP Fleet)
- Dynamic node management at runtime
- Thread-safe concurrent operations
- Callable syntax for intuitive invocation

## TCP Fleet (Bidirectional)

Use `Fleet` when you need request/response communication with multiple servers.

### Quick Start

```julia
using RepeRPC

# Define fleet configuration
config = [
    NodeConfig("compute-1.local", 8080; tags=["compute"]),
    NodeConfig("compute-2.local", 8080; tags=["compute"]),
    NodeConfig("storage.local", 8080; tags=["storage"]),
]

fleet = Fleet(config)
connect!(fleet)

# Broadcast to all nodes
results = broadcast(fleet, "/status")

for (name, result) in results
    if succeeded(result)
        println("$name: $(result.value)")
    else
        println("$name: ERROR - $(result.error)")
    end
end

disconnect!(fleet)
```

### NodeConfig

Configuration for a remote server node.

```julia
# Simple - name defaults to hostname
NodeConfig("compute-1.local", 8080)

# With tags for filtering
NodeConfig("compute-1.local", 8080; tags=["compute", "gpu"])

# Custom name (required for multiple servers on same host)
NodeConfig("192.168.1.10", 8080; name="compute-1")
NodeConfig("192.168.1.10", 8081; name="compute-2")

# Custom timeout (overrides fleet default)
NodeConfig("slow-server.local", 8080; timeout=60.0)
```

| Field | Default | Description |
|-------|---------|-------------|
| `host` | required | Hostname or IP address |
| `port` | required | Port number (1-65535) |
| `name` | `host` | Unique identifier |
| `tags` | `[]` | Tags for filtering |
| `timeout` | `30.0` | Request timeout in seconds |

**Timeout precedence:** `NodeConfig.timeout` > `Fleet.default_timeout` > hardcoded default (30.0s)

### Fleet Construction

```julia
# From NodeConfig vector
fleet = Fleet(configs; timeout=30.0, max_retry_attempts=3, retry_delay=1.0)

# Empty fleet (for dynamic population)
fleet = Fleet()
```

**Note:** Duplicate node names throw `ArgumentError`:

```julia
# ERROR: duplicate name "server.local"
Fleet([
    NodeConfig("server.local", 8080),
    NodeConfig("server.local", 8081),
])

# OK: explicit unique names
Fleet([
    NodeConfig("server.local", 8080; name="service-a"),
    NodeConfig("server.local", 8081; name="service-b"),
])
```

### Connection Management

```julia
# Connect to all nodes
result = connect!(fleet)
println("Connected: $(result.connected)")
println("Failed: $(result.failed)")

# Disconnect from all nodes
result = disconnect!(fleet)

# Reconnect dropped connections
result = reconnect!(fleet)

# Check connection status
isconnected(fleet)              # All nodes connected?
isconnected(fleet, "node-1")    # Specific node?
```

### Node Access

```julia
# Get all nodes
all_nodes = nodes(fleet)

# Get only connected nodes
active = connected_nodes(fleet)

# Filter by tags (returns nodes matching ALL specified tags)
compute_nodes = filter_nodes(fleet; tags=["compute"])
gpu_nodes = filter_nodes(fleet; tags=["compute", "gpu"])

# Access by name
node = fleet["compute-1.local"]

# Get all node names
names = keys(fleet)

# Get node count
n = length(fleet)
```

### Remote Invocation

```julia
# Call a specific node
result = call(fleet, "compute-1.local", "/compute", Dict("value" => 42))
if succeeded(result)
    println("Result: $(result.value)")
end

# Broadcast to all nodes
results = broadcast(fleet, "/status")

# Broadcast with tag filtering
results = broadcast(fleet, "/compute", Dict("x" => 10); tags=["compute"])

# Callable syntax - broadcast
results = fleet("/status")
results = fleet("/compute", Dict("x" => 10); tags=["compute"])

# Callable syntax - single node
result = fleet("compute-1.local", "/compute", Dict("x" => 10))
```

### Map-Reduce

Aggregate results from multiple nodes:

```julia
# Sum results from compute nodes
total = map_reduce(fleet, "/compute", Dict("value" => 10); tags=["compute"]) do results
    sum(r.value["result"] for r in results if succeeded(r))
end

# Count successful responses
count = map_reduce(fleet, "/ping") do results
    count(succeeded, results)
end

# Collect all values
all_values = map_reduce(fleet, "/data") do results
    [r.value for r in results if succeeded(r)]
end
```

### RemoteResult

Structured response from remote calls:

```julia
struct RemoteResult{T}
    node::String                       # Node that processed the request
    value::Union{T, Nothing}           # Result value (nothing on failure)
    error::Union{Exception, Nothing}   # Exception (nothing on success)
    elapsed::Float64                   # Time taken in seconds
end

# Check success/failure
succeeded(result)  # true if error === nothing
failed(result)     # true if error !== nothing

# Get value (throws if failed)
value = result[]
```

### Bulk Transfer (streaming and SVS pulls)

For payloads too large to return in a single `call`, the fleet offers two distinct
download mechanisms against a named node. Both are **single-node** (not broadcast),
both return a `RemoteResult`, and both are **never retried** — a transfer that has
already written partial bytes (or advanced a server-side stream session) must not
be silently re-driven on top of itself, so the fleet retry policy does not apply.
A failure is captured into the returned `RemoteResult` rather than thrown, except an
unknown `node_name`, which raises `KeyError` before any transfer begins.

`stream`/`download` are unexported (they would collide with `Base`); reach them as
`RepeRPC.stream` / `RepeRPC.download`. The SVS pulls are exported.

**1. Chunked response streaming** — one response body streamed into a sink. On a TCP
node the chunked path bounds peak memory by `buffer_size`; on a WebSocket node it
falls back to a fully materialized request (the memory bound then holds only for
raw-binary or string bodies, and a body over the node's `max_body_length` is
refused). Use this when the server returns the whole body in one response.

```julia
io = IOBuffer()
r = RepeRPC.stream(fleet, "storage-1", "/blob"; into = io)          # RemoteResult{StreamedResponse}
r = RepeRPC.download(fleet, "storage-1", "/artifacts/get", "out.bin"; params = Dict("id" => "42"))
```

**2. SVS pulls** — a [Serialized Value Stream](StreamingValues.md): the server
BEVE-serializes (and optionally zstd-compresses) an in-memory value whose length is
not known up front, streamed one chunk per request/response round trip. Neither side
materializes the full serialized copy. The fleet wrappers target one node by name and
mirror the single-client pulls (`pull_value`, `pull_to_beve_file`,
`pull_to_beve_zst_file`):

```julia
# Stream-decode into a generic value tree (Dict/Vector/scalar).
r = pull_value(fleet, "storage-1", "telemetry")               # RemoteResult{Any}
succeeded(r) && @show r[]["samples"]

# Decode into a struct type (small-payload path; materializes before constructing T).
rt = pull_value(Frame, fleet, "storage-1", "telemetry")       # RemoteResult{Frame}

# Stream straight to disk, memory-bounded and written atomically
# (temp-then-rename on the terminal chunk, so a dropped connection leaves no
# truncated file that looks complete). Both require a zstd-compressed producer.
rf = pull_to_beve_file(fleet, "storage-1", "telemetry", "telemetry.beve")          # decompress to .beve
rz = pull_to_beve_zst_file(fleet, "storage-1", "telemetry", "telemetry.beve.zst")  # compressed bytes verbatim
```

Each pull takes an optional `timeout` keyword (the per-`next` round-trip timeout;
`nothing` uses the node client's default). A successful file-mode pull carries
`value === nothing`, so test the outcome with `succeeded`/`failed`, not the value.
`pull_value` with a struct type and the `.beve` mode require `CodecZstd` loaded for
compressed streams.

| Mechanism | Memory bound | When to use |
|---|---|---|
| `RepeRPC.stream` / `RepeRPC.download` | `buffer_size` (TCP only) | A known-length body returned in one response |
| `pull_value` / `pull_to_beve_*` | one chunk + chosen output | A serialized in-memory value with no length known up front |

### Health Monitoring

```julia
health = health_check(fleet; health_endpoint="/status")

for (name, status) in health
    if status.healthy
        println("$name: $(round(status.latency * 1000, digits=1))ms")
    else
        println("$name: UNHEALTHY - $(status.error)")
    end
end
```

`HealthStatus` is a NamedTuple with fields:
- `healthy::Bool` - Whether the node responded successfully
- `latency::Float64` - Response time in seconds
- `error::Union{Exception, Nothing}` - Error if unhealthy

### Dynamic Node Management

```julia
# Add a node at runtime
add_node!(fleet, NodeConfig("compute-4.local", 8080; tags=["compute"]))
connect!(fleet)  # Connect newly added nodes

# Remove a node (disconnects automatically)
remove_node!(fleet, "compute-4.local")
```

### Complete Example

```julia
using RepeRPC

const FLEET_CONFIG = [
    NodeConfig("compute-1.local", 8080; tags=["compute"]),
    NodeConfig("compute-2.local", 8080; tags=["compute"]),
    NodeConfig("compute-3.local", 8080; tags=["compute"]),
    NodeConfig("storage.local", 8080; tags=["storage"]),
]

function main()
    fleet = Fleet(FLEET_CONFIG; timeout=30.0, max_retry_attempts=3)

    try
        # Connect
        status = connect!(fleet)
        println("Connected: $(length(status.connected))/$(length(fleet)) nodes")

        # Health check
        println("\n--- Fleet Health ---")
        for (name, h) in health_check(fleet)
            symbol = h.healthy ? "+" : "-"
            msg = h.healthy ? "$(round(h.latency * 1000, digits=1))ms" : h.error
            println("[$symbol] $name: $msg")
        end

        # Broadcast to compute nodes
        println("\n--- Compute Results ---")
        results = broadcast(fleet, "/compute", Dict("value" => 42); tags=["compute"])
        for (name, r) in results
            succeeded(r) && println("$name: $(r.value)")
        end

        # Aggregate
        total = map_reduce(fleet, "/compute", Dict("value" => 50); tags=["compute"]) do results
            sum(r.value["result"] for r in results if succeeded(r))
        end
        println("\nTotal: $total")

    finally
        disconnect!(fleet)
    end
end

main()
```

---

## UniUDP Fleet (Unidirectional)

Use `UniUDPFleet` for fire-and-forget broadcasting over UDP. This is ideal for:

- One-way satellite or radio links
- Sensor networks with asymmetric connectivity
- Broadcast/multicast scenarios
- High-throughput logging without acknowledgments

### Quick Start

```julia
using RepeRPC

# Define UniUDP fleet
config = [
    UniUDPNodeConfig("gateway-1.local", 5000; tags=["primary"]),
    UniUDPNodeConfig("gateway-2.local", 5000; tags=["backup"]),
    UniUDPNodeConfig("logger.local", 5001; tags=["logging"]),
]

fleet = UniUDPFleet(config)

# Broadcast notification to all
results = send_notify(fleet, "/sensor/reading", Dict("value" => 23.5))

for (name, result) in results
    if succeeded(result)
        println("$name: sent (msg_id=$(result.message_id))")
    else
        println("$name: FAILED - $(result.error)")
    end
end

close(fleet)
```

### UniUDPNodeConfig

Configuration for a UniUDP target endpoint:

```julia
# Simple configuration
UniUDPNodeConfig("sensor-gateway.local", 5000)

# With reliability settings for lossy links
UniUDPNodeConfig("satellite-uplink.local", 5000;
    redundancy = 3,        # Triple redundancy
    fec_group_size = 4,    # XOR parity every 4 chunks
    tags = ["uplink", "critical"]
)

# Multiple receivers on same host
UniUDPNodeConfig("collector.local", 5000; name="collector-primary")
UniUDPNodeConfig("collector.local", 5001; name="collector-backup")
```

| Field | Default | Description |
|-------|---------|-------------|
| `host` | required | Hostname or IP address |
| `port` | required | Port number (1-65535) |
| `name` | `host` | Unique identifier |
| `tags` | `[]` | Tags for filtering |
| `redundancy` | `1` | Packet redundancy (1-N) |
| `chunk_size` | `1024` | Bytes per chunk |
| `fec_group_size` | `4` | FEC parity group size |

### Send Operations

```julia
# Broadcast notification to all nodes
results = send_notify(fleet, "/sensor/reading", Dict("value" => 23.5))

# Broadcast with tag filtering
results = send_notify(fleet, "/alert", Dict("level" => "warning"); tags=["primary"])

# Broadcast request (fire-and-forget, server handles result via callback)
results = send_request(fleet, "/compute/analyze", Dict("data_id" => "42"))

# Notify all nodes (convenience function, no tag filtering)
results = notify_all(fleet, "/heartbeat", Dict("source" => "controller"))

# Callable syntax - broadcast
results = fleet("/sensor/reading", Dict("value" => 23.5))
results = fleet("/alert", Dict("level" => "warning"); tags=["primary"])

# Callable syntax - single node
result = fleet("gateway-1.local", "/sensor/reading", Dict("value" => 23.5))
```

### SendResult

Result from a UniUDP send operation:

```julia
struct SendResult
    node::String                       # Target node name
    message_id::UInt64                 # UniUDP message ID
    error::Union{Exception, Nothing}   # Exception (nothing on success)
    elapsed::Float64                   # Time taken in seconds
end

# Check success/failure
succeeded(result)  # true if error === nothing
failed(result)     # true if error !== nothing
```

**Note:** `succeeded(result)` indicates the send syscall completed, not delivery confirmation. UDP is inherently unreliable.

### Node Management

```julia
# Get all nodes
all_nodes = nodes(fleet)

# Filter by tags
sensor_nodes = filter_nodes(fleet; tags=["sensor"])

# Access by name
node = fleet["gateway-1.local"]

# Add/remove nodes dynamically
add_node!(fleet, UniUDPNodeConfig("gateway-3.local", 5000))
remove_node!(fleet, "gateway-1.local")

# Close all sockets
close(fleet)
```

### High-Reliability Configuration

For critical notifications over lossy links:

```julia
critical_fleet = UniUDPFleet([
    UniUDPNodeConfig("control-1.local", 5000;
        name = "control-primary",
        redundancy = 3,         # Triple redundancy
        fec_group_size = 4,     # FEC for additional recovery
        tags = ["control", "critical"]
    ),
    UniUDPNodeConfig("control-2.local", 5000;
        name = "control-backup",
        redundancy = 3,
        fec_group_size = 4,
        tags = ["control", "critical"]
    ),
])

# Emergency broadcast with high reliability
send_notify(critical_fleet, "/emergency/shutdown", Dict(
    "reason" => "overheat",
    "timestamp" => time()
))
```

### Mixed Fleet Pattern

Combine TCP and UniUDP fleets for hybrid architectures:

```julia
# TCP fleet for request/response
tcp_fleet = Fleet([
    NodeConfig("api-server-1.local", 8080; tags=["api"]),
    NodeConfig("api-server-2.local", 8080; tags=["api"]),
])
connect!(tcp_fleet)

# UniUDP fleet for fire-and-forget notifications
udp_fleet = UniUDPFleet([
    UniUDPNodeConfig("monitor-1.local", 5000; tags=["monitor"]),
    UniUDPNodeConfig("monitor-2.local", 5000; tags=["monitor"]),
])

# Coordinator: get response via TCP, notify monitors via UDP
function process_request(data)
    results = broadcast(tcp_fleet, "/process", data; tags=["api"])

    send_notify(udp_fleet, "/activity", Dict(
        "request" => data,
        "processed_by" => [r.node for r in values(results) if succeeded(r)]
    ))

    return results
end
```

---

## API Reference

### TCP Fleet Types

| Type | Description |
|------|-------------|
| `NodeConfig` | Configuration for a TCP server node |
| `Node` | Internal representation of a connected server |
| `Fleet` | Multi-server control interface |
| `RemoteResult{T}` | Structured response from remote calls |
| `HealthStatus` | Health check result (NamedTuple) |

### TCP Fleet Functions

| Function | Description |
|----------|-------------|
| `Fleet(configs; timeout, max_retry_attempts, retry_delay)` | Create fleet |
| `connect!(fleet)` | Connect to all nodes |
| `disconnect!(fleet)` | Disconnect from all nodes |
| `reconnect!(fleet)` | Reconnect dropped connections |
| `isconnected(fleet)` / `isconnected(fleet, name)` | Check connection status |
| `nodes(fleet)` | Get all nodes |
| `connected_nodes(fleet)` | Get connected nodes |
| `filter_nodes(fleet; tags)` | Filter nodes by tags |
| `add_node!(fleet, config)` | Add node at runtime |
| `remove_node!(fleet, name)` | Remove and disconnect node |
| `call(fleet, node_name, method, params)` | Call specific node |
| `broadcast(fleet, method, params; tags)` | Call all matching nodes |
| `map_reduce(fn, fleet, method, params; tags)` | Broadcast and reduce |
| `health_check(fleet; health_endpoint)` | Check node health |
| `RepeRPC.stream(fleet, node_name, method; into, ...)` | Stream a response body into a sink (no-retry) |
| `RepeRPC.download(fleet, node_name, method, dest_path; ...)` | Stream a response body to a file (no-retry) |
| `pull_value(fleet, node_name, resource; timeout)` | SVS-pull and decode a value (no-retry) |
| `pull_value(T, fleet, node_name, resource; timeout)` | SVS-pull and decode into struct `T` (no-retry) |
| `pull_to_beve_file(fleet, node_name, resource, path; timeout)` | SVS-pull, decompress, write `.beve` atomically (no-retry) |
| `pull_to_beve_zst_file(fleet, node_name, resource, path; timeout)` | SVS-pull, write `.beve.zst` verbatim atomically (no-retry) |

### UniUDP Fleet Types

| Type | Description |
|------|-------------|
| `UniUDPNodeConfig` | Configuration for a UniUDP endpoint |
| `UniUDPNode` | Internal representation of a UniUDP target |
| `UniUDPFleet` | Multi-endpoint broadcast interface |
| `SendResult` | Result from send operation |

### UniUDP Fleet Functions

| Function | Description |
|----------|-------------|
| `UniUDPFleet(configs)` | Create fleet |
| `nodes(fleet)` | Get all nodes |
| `filter_nodes(fleet; tags)` | Filter nodes by tags |
| `add_node!(fleet, config)` | Add node at runtime |
| `remove_node!(fleet, name)` | Remove node |
| `close(fleet)` | Close all sockets |
| `send_notify(fleet, method, params; tags)` | Broadcast notification |
| `send_request(fleet, method, params; tags)` | Broadcast request |
| `notify_all(fleet, method, params)` | Notify all nodes |

### Convenience Functions

| Function | Description |
|----------|-------------|
| `succeeded(result)` | Check if result succeeded |
| `failed(result)` | Check if result failed |

---

## Implementation Notes

### Thread Safety

Both `Fleet` and `UniUDPFleet` use `ReentrantLock` to protect their internal node dictionaries:

- **Read operations** (`nodes`, `filter_nodes`, `broadcast`): Acquire lock briefly to snapshot nodes, then release before executing requests
- **Write operations** (`add_node!`, `remove_node!`): Acquire lock, modify dict, release lock
- **Requests in flight** operate on snapshots; concurrent modifications won't affect them

### Error Handling

**TCP Fleet:**
- Connection failures are captured and returned, not thrown
- Request failures are captured in `RemoteResult.error`
- Retry is automatic based on fleet's `retry_policy`

**UniUDP Fleet:**
- Send errors are captured in `SendResult.error`
- `succeeded()` indicates send syscall completed, not delivery
- For delivery confirmation, use application-level acknowledgments

### Key Differences: TCP vs UniUDP Fleet

| Aspect | TCP Fleet | UniUDP Fleet |
|--------|-----------|--------------|
| Communication | Bidirectional | Unidirectional |
| Connection state | Persistent TCP | Stateless UDP |
| `call()` | Returns response | N/A |
| `broadcast()` | Collects responses | Fire-and-forget |
| `map_reduce()` | Aggregates results | N/A |
| `connect!()` | Establishes connections | N/A |
| Error indication | Response delivery | Send syscall only |
