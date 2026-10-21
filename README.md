# RepeRPC.jl

Julia implementation of the [REPE (Remote Efficient Protocol Extension)](https://github.com/repe-org/REPE) RPC specification. REPE is a fast and simple binary RPC protocol that supports flexible data formats and query specifications.

## Features

- Full implementation of the official [REPE v1 specification](https://github.com/repe-org/REPE)
- Binary protocol with little-endian encoding
- Support for multiple data formats (JSON, BEVE, UTF-8, raw binary)
- Asynchronous client/server architecture
- Error handling with standardized error codes
- Notification support (fire-and-forget messages)
- **Registry support** for serving dictionaries with JSON Pointer syntax (Glaze-compatible)
- **WebSocket support** for browser-based REPE clients with optional HTML serving
- **Streaming** of multi-MB/GB bodies socket ↔ `IO` in fixed-size chunks, with a `max_body_length` guard
- **Serialized value streaming (SVS)** for downloading a large in-memory value as BEVE (optionally zstd-compressed), pulled in chunks with bounded memory on both ends — decode it into a value or write a `.beve`/`.beve.zst` file
- **UniUDP support** for REPE over unidirectional UDP with redundancy and FEC
- **Fleet API** for multi-server control with TCP and UniUDP support
- **ReplTree integration** for serving interactive REPL menus over the network (optional)
- Compatible with C++ Glaze implementation
- Comprehensive test suite with 1000+ unit tests
- Integration tested with Glaze C++ servers

## Requirements

- Julia 1.12 or higher
- For C++ server (optional):
  - C++20 compatible compiler
  - CMake 3.15+
  - Glaze library (automatically fetched by CMake)

## Installation

```julia
using Pkg
Pkg.add("RepeRPC")
```

## Quick Start

### Server Example

```julia
using RepeRPC

# Create server
server = Server("localhost", 8080)

# Enable stack traces for easier debugging (optional)
# server = Server("localhost", 8080; print_stacktrace=true)

# Register handlers
RepeRPC.register(server, "/api/add", function(params, request)
    return Dict("result" => params["a"] + params["b"])
end)

# Start server (blocking)
listen(server)

# Or start server asynchronously
listen(server; async=true)

# Stop the server when done
stop(server)
```

Pass `print_stacktrace=true` when constructing the server if you want REPE to log stack traces alongside the standard error messages while you are debugging.

### Client Example

```julia
using RepeRPC

# Create and connect client
client = Client("localhost", 8080)
connect(client)

# Make RPC call
result = send_request(client, "/api/add", Dict("a" => 10, "b" => 20))
println(result["result"])  # 30

# Send notification (no response expected)
send_notify(client, "/api/log", "Event occurred")

disconnect(client)
```

### Typed Responses

REPE can decode JSON or BEVE responses directly into Julia structs. Define standard Julia structs for your payloads and pass the type (or a `result_type` keyword) when calling `send_request` or `send_request_async`.

```julia
using RepeRPC

struct SensorReading
    id::Int
    status::String
end

client = Client("localhost", 8080)
connect(client)

# JSON response decoded straight into SensorReading
reading = send_request(SensorReading, client, "/api/sensor", Dict("id" => 42))
@assert reading isa SensorReading

# Keyword form is also available
reading2 = send_request(client, "/api/sensor", Dict("id" => 42);
                        result_type = SensorReading)

# Async variant
task = send_request_async(SensorReading, client, "/api/sensor", Dict("id" => 42))
reading3 = fetch(task)

# BEVE responses are supported automatically by the typed API
reading_beve = send_request(SensorReading, client, "/api/sensor/beve", Dict())

disconnect(client)
```

## Protocol Specification

For the complete protocol specification, see the [official REPE documentation](https://github.com/repe-org/REPE).

REPE messages consist of a fixed 48-byte header followed by optional query and body sections:

```
[Header (48 bytes)] [Query (variable)] [Body (variable)]
```

### Header Structure

- `length` (8 bytes): Total message length
- `spec` (2 bytes): Magic number (0x1507)
- `version` (1 byte): Protocol version (1)
- `notify` (1 byte): No-response flag
- `reserved` (4 bytes): Must be zero
- `id` (8 bytes): Request identifier
- `query_length` (8 bytes): Query section length
- `body_length` (8 bytes): Body section length
- `query_format` (2 bytes): Query format type
- `body_format` (2 bytes): Body format type
- `ec` (4 bytes): Error code

## Data Formats

### Query Formats
- `QUERY_RAW_BINARY` (0): Raw binary data
- `QUERY_JSON_POINTER` (1): JSON Pointer syntax

### Body Formats
- `RepeRPC.RAW_BINARY` (0): Raw binary data
- `RepeRPC.BEVE` (1): BEVE binary format  
- `RepeRPC.JSON` (2): JSON data
- `RepeRPC.UTF8` (3): UTF-8 text

Note: The underlying protocol constants are `BODY_RAW_BINARY`, `BODY_BEVE`, `BODY_JSON`, and `BODY_UTF8`, but the convenient aliases above are recommended for use in your code.

## Error Codes

Standard REPE error codes (0-4095 reserved):
- `EC_OK` (0): No error
- `EC_VERSION_MISMATCH` (1): Protocol version mismatch
- `EC_INVALID_HEADER` (2): Invalid header
- `EC_INVALID_QUERY` (3): Invalid query
- `EC_INVALID_BODY` (4): Invalid body
- `EC_PARSE_ERROR` (5): Parse error
- `EC_METHOD_NOT_FOUND` (6): Method not found
- `EC_TIMEOUT` (7): Request timeout

Application-specific error codes start at 4096.

## Advanced Usage

### Custom Middleware

```julia
server = Server("localhost", 8080)

# Add logging middleware
RepeRPC.use(server, function(request)
    println("Request: ", parse_query(request))
    return nothing  # Continue processing
end)

# Add authentication middleware
RepeRPC.use(server, function(request)
    if !check_auth(request)  # check_auth is your custom function
        # Return custom application error (codes >= 4096)
        return ErrorCode(4096)  # Custom EC_UNAUTHORIZED
    end
    return nothing
end)
```

### Different Data Formats

```julia
# Send JSON data
result = send_request(client, "/api/data", 
                     Dict("key" => "value"),
                     body_format = RepeRPC.JSON)

# Send BEVE binary data (compact and efficient)
result = send_request(client, "/api/data",
                     Dict("numbers" => [1, 2, 3], "flag" => true),
                     body_format = RepeRPC.BEVE)

# Send plain text
result = send_request(client, "/api/text",
                     "Hello, World!",
                     body_format = RepeRPC.UTF8)

# Send raw binary
data = UInt8[1, 2, 3, 4, 5]
result = send_request(client, "/api/binary",
                     data,
                     body_format = RepeRPC.RAW_BINARY)
```

### Timeout Control

```julia
# Set default timeout for client
client = Client("localhost", 8080; timeout=10.0)

# Override timeout for specific request
result = send_request(client, "/api/slow", params, timeout=60.0)
```

### Async and Batch Operations

```julia
# Async request - returns a Task
task = send_request_async(client, "/api/data", params,
                         body_format = RepeRPC.JSON)
result = fetch(task)

# Batch multiple requests
requests = [
    ("/api/method1", Dict("param" => 1)),
    ("/api/method2", Dict("param" => 2)),
    ("/api/method3", Dict("param" => 3))
]
tasks = batch(client, requests; body_format = RepeRPC.JSON)
results = await_batch(tasks)

# Check connection status
if isconnected(client)
    println("Client is connected")
end
```

## Streaming Large Bodies

REPE can move multi-MB to multi-GB bodies over the TCP `Client`/`Server` without ever materializing the whole payload in memory. A body flows **socket ↔ `IO` in fixed-size chunks**, so peak memory is bounded by the chunk size regardless of payload size. The wire format is unchanged: a streamed message is byte-for-byte identical to the equivalent non-streamed one, so streamed and non-streamed peers (including Glaze C++) interoperate freely in either direction.

### Stream a response from the server

A handler returns a `StreamBody` (or `FileBody`) instead of an in-memory result. The server writes the header with the known `body_length`, then pumps the source to the socket in chunks.

```julia
server = Server("0.0.0.0", 8080)

# Stream a file straight from disk (length = filesize, handle closed after send)
register(server, "/artifacts/get", function (params, request)
    path = resolve_artifact(params["id"])   # confine to a safe root in real code
    return FileBody(path)
end)

# Or stream any IO of known length. `own=false` keeps a reused IO open.
register(server, "/blob", (params, request) -> StreamBody(IOBuffer(data), length(data)))
```

`StreamBody` requires the length up front (REPE is a prefix-length protocol, so there is no chunked transfer encoding). A source whose size is unknown until fully produced must be sized or staged first.

### Download a response on the client

```julia
client = Client("host", 8080)

# Stream the response body straight into a file (truncates dest; not exported,
# so call it as RepeRPC.download to avoid shadowing Downloads.download)
resp = RepeRPC.download(client, "/artifacts/get", "out.bin"; params = Dict("id" => "42"))
@show resp.bytes resp.body_format

# Or stream into any IO sink
open("out.bin", "w") do io
    stream_response(client, "/blob"; into = io, buffer_size = 1024 * 1024)
end
```

`stream_response`/`download` return a `StreamedResponse` (`bytes`, `body_format`, `ec`); the body is in the sink. They default to **no timeout** (a bulk transfer can legitimately run long); pass `timeout` to bound it. A server error or a truncated transfer raises, and any bytes already written to the sink are partial, not a complete body. A failed `download` leaves the partial file in place, so for atomicity stream to a temp path and rename on success.

### Stream a request body (upload)

```julia
open("big_upload.bin", "r") do io
    n = filesize("big_upload.bin")
    stream_request(client, "/ingest", io, n; body_format = RepeRPC.RAW_BINARY)
end
```

The client never materializes the upload body. The server receives it normally (bounded by `max_body_length`, below); a server-side zero-copy *receive* handler is out of scope.

### `keepalive` (TCP client)

A peer that disappears *without* closing the socket -- power loss, a pulled cable, a firewall dropping the flow -- sends no FIN, so the connection looks healthy and the reader waits forever. TCP keepalive is what detects it: a request then fails with `ConnectionError` (which `Fleet` retries) instead of an unhelpful `TimeoutError`, and a streamed wait with `timeout = nothing` stops hanging indefinitely.

`Server` has the same exposure in the other direction: a client that vanishes leaves its connection task parked in `eof`, holding a socket until the process runs out of file descriptors. Both sides take the same keyword.

```julia
Client("host", 8080)                      # default: probe after 15 s idle, dead at ~25 s
Client("host", 8080; keepalive = 60.0)    # longer grace for a flaky link
Client("host", 8080; keepalive = nothing) # disable probing
Server("0.0.0.0", 8080; keepalive = 60.0) # applied to each accepted connection
```

Probes are answered by the peer's kernel, so a long-running handler is unaffected -- keepalive detects an unreachable *host*, not a busy one.

### `max_body_length` guard

Both TCP read paths (server and client) allocate from wire-controlled lengths, so a garbled or malicious header could otherwise force an unbounded allocation. The cap bounds both the `query_length` and the `body_length` and is checked before allocating; the default `nothing` (unlimited) still refuses a length past `typemax(Int)`:

```julia
Server("0.0.0.0", 8080; max_body_length = 8 * 1024^3)   # reject bodies over 8 GiB
Client("host", 8080;    max_body_length = 8 * 1024^3)
```

A server rejects an oversized request with `EC_INVALID_HEADER` and closes the connection; a client refuses an oversized response rather than allocating.

Over WebSockets the cap is **client-side only**, and it is a *post-receive* ceiling rather than a pre-allocation bound. HTTP.jl exposes no per-frame read, so by the time REPE sees the header the whole frame is already buffered in memory; `WSClient(...; max_body_length = ...)` bounds what REPE parses and delivers, not what HTTP.jl buffers. `WSServer` takes no `max_body_length` at all. Inbound WebSocket messages are bounded by frame size instead: `WSServer`, `WSClient`, and `NodeConfig` take `max_frame_size`, 16 MiB by default, and a peer that sends a larger frame has its connection closed. Raise it for larger messages, and use a TCP node where you need a hard pre-allocation bound.

### Fleet streaming

```julia
# Bounded-memory on TCP nodes; WebSocket nodes fall back to a materialized
# request written into the sink (only raw-binary/string bodies). Both keep working.
r = RepeRPC.stream(fleet, "storage-1", "/blob"; into = io)        # RemoteResult{StreamedResponse}
r = RepeRPC.download(fleet, "storage-1", "/artifacts/get", "out.bin"; params = Dict("id" => "42"))
succeeded(r) && @show r.value.bytes
```

Streamed fleet calls are **not retried**: a transfer that already wrote partial bytes must not be silently re-run on top of them, so the fleet retry policy does not apply to `stream`/`download`.

> **Concurrency.** Streaming bounds *memory*, not the latency of other requests sharing the connection. The single reader pumps a whole body before it can demux any other response, so a large transfer monopolizes its connection for its full duration. Route bulk transfers over a dedicated `Client` when they must coexist with latency-sensitive traffic.

## Streaming Serialized Values (SVS)

`StreamBody` (above) streams a body whose **length is known up front**. A *serialized in-memory value* — a struct, a `Dict`, a numeric `Vector` — has no such length until it has been serialized (and compressed), so it cannot use the prefix-length path. **SVS** (Serialized Value Stream) fills that gap: the server BEVE-serializes the value and, optionally, zstd-compresses it *as the client pulls it*, one chunk per request/response round trip. Neither side ever materializes the full serialized or compressed copy — the producer holds only the value it already has plus small codec buffers; the consumer holds one chunk plus its chosen output. Flow control is the request/response cadence itself (no ACK window, no unbounded queue), and it works identically over the TCP `Server`/`Client` and the WebSocket `WSServer`/`WSClient`.

The wire is the cross-language [REPE SVS contract](https://github.com/repe-org/REPE/blob/main/serialized-stream-protocol.md), so a RepeRPC.jl peer interoperates with peers in other languages. Direction is download-only: the server produces, the client pulls.

zstd compression (the default) requires `CodecZstd` to be loaded (`using CodecZstd`); choose `compression = :none` to avoid it.

### Serve values

`serve_value_stream` installs the SVS routes on a server. The `resolve` callback maps an opaque resource key to the value to stream, or `nothing` if unknown. Any BEVE-serializable value works.

```julia
using RepeRPC, CodecZstd

server = Server("127.0.0.1", 9000)
serve_value_stream(server, resource -> resource == "telemetry" ? load_frame() : nothing)
listen(server; async = true)
```

Tune the producer with `StreamOpts` (none of these are on the wire, so producer and consumer interoperate regardless of how they are set):

```julia
opts = StreamOpts(;
    chunk_bytes   = 1024 * 1024,  # target chunk size (keep ≤ a few MiB for WebSocket)
    compression   = :zstd,        # :zstd (default) or :none
    zstd_level    = 3,
    session_depth = 4,            # bounded producer buffer — where backpressure lives
    max_sessions  = 256,          # cap on concurrent open sessions (rejects excess opens)
    idle_timeout  = 300.0)        # reclaim a session abandoned this many seconds (Inf disables)
serve_value_stream(server, resolve; opts = opts)
```

A producer session is bound to the connection that opened it: a dropped connection frees its sessions immediately, and `cancel` or completing the pull frees them at once; `idle_timeout` is the backstop for a connection left open with its pull abandoned.

### Serve files (no reserialization)

When the value already lives on disk as a `.beve` / `.beve.zst` file (a finished collection, a cached artifact), `serve_value_stream`'s resolver would have to deserialize the file into a struct only to serialize it again as it streams. `serve_file_stream` skips that round trip: the `resolve` callback returns a `FileResource` (or a bare path `String`), and the producer streams the file's bytes straight into the chunk stream. It installs the **same** SVS routes — a server carries one SVS producer, so a file stream and a value stream are mutually exclusive on one server (last registration wins) — and the consumer side (`pull_value`, `pull_to_beve_file`, `pull_to_beve_zst_file`, `pull_to_file`) is unchanged.

```julia
using RepeRPC, CodecZstd

server = Server("127.0.0.1", 9000)
# Stream whichever file the resource maps to, verbatim.
serve_file_stream(server, resource -> isfile(resource) ? FileResource(resource) : nothing)
listen(server; async = true)
```

The producer reconciles the file's **stored** compression with the stream's **wire** compression (`StreamOpts.compression`):

- **stored == wire** (a `.beve.zst` file on a `:zstd` stream, or a `.beve` file on a `:none` stream): pure passthrough — no codec, no BEVE pass. This is the cheapest mode and the reason the entry point exists.
- **stored `:none`, wire `:zstd`**: the `.beve` file is zstd-compressed on the fly (still no BEVE pass), so a plain file is delivered compressed on the wire.
- **stored `:zstd`, wire `:none`**: the `.beve.zst` file is decompressed on the fly, so it can be served to a consumer without `CodecZstd`.

`FileResource(path; format = BODY_BEVE, compression = :auto)` controls the advertised `format` and the stored compression; `:auto` infers `:zstd` from a `.zst` suffix, else `:none`. The two transcoding directions need `CodecZstd` on the server; an `open` that would require it without it returns an error rather than failing mid-stream.

### Pull values

```julia
client = Client("127.0.0.1", 9000)
connect(client)

# Stream-decode into a generic value (Dict/Vector/scalar tree). Peak memory is the
# result tree plus one chunk — the compressed and decompressed buffers are never
# materialized.
value = pull_value(client, "telemetry")

# Decode into a struct type. NOTE: this path materializes the decompressed BEVE
# bytes before constructing T, so it is for small payloads only (a streaming typed
# decoder is pending upstream in BeveFormat.jl). Use the generic form or a file mode at scale.
frame = pull_value(Frame, client, "telemetry")
```

Or write the stream straight to disk, fully memory-bounded regardless of payload size and **written atomically** (a temp file is renamed onto the destination only after the terminal chunk arrives, so a dropped connection never leaves a truncated file that looks complete):

```julia
# Decompress and write a .beve file (requires a compressed, BEVE-formatted producer).
pull_to_beve_file(client, "telemetry", "telemetry.beve")

# Write the compressed bytes verbatim as a .beve.zst file (requires a compressed producer).
pull_to_beve_zst_file(client, "telemetry", "telemetry.beve.zst")

# Format-agnostic "download to a file": decompress if the stream is zstd, else write
# the bytes as-is. The general helper for an uncompressed (e.g. file-stream) producer.
pull_to_file(client, "telemetry", "telemetry.beve")
```

Each pull takes an optional `timeout` (the per-`next`-round-trip timeout; `nothing` uses the client default). The same calls work over a `WSClient`.

### Pull through a Fleet

The same pulls take a `Fleet` and a node name, targeting one node by name and wrapping the outcome in a `RemoteResult` (the same envelope as `call`/`stream`/`download`):

```julia
fleet = Fleet([NodeConfig("storage-1", 9000), NodeConfig("storage-2", 9000)])
connect!(fleet)

r = pull_value(fleet, "storage-1", "telemetry")           # RemoteResult{Any}
succeeded(r) && @show r[]["samples"]                      # r[] throws the captured error if it failed

rt = pull_value(Frame, fleet, "storage-1", "telemetry")   # RemoteResult{Frame}
rf = pull_to_beve_file(fleet, "storage-1", "telemetry", "telemetry.beve")  # RemoteResult{Nothing}
rz = pull_to_beve_zst_file(fleet, "storage-1", "telemetry", "telemetry.beve.zst")
rp = pull_to_file(fleet, "storage-1", "telemetry", "telemetry.beve")       # RemoteResult{Nothing}
```

Like the fleet's `stream`/`download`, an SVS pull is **not retried**: a multi-round-trip session that has already advanced (or written partial file bytes) must not be silently re-driven, so the fleet retry policy does not apply. Every failure is captured into the returned `RemoteResult` rather than thrown, including an unknown or removed `node_name` (a `KeyError`), so one bad node cannot abort a batch of pulls; call `r[]` to rethrow the miss. A successful file-mode pull carries `value === nothing`, so test the outcome with `succeeded`/`failed`, not the value.

### Connection teardown hook

SVS ties producer sessions to the life of the connection through a general server hook you can use in your own handlers. A handler that accepts a third argument receives a `ServerConnection`, and `on_close` registers cleanup to run exactly once when that connection ends (or immediately if it has already closed):

```julia
register(server, "/subscribe", function (params, request, conn)
    sub = open_subscription(params["topic"])
    on_close(conn, () -> close_subscription(sub))   # released when the client disconnects
    return ack(sub.id)
end)
```

Plain `(params, request)` handlers are unaffected.

## Registry

The `Registry` provides a convenient way to serve variables and functions using JSON Pointer syntax, matching the behavior of the Glaze C++ REPE registry. This allows you to expose a dictionary-like structure where:

- **Empty body** = READ the value at the path
- **Non-empty body + Function** = CALL the function with the body as arguments
- **Non-empty body + non-Function** = WRITE the value to the path

### Basic Registry Usage

```julia
using RepeRPC

# Create a registry with variables and functions
registry = Registry(
    "counter" => 0,
    "config" => Dict("timeout" => 30, "retries" => 3),
    "add" => (;a, b) -> a + b,           # kwargs-style function
    "multiply" => (x, y) -> x * y         # positional args (use array body)
)

# Serve the registry
server = Server("localhost", 8080)
serve(server, registry)
listen(server)
```

### Client Access

```julia
client = Client("localhost", 8080)
connect(client)

# READ a value (empty body)
counter = send_request(client, "/counter", nothing)  # Returns: 0

# READ nested value
timeout = send_request(client, "/config/timeout", nothing)  # Returns: 30

# WRITE a value (non-empty body to non-function)
send_request(client, "/counter", 42)  # Sets counter to 42

# CALL a function with kwargs (dict body)
result = send_request(client, "/add", Dict("a" => 10, "b" => 20))  # Returns: 30

# CALL a function with positional args (array body)
result = send_request(client, "/multiply", [4, 7])  # Returns: 28

disconnect(client)
```

### Building a Registry

```julia
# Create empty and add entries
registry = Registry()
registry["version"] = "1.0.0"
registry["debug"] = false

# Register at a path (creates nested structure)
register!(registry, "api/users/count", 0)

# Merge a dictionary at root
merge!(registry, Dict(
    "status" => "online",
    "uptime" => () -> time()
))

# Merge a dictionary at a specific path
merge!(registry, "/api/v2", Dict(
    "users" => Dict(
        "list" => () -> get_all_users(),
        "create" => (;name, email) -> create_user(name, email)
    ),
    "posts" => Dict(
        "list" => () -> get_all_posts(),
        "create" => (;title, body) -> create_post(title, body)
    )
))
# Now accessible as /api/v2/users/list, /api/v2/posts/create, etc.
```

### JSON Pointer Support

The registry uses [JSON Pointer (RFC 6901)](https://datatracker.ietf.org/doc/html/rfc6901) syntax for paths:

```julia
data = Dict(
    "users" => [
        Dict("name" => "Alice", "age" => 30),
        Dict("name" => "Bob", "age" => 25)
    ],
    "config" => Dict("timeout" => 30)
)
registry = Registry(data)

# Access nested values
resolve_json_pointer(registry, "/users/0/name")      # "Alice" (0-based array index)
resolve_json_pointer(registry, "/config/timeout")    # 30

# Set values
set_json_pointer!(registry, "/config/timeout", 60)
```

### Function Invocation

Functions in the registry are called based on how the body is structured:

| Body Type | Invocation Style | Example |
|-----------|------------------|---------|
| `Dict` | Keyword arguments | `(;a, b) -> a + b` called with `{"a": 5, "b": 3}` |
| `Array` | Positional arguments | `(x, y) -> x * y` called with `[4, 7]` |
| Empty `Dict`/`nothing` | No arguments | `() -> get_status()` |

```julia
registry = Registry(
    # Keyword args function
    "greet" => (;name, greeting="Hello") -> "$greeting, $name!",

    # Positional args function
    "sum" => (args...) -> sum(args),

    # No-arg function
    "timestamp" => () -> time()
)
```

### Extending a Served Registry

`serve()` returns the underlying `Registry`, so you can keep registering
endpoints after the server is wired up. Two distinct registration paths
coexist:

- `register!(registry, "/path", value)` adds an entry to the registry that
  `serve` is forwarding requests into.
- `register(server, "/path", handler)` installs a direct handler on the server
  that bypasses the registry entirely. With `fall_through=true`, direct
  handlers are invoked when the registry has no entry at the requested path.

```julia
server   = Server("localhost", 8080)
registry = serve(server, my_menu; fall_through=true)   # returns the Registry

# Add registry endpoints later
register!(registry, "/health", () -> "ok")

# Or splice in plain handlers that bypass the registry
register(server, "/admin/shutdown", function(params, request)
    @info "shutdown requested" by = get(params, "user", "?")
    return Dict("acknowledged" => true)
end)

listen(server)
```

The `fall_through` keyword controls whether unresolved registry paths are
passed on to subsequent middleware and direct handlers. The defaults are
asymmetric to preserve compatibility:

| `serve` overload | `fall_through` default | Notes |
|------------------|------------------------|-------|
| `serve(server, registry::Registry)` | `false` | Preserves legacy auto-create on unresolved top-level writes. |
| `serve(server, flat_registry::AbstractDict)` | `true` | Dynamic auto-create was never an expected mode for flat-registry servers. |
| `serve(server, menu::MenuBranch)` | `true` | Same as flat-registry. |

When `fall_through=true`, any middleware you `use(server, ...)` after `serve`
will run for requests the registry does not handle. Under `fall_through=false`
that middleware would have been dead code, so register it deliberately.

### Path Prefix

Use `path_prefix` in `serve()` to strip a prefix from incoming requests:

```julia
registry = Registry("users" => [...], "posts" => [...])
server = Server("localhost", 8080)
serve(server, registry; path_prefix="/api/v1")
listen(server)

# Client requests /api/v1/users -> resolves to /users in registry
```

## Glaze C++ Interoperability

RepeRPC.jl is fully compatible with the C++ Glaze implementation. You can build and run a C++ REPE server using Glaze:

### Building the C++ Server

```bash
cd cpp_server
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

### Running Integration Tests

```bash
# Start the C++ server
./cpp_server/build/repe_server 8081

# In another terminal, run Julia client
julia --project=. examples/glaze_interop.jl client localhost 8081
```

### Example C++ Server with Glaze

The repository includes a complete C++ REPE server implementation using Glaze's native REPE support (`glaze/rpc/repe/repe.hpp`). The server demonstrates:
- Binary REPE protocol handling
- JSON message parsing with Glaze
- Error handling and exception mapping
- Multiple RPC methods (add, multiply, divide, echo, status)

```julia
# Connect to Glaze C++ server
client = Client("localhost", 8081)
connect(client)

# Call C++ methods from Julia
result = send_request(client, "/add",
                     Dict("a" => 10.0, "b" => 20.0),
                     body_format = RepeRPC.JSON)
println(result["result"])  # 30.0

# The C++ divide function properly throws exceptions for errors
try
    send_request(client, "/divide",
                Dict("numerator" => 1.0, "denominator" => 0.0),
                body_format = RepeRPC.JSON)
catch e
    println("Caught division by zero error")
end
```

See `examples/glaze_interop.jl` and `test/run_glaze_test.sh` for complete examples.

## BEVE Binary Format Support

RepeRPC.jl supports [BEVE (Binary Efficient Versatile Encoding)](https://github.com/beve-org/beve), a compact binary serialization format that can be more efficient than JSON for certain types of data, via [BeveFormat.jl](https://github.com/matrix-research-inc/BeveFormat.jl).

### When to Use BEVE

BEVE is typically better for:
- Structured data with numeric values
- Arrays and matrices  
- High-frequency message passing
- Applications requiring compact binary encoding
- Interoperability with C++ BEVE libraries

JSON remains better for:
- Human-readable debugging
- Web API compatibility
- Simple text-heavy data
- Schema-less data structures

### BEVE Usage Example

```julia
using RepeRPC

# Complex structured data
sensor_data = Dict(
    "timestamp" => 1641038400,
    "readings" => [
        Dict("sensor_id" => "temp_01", "value" => 23.5),
        Dict("sensor_id" => "humidity_01", "value" => 45.2),
        Dict("sensor_id" => "pressure_01", "value" => 1013.25)
    ],
    "metadata" => Dict(
        "location" => Dict("lat" => 37.7749, "lon" => -122.4194),
        "device_id" => "weather_station_001"
    )
)

# Send with BEVE encoding
client = Client("localhost", 8080)
connect(client)

result = send_request(client, "/sensors/upload",
                     sensor_data,
                     body_format = RepeRPC.BEVE)

disconnect(client)
```

### Format Comparison

You can test the efficiency of different formats:

```julia
data = Dict("matrix" => [[1,2,3], [4,5,6]], "numbers" => collect(1:100))

# Compare sizes
beve_size = length(encode_body(data, RepeRPC.BEVE))
json_size = length(encode_body(data, RepeRPC.JSON))

println("BEVE: $beve_size bytes")
println("JSON: $json_size bytes")
```

See `examples/beve_demo.jl` for comprehensive BEVE examples and performance comparisons.

## WebSocket Support

RepeRPC.jl supports WebSocket transport for browser-based clients. Each WebSocket binary frame carries a raw REPE message with zero translation overhead. Load HTTP.jl to activate the extension:

```julia
using RepeRPC
import HTTP

counter = Ref(0)

server = WSServer("0.0.0.0", 8080;
    html = read("index.html", String))  # optional: serve a web page

serve(server, Dict{String, Any}(
    "/counter"   => (val...) -> (isempty(val) ? counter[] : counter[] = Int(first(val))),
    "/increment" => () -> (counter[] += 1),
    "/add"       => (;a, b) -> a + b,
))

listen(server)
```

```julia
# Julia WebSocket client
client = WSClient("localhost", 8080)
connect(client)

result = send_request(client, "/add", Dict("a" => 10, "b" => 20))
println(result)  # 30

disconnect(client)
```

For complete documentation including the browser JavaScript client, HTML serving, and the interactive demo, see [docs/WebSocket.md](docs/WebSocket.md).

### Secure Deployment (WSS + Bearer Auth)

WebSocket servers can terminate TLS natively and require a bearer token on
every upgrade request. No reverse proxy or VPN is required.

#### Server with TLS and bearer token

```julia
using RepeRPC
import HTTP
using Reseau   # HTTP.jl's TLS layer; add it to your project: ] add Reseau

tls = Reseau.TLS.Config(cert_file = "server.crt", key_file = "server.key")  # PEM files

server = WSServer("0.0.0.0", 8443;
    tls_config = tls,
    auth_token = ENV["REPE_TOKEN"])

RepeRPC.register(server, "/status", (params, request) -> Dict("ok" => true))
listen(server; async = true)
```

The server returns `401 Unauthorized` (with `WWW-Authenticate: Bearer`) for
any request missing or mismatching the token, and closes the connection
before the WebSocket upgrade. Token comparison is constant-time.

#### Client over WSS

```julia
client = WSClient("wss://data-1.example.com:8443/repe";
    auth_token = ENV["REPE_TOKEN"])
connect(client)

result = send_request(client, "/status", nothing)
disconnect(client)
```

For development with a self-signed certificate, set
`require_ssl_verification = false`. Use a real CA-issued cert (or a custom
`tls_config = Reseau.TLS.Config(ca_file = "ca.crt")` with your CA bundle) in
production.

#### Custom auth schemes (`auth_verifier`)

For per-method authorization, scope checks, IP allow-lists, or non-bearer
schemes, pass an `auth_verifier` callback to `WSServer`:

```julia
server = WSServer("0.0.0.0", 8443;
    tls_config    = tls,
    auth_verifier = function(http)
        # http.message is the HTTP.Request. HTTP.header is case-insensitive
        # and returns the empty string when the header is missing.
        return HTTP.header(http.message, "X-API-Key", "") == ENV["REPE_API_KEY"]
    end)
```

The verifier runs before the upgrade. It is treated as failure if it returns
anything other than `true` (including throwing). When `auth_verifier` is set
it takes precedence over `auth_token`.

#### Fleet over WSS

`NodeConfig` accepts a URL-style constructor that selects WebSocket transport
and carries auth credentials through to each node's client:

```julia
configs = [
    NodeConfig("wss://data-1.example.com:8443/repe";
        name = "data-1", auth_token = ENV["REPE_TOKEN"], tags = ["team"]),
    NodeConfig("wss://data-2.example.com:8443/repe";
        name = "data-2", auth_token = ENV["REPE_TOKEN"], tags = ["team"]),
]

fleet = Fleet(configs)
connect!(fleet)
results = broadcast(fleet, "/status", nothing)
disconnect!(fleet)
```

TCP and WebSocket nodes can coexist in the same fleet. `Node.client` is typed
as the abstract `AbstractClient`, so `broadcast`, `call`, `map_reduce`, and
`health_check` work uniformly across transports.

For the full secure deployment guide (native TLS server, running a private
CA for closed networks, loading the CA root into the client, and the
development self-signed shortcut), see
[docs/WebSocket.md](docs/WebSocket.md#secure-deployment).

## UniUDP - Unidirectional UDP Support

RepeRPC.jl includes built-in support for sending REPE messages over unidirectional UDP links with configurable redundancy and forward error correction (FEC). This is useful for:

- One-way satellite or radio links
- Sensor networks with asymmetric connectivity
- Broadcast/multicast scenarios
- High-throughput logging where acknowledgments aren't needed

### UniUDP Client

```julia
using RepeRPC

client = UniUDPClient(ip"192.168.1.100", 5000;
    redundancy = 2,       # Send each chunk twice
    chunk_size = 1024,    # Bytes per chunk
    fec_group_size = 4    # XOR parity every 4 chunks
)

# Fire-and-forget notification
send_notify(client, "/sensor/temperature", Dict("value" => 23.5))

close(client)
```

### UniUDP Server

```julia
using RepeRPC

server = UniUDPServer(5000;
    response_callback = (method, result, msg) -> println("Result: $result")
)

register(server, "/compute/square") do params, msg
    return params["x"]^2
end

listen(server)
```

For complete documentation including the packet format, FEC configuration, and raw protocol access, see [docs/UniUDP.md](docs/UniUDP.md).

## Fleet API - Multi-Server Control

RepeRPC.jl includes a Fleet API for managing and communicating with multiple servers through a unified interface. This supports both TCP (`Fleet`) for bidirectional communication and UDP (`UniUDPFleet`) for fire-and-forget broadcasting.

### TCP Fleet

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

# Broadcast to all compute nodes
results = broadcast(fleet, "/compute", Dict("value" => 42); tags=["compute"])

for (name, result) in results
    if succeeded(result)
        println("$name: $(result.value)")
    end
end

# Aggregate results with map_reduce
total = map_reduce(fleet, "/compute", Dict("value" => 10); tags=["compute"]) do results
    sum(r.value["result"] for r in results if succeeded(r))
end

disconnect!(fleet)
```

### UniUDP Fleet

```julia
using RepeRPC

# Define UniUDP fleet for sensor network
config = [
    UniUDPNodeConfig("gateway-1.local", 5000; tags=["primary"]),
    UniUDPNodeConfig("gateway-2.local", 5000; tags=["backup"]),
]

fleet = UniUDPFleet(config)

# Fire-and-forget broadcast to all nodes
results = send_notify(fleet, "/sensor/reading", Dict("value" => 23.5))

# Filter by tag
results = send_notify(fleet, "/alert", Dict("level" => "warning"); tags=["primary"])

close(fleet)
```

For complete documentation including tag-based filtering, health monitoring, dynamic node management, and mixed fleet patterns, see [docs/Fleet.md](docs/Fleet.md).

## ReplTree Integration

RepeRPC.jl optionally integrates with [ReplTree.jl](https://github.com/matrix-research-inc/ReplTree.jl) to serve interactive REPL menu hierarchies over the network. Load `ReplTree` alongside `RepeRPC` to activate the extension automatically.

### Flat Registry

Serve a `Dict{String, Any}` whose keys are JSON Pointer paths — no ReplTree dependency needed:

```julia
using RepeRPC

flat_registry = Dict{String, Any}(
    "/name"           => () -> "My Kitchen",
    "/stove/cook/add" => (food) -> "Cooking $food",
    "/math/add"       => (;a, b) -> a + b,
    "/counter"        => 0
)

server = Server("localhost", 8080)
serve(server, flat_registry)
listen(server)
```

By default, callable endpoints are invoked when accessed with an empty body (`call_on_read=true`).

### Mutable Struct Write-Through

Register mutable structs so that writes through REPE update the original object in place, keeping closures that captured the struct in sync:

```julia
mutable struct SensorConfig
    temperature::Int
    label::String
end

config = SensorConfig(72, "main")
flat_registry = Dict{String, Any}(
    "/name"     => () -> "Sensor App",
    "/get_temp" => () -> config.temperature,
)

server = Server("localhost", 8080)
serve(server, flat_registry; structs=Dict("/config" => config))
listen(server)
```

### Remote Menu

With ReplTree loaded, `remote_menu` builds a client-side `MenuBranch` that transparently proxies calls to a remote server:

```julia
using RepeRPC, ReplTree

client = Client("localhost", 8080)
connect(client)

menu = remote_menu(client)
menu.name()                      # reads /name
menu.stove.cook.add("pasta")     # calls /stove/cook/add
menu.math.add(a=10, b=20)       # calls /math/add with kwargs
```

Pass a `Fleet` instead of a `Client` to broadcast every leaf across all matching nodes; each invocation returns the per-node `Dict{String, RemoteResult}`:

```julia
fleet = Fleet([NodeConfig("worker-1", 8080), NodeConfig("worker-2", 8080)])
connect!(fleet)

team = remote_menu(fleet)
team.run_sync()                       # broadcasts /run_sync to every worker
team.debug.show_times()               # ditto, returns per-node results
team_primary = remote_menu(fleet; tags=["primary"])
team_primary.run_sync()               # restricted to nodes tagged "primary"
```

For complete documentation including schema discovery, functor support, and struct registration details, see [docs/ReplTree.md](docs/ReplTree.md).

## Testing

### Run Julia Unit Tests

```bash
julia --project=. -e 'using Pkg; Pkg.test()'
```

All 650+ unit tests should pass, covering:
- Header serialization/deserialization
- Message encoding/decoding (JSON, UTF8, BEVE, binary)
- Client-server communication
- Concurrent/async operations
- Error handling
- BEVE binary format support
- Registry with JSON Pointer resolution
- UniUDP protocol (chunking, redundancy, FEC)
- UniUDP REPE integration
- Fleet API (TCP and UniUDP multi-server control)
- WebSocket transport (server, client, registry serving)

### Run C++ Integration Tests

```bash
cd test
./run_glaze_test.sh
```

This script automatically:
1. Starts the C++ REPE server
2. Runs Julia client tests against it
3. Verifies protocol compatibility
4. Cleans up the server process

## License

MIT License - See LICENSE file for details

## References

- [REPE Specification](https://github.com/repe-org/REPE)
- [Glaze C++ Library](https://github.com/stephenberry/glaze)
