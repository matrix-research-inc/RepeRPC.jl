# Streaming

RepeRPC.jl has two ways to move payloads too large to hold comfortably in memory:

- **[Streaming large bodies](#streaming-large-bodies)**: a body whose length is known up front (a file, a sized `IO`) flows socket ↔ `IO` in fixed-size chunks over the TCP `Client`/`Server`.
- **[Streaming serialized values (SVS)](#streaming-serialized-values-svs)**: an in-memory value (or a `.beve`/`.beve.zst` file) is BEVE-serialized and optionally zstd-compressed as the client pulls it, one chunk per round trip, over TCP or WebSocket.

## Streaming Large Bodies

A streamed body never materializes in memory: peak memory is bounded by the chunk size regardless of payload size. The wire format is unchanged: a streamed message is byte-for-byte identical to the equivalent non-streamed one, so streamed and non-streamed peers (including Glaze C++) interoperate freely in either direction.

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

`StreamBody` requires the length up front (REPE is a prefix-length protocol, so there is no chunked transfer encoding). A source whose size is unknown until fully produced must be sized or staged first, or served with [SVS](#streaming-serialized-values-svs).

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

The client never materializes the upload body. The server receives it normally, bounded by [`max_body_length`](Usage.md#max_body_length); a server-side zero-copy *receive* handler is out of scope.

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

A *serialized in-memory value* (a struct, a `Dict`, a numeric `Vector`) has no known length until it has been serialized and compressed, so it cannot use the prefix-length path above. **SVS** (Serialized Value Stream) fills that gap: the server BEVE-serializes the value and, optionally, zstd-compresses it *as the client pulls it*, one chunk per request/response round trip. Neither side materializes the full serialized or compressed copy: the producer holds only the value it already has plus small codec buffers, and the consumer holds one chunk plus its chosen output. Flow control is the request/response cadence itself (no ACK window, no unbounded queue), and it works identically over the TCP `Server`/`Client` and the WebSocket `WSServer`/`WSClient`.

The wire is the cross-language [REPE SVS contract](https://github.com/repe-org/REPE/blob/main/serialized-stream-protocol.md), so a RepeRPC.jl peer interoperates with peers in other languages. Direction is download-only: the server produces, the client pulls. Design rationale is recorded in [StreamingValues.md](StreamingValues.md).

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

When the value already lives on disk as a `.beve` / `.beve.zst` file, `serve_value_stream`'s resolver would have to deserialize the file only to serialize it again. `serve_file_stream` skips that round trip: the `resolve` callback returns a `FileResource` (or a bare path `String`), and the producer streams the file's bytes straight into the chunk stream. It installs the **same** SVS routes (a server carries one SVS producer, so a file stream and a value stream are mutually exclusive on one server; last registration wins), and the consumer side is unchanged.

```julia
using RepeRPC, CodecZstd

server = Server("127.0.0.1", 9000)
serve_file_stream(server, resource -> isfile(resource) ? FileResource(resource) : nothing)
listen(server; async = true)
```

The producer reconciles the file's **stored** compression with the stream's **wire** compression (`StreamOpts.compression`):

- **stored == wire** (a `.beve.zst` file on a `:zstd` stream, or a `.beve` file on a `:none` stream): pure passthrough, no codec and no BEVE pass. This is the cheapest mode.
- **stored `:none`, wire `:zstd`**: the `.beve` file is zstd-compressed on the fly.
- **stored `:zstd`, wire `:none`**: the `.beve.zst` file is decompressed on the fly, so it can be served to a consumer without `CodecZstd`.

`FileResource(path; format = BODY_BEVE, compression = :auto)` controls the advertised `format` and the stored compression; `:auto` infers `:zstd` from a `.zst` suffix, else `:none`. The two transcoding directions need `CodecZstd` on the server; an `open` that would require it without it returns an error rather than failing mid-stream.

### Pull values

```julia
client = Client("127.0.0.1", 9000)
connect(client)

# Stream-decode into a generic value (Dict/Vector/scalar tree).
value = pull_value(client, "telemetry")

# Stream-decode into a struct type.
frame = pull_value(Frame, client, "telemetry")
```

Both forms decode straight from the chunk stream: peak memory is the result plus one chunk, and the compressed and decompressed buffers are never materialized.

Or write the stream straight to disk, fully memory-bounded and **written atomically** (a temp file is renamed onto the destination only after the terminal chunk arrives, so a dropped connection never leaves a truncated file that looks complete):

```julia
# Decompress and write a .beve file (requires a compressed, BEVE-formatted producer).
pull_to_beve_file(client, "telemetry", "telemetry.beve")

# Write the compressed bytes verbatim as a .beve.zst file (requires a compressed producer).
pull_to_beve_zst_file(client, "telemetry", "telemetry.beve.zst")

# Format-agnostic: decompress if the stream is zstd, else write the bytes as-is.
pull_to_file(client, "telemetry", "telemetry.beve")
```

Each pull takes an optional `timeout` (the per-`next`-round-trip timeout; `nothing` uses the client default). The same calls work over a `WSClient`.

### Pull through a Fleet

The same pulls take a `Fleet` and a node name, wrapping the outcome in a `RemoteResult`:

```julia
fleet = Fleet([NodeConfig("storage-1", 9000), NodeConfig("storage-2", 9000)])
connect!(fleet)

r = pull_value(fleet, "storage-1", "telemetry")           # RemoteResult{Any}
succeeded(r) && @show r[]["samples"]                      # r[] throws the captured error if it failed

rt = pull_value(Frame, fleet, "storage-1", "telemetry")   # RemoteResult{Frame}
rf = pull_to_beve_file(fleet, "storage-1", "telemetry", "telemetry.beve")  # RemoteResult{Nothing}
rz = pull_to_beve_zst_file(fleet, "storage-1", "telemetry", "telemetry.beve.zst")
rp = pull_to_file(fleet, "storage-1", "telemetry", "telemetry.beve")
```

Like the fleet's `stream`/`download`, an SVS pull is **not retried**: a session that has already advanced (or written partial file bytes) must not be silently re-driven. Every failure is captured into the returned `RemoteResult` rather than thrown, including an unknown or removed `node_name` (a `KeyError`), so one bad node cannot abort a batch of pulls; call `r[]` to rethrow. A successful file-mode pull carries `value === nothing`, so test the outcome with `succeeded`/`failed`, not the value.

## Connection Teardown Hook

SVS ties producer sessions to the life of the connection through a general server hook you can use in your own handlers. A handler that accepts a third argument receives a `ServerConnection`, and `on_close` registers cleanup to run exactly once when that connection ends (or immediately if it has already closed):

```julia
register(server, "/subscribe", function (params, request, conn)
    sub = open_subscription(params["topic"])
    on_close(conn, () -> close_subscription(sub))   # released when the client disconnects
    return ack(sub.id)
end)
```

Plain `(params, request)` handlers are unaffected.
