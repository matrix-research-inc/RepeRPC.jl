# Client and Server Usage

Core usage of the TCP `Server` and `Client`. The same request API applies to the [WebSocket](WebSocket.md) client, and most of it to [UniUDP](UniUDP.md).

## Server

```julia
using RepeRPC

server = Server("localhost", 8080)

RepeRPC.register(server, "/api/add", function(params, request)
    return Dict("result" => params["a"] + params["b"])
end)

listen(server)               # blocking
listen(server; async=true)   # or non-blocking
stop(server)
```

Pass `print_stacktrace=true` when constructing the server to log stack traces alongside the standard error messages while debugging:

```julia
server = Server("localhost", 8080; print_stacktrace=true)
```

A handler that accepts a third argument receives the `ServerConnection`; see [Connection teardown hook](Streaming.md#connection-teardown-hook).

### Handler errors

A handler that throws `REPEError` sends its code and message to the client, which rethrows it as a `REPEError`. Any other exception becomes `EC_PARSE_ERROR` with the exception's `showerror` text, matching Glaze's C++ registry. Registry functions behave the same way.

```julia
RepeRPC.register(server, "/withdraw", function(params, request)
    params["amount"] > 100 && throw(REPEError(ErrorCode(4096), "limit exceeded"))
    return Dict("ok" => true)
end)
```

## Client

```julia
client = Client("localhost", 8080)
connect(client)

result = send_request(client, "/api/add", Dict("a" => 10, "b" => 20))
println(result["result"])  # 30

# Notification (no response expected)
send_notify(client, "/api/log", "Event occurred")

isconnected(client)
disconnect(client)
```

## Typed Responses

JSON or BEVE responses can be decoded directly into Julia structs. Pass the type (or a `result_type` keyword) to `send_request` or `send_request_async`.

```julia
struct SensorReading
    id::Int
    status::String
end

reading = send_request(SensorReading, client, "/api/sensor", Dict("id" => 42))

# Keyword form
reading = send_request(client, "/api/sensor", Dict("id" => 42); result_type = SensorReading)

# Async variant
reading = fetch(send_request_async(SensorReading, client, "/api/sensor", Dict("id" => 42)))
```

## Body Formats

```julia
send_request(client, "/api/data", Dict("key" => "value"); body_format = RepeRPC.JSON)
send_request(client, "/api/data", Dict("numbers" => [1, 2, 3]); body_format = RepeRPC.BEVE)
send_request(client, "/api/text", "Hello, World!"; body_format = RepeRPC.UTF8)
send_request(client, "/api/binary", UInt8[1, 2, 3]; body_format = RepeRPC.RAW_BINARY)
```

See [Protocol and Formats](Protocol.md) for format details and when to prefer BEVE over JSON.

## Middleware

Middleware runs before handlers. Return `nothing` to continue, or an `ErrorCode` to reject the request.

```julia
RepeRPC.use(server, function(request)
    println("Request: ", parse_query(request))
    return nothing
end)

RepeRPC.use(server, function(request)
    if !check_auth(request)      # your own function
        return ErrorCode(4096)   # application error codes start at 4096
    end
    return nothing
end)
```

## Timeouts

```julia
client = Client("localhost", 8080; timeout=10.0)            # default for all requests
send_request(client, "/api/slow", params; timeout=60.0)    # per-request override
```

## Async and Batch Requests

```julia
task = send_request_async(client, "/api/data", params)
result = fetch(task)

requests = [
    ("/api/method1", Dict("param" => 1)),
    ("/api/method2", Dict("param" => 2)),
]
results = await_batch(batch(client, requests; body_format = RepeRPC.JSON))
```

## Keepalive

A peer that disappears *without* closing the socket (power loss, a pulled cable, a firewall dropping the flow) sends no FIN, so the connection looks healthy and the reader waits forever. TCP keepalive detects it: a request then fails with `ConnectionError` (which `Fleet` retries) instead of an unhelpful `TimeoutError`, and a streamed wait with `timeout = nothing` stops hanging indefinitely.

`Server` has the same exposure in the other direction: a client that vanishes leaves its connection task parked in `eof`, holding a socket until the process runs out of file descriptors. Both sides take the same keyword.

```julia
Client("host", 8080)                      # default: probe after 15 s idle, dead at ~25 s
Client("host", 8080; keepalive = 60.0)    # longer grace for a flaky link
Client("host", 8080; keepalive = nothing) # disable probing
Server("0.0.0.0", 8080; keepalive = 60.0) # applied to each accepted connection
```

Probes are answered by the peer's kernel, so a long-running handler is unaffected: keepalive detects an unreachable *host*, not a busy one.

## `max_body_length`

Both TCP read paths (server and client) allocate from wire-controlled lengths, so a garbled or malicious header could otherwise force an unbounded allocation. The cap bounds both `query_length` and `body_length` and is checked before allocating; the default `nothing` (unlimited) still refuses a length past `typemax(Int)`.

```julia
Server("0.0.0.0", 8080; max_body_length = 8 * 1024^3)   # reject bodies over 8 GiB
Client("host", 8080;    max_body_length = 8 * 1024^3)
```

A server rejects an oversized request with `EC_INVALID_HEADER` and closes the connection; a client refuses an oversized response rather than allocating.

Over WebSockets the cap is **client-side only**, and it is a *post-receive* ceiling rather than a pre-allocation bound. HTTP.jl exposes no per-frame read, so by the time REPE sees the header the whole frame is already buffered; `WSClient(...; max_body_length = ...)` bounds what REPE parses and delivers, not what HTTP.jl buffers. `WSServer` takes no `max_body_length`. Inbound frames are capped by `max_frame_size` instead; see [Message Size](WebSocket.md#message-size). Use a TCP node where you need a hard pre-allocation bound.
