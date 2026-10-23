# RepeRPC.jl

Julia implementation of the [REPE (Remote Efficient Protocol Extension)](https://github.com/repe-org/REPE) RPC specification. REPE is a fast and simple binary RPC protocol that supports flexible data formats and query specifications.

## Features

- Full implementation of the [REPE v1 specification](https://github.com/repe-org/REPE), interoperable with the C++ [Glaze](https://github.com/stephenberry/glaze) implementation
- JSON, BEVE, UTF-8, and raw binary bodies, with typed decoding into Julia structs
- Asynchronous client/server with notifications, batching, and middleware
- **Registry** for serving variables and functions with JSON Pointer paths (Glaze-compatible)
- **WebSocket** transport for browser clients, with native TLS and bearer auth
- **Streaming** of multi-GB bodies and serialized values with bounded memory
- **UniUDP** transport for unidirectional UDP links with redundancy and FEC
- **Fleet API** for controlling many servers over TCP, WebSocket, or UniUDP
- **ReplTree integration** for serving interactive REPL menus over the network

## Installation

Requires Julia 1.12 or higher.

```julia
using Pkg
Pkg.add("RepeRPC")
```

## Quick Start

```julia
using RepeRPC

server = Server("localhost", 8080)

RepeRPC.register(server, "/api/add", function(params, request)
    return Dict("result" => params["a"] + params["b"])
end)

listen(server; async=true)
```

```julia
client = Client("localhost", 8080)
connect(client)

result = send_request(client, "/api/add", Dict("a" => 10, "b" => 20))
println(result["result"])  # 30

send_notify(client, "/api/log", "Event occurred")  # no response expected

disconnect(client)
```

Or serve variables and functions directly with a `Registry`:

```julia
registry = Registry(
    "counter" => 0,
    "add" => (;a, b) -> a + b,
)
serve(server, registry)

send_request(client, "/counter", nothing)                 # read: 0
send_request(client, "/counter", 42)                      # write
send_request(client, "/add", Dict("a" => 10, "b" => 20))  # call: 30
```

## Documentation

| Topic | Description |
|-------|-------------|
| [Usage](docs/Usage.md) | Server and client, typed responses, body formats, middleware, timeouts, async/batch, keepalive, `max_body_length` |
| [Protocol and Formats](docs/Protocol.md) | Header layout, query/body formats, error codes, BEVE |
| [Registry](docs/Registry.md) | JSON Pointer registries, function invocation, extending a served registry, path prefixes |
| [Streaming](docs/Streaming.md) | Streaming large bodies, serialized value streams (SVS), connection teardown hook |
| [WebSocket](docs/WebSocket.md) | WebSocket server and client, browser JavaScript client, HTML serving, TLS and auth |
| [UniUDP](docs/UniUDP.md) | REPE over unidirectional UDP, packet format, FEC |
| [Fleet](docs/Fleet.md) | Multi-server control over TCP, WebSocket, and UniUDP |
| [ReplTree](docs/ReplTree.md) | Flat registries, mutable struct write-through, remote REPL menus |
| [Glaze C++ Interop](docs/GlazeInterop.md) | Building the bundled C++ server and running integration tests |

Runnable examples are in [`examples/`](examples).

## Testing

```bash
julia --project=. -e 'using Pkg; Pkg.test()'
```

For C++ integration tests against Glaze, see [docs/GlazeInterop.md](docs/GlazeInterop.md#integration-tests).

## License

MIT License. See [LICENSE](LICENSE) for details.
