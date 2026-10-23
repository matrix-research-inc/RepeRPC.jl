# ReplTree Integration

RepeRPC.jl provides optional integration with [ReplTree.jl](https://github.com/matrix-research-inc/ReplTree.jl) for serving interactive REPL menu hierarchies over the network. Load `ReplTree` alongside `RepeRPC` to activate the extension — no explicit import is needed.

## Features

- Serve a `ReplTree.MenuBranch` hierarchy through a REPE server
- Flat registry support for `Dict{String, Any}` with JSON Pointer keys
- `call_on_read` mode: callable endpoints are invoked on empty-body reads
- Mutable struct registration with write-through that preserves object references
- Schema endpoint (`/__schema__`) for client-side discovery
- `remote_menu` builds a client-side `MenuBranch` that proxies calls to a remote server
- Functor (callable struct) support alongside plain functions

## Quick Start

### Server

```julia
using RepeRPC, ReplTree

menu = registry_to_menu(my_registry())

server = Server("localhost", 8080)
serve(server, menu)
listen(server)
```

### Client

```julia
using RepeRPC, ReplTree

client = Client("localhost", 8080)
connect(client)

menu = remote_menu(client)
# Interactive REPL access to remote endpoints:
# menu.name()              → reads /name
# menu.stove.cook.add("pasta") → calls /stove/cook/add with "pasta"
```

## Flat Registry

You can serve a plain `Dict{String, Any}` whose keys are JSON Pointer paths, without depending on ReplTree at all. This is the foundation that the ReplTree extension builds on.

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

By default, `call_on_read=true` for flat registries: callable endpoints are invoked when accessed with an empty request body. Data endpoints are read normally.

```julia
client = Client("localhost", 8080)
connect(client)

send_request(client, "/name", nothing)                         # "My Kitchen"
send_request(client, "/stove/cook/add", "pasta")               # "Cooking pasta"
send_request(client, "/math/add", Dict("a" => 10, "b" => 20)) # 30
send_request(client, "/counter", nothing)                      # 0
send_request(client, "/counter", 42)                           # writes 42

disconnect(client)
```

### call_on_read

The `call_on_read` flag controls what happens when a callable endpoint receives an empty body:

| `call_on_read` | Empty body + callable | Non-empty body + callable |
|----------------|----------------------|--------------------------|
| `false` (default for `Registry`) | Returns function info (`{"type": "function", ...}`) | Calls with body as arguments |
| `true` (default for flat registries) | Invokes with no arguments | Calls with body as arguments |

You can override the default for either `serve` method:

```julia
# Standard registry with call_on_read enabled
serve(server, registry; call_on_read=true)

# Flat registry with call_on_read disabled
serve(server, flat_registry; call_on_read=false)
```

## Mutable Struct Registration

When a flat registry contains closures that capture a mutable struct, you typically want writes through REPE to update the original struct in place so that those closures see the changes. The `structs` keyword argument handles this:

```julia
mutable struct SensorConfig
    temperature::Int
    label::String
end

config = SensorConfig(72, "main")

flat_registry = Dict{String, Any}(
    "/name"               => () -> "Sensor App",
    "/config/temperature" => () -> config.temperature,
    "/config/label"       => () -> config.label,
    "/get_temp"           => () -> config.temperature,
)

server = Server("localhost", 8080)
serve(server, flat_registry; structs=Dict("/config" => config))
listen(server)
```

When `structs` is provided:
1. Each struct is registered directly at its path in the underlying `Registry`
2. Flat registry entries whose paths fall under a struct path are skipped (the struct's fields are navigated via JSON Pointer instead)
3. Writes to the struct or its fields update the original object in place
4. Closures that captured the struct see the updated values

```julia
client = Client("localhost", 8080)
connect(client)

send_request(client, "/config/temperature", nothing)  # 72
send_request(client, "/config/temperature", 99)        # writes in place
# config.temperature is now 99

send_request(client, "/get_temp", nothing)             # 99 (closure sees update)

# Write multiple fields at once
send_request(client, "/config", Dict("temperature" => 50, "label" => "backup"))
# config.temperature == 50, config.label == "backup"

disconnect(client)
```

## Serving a MenuBranch (ReplTree Extension)

When `ReplTree` is loaded, you can pass a `MenuBranch` directly to `serve`. The extension converts it to a flat registry internally.

```julia
using RepeRPC, ReplTree

config = KitchenConfig(stove_elements=4, oven_bays=2)
menu = registry_to_menu(example_kitchen_registry())

server = Server("localhost", 8080)
serve(server, menu; structs=Dict("/config" => config))
listen(server)
```

This accepts the same keyword arguments as the flat registry method: `path_prefix`, `call_on_read`, and `structs`.

## Remote Menu (Client-Side)

`remote_menu` queries a server's `/__schema__` endpoint and builds a local `MenuBranch` where each leaf transparently proxies calls to the server:

```julia
using RepeRPC, ReplTree

client = Client("localhost", 8080)
connect(client)

menu = remote_menu(client)

# Every leaf is a proxy function:
menu.name()                      # → send_request(client, "/name", nothing)
menu.stove.cook.add("pasta")     # → send_request(client, "/stove/cook/add", "pasta")
menu.math.add(a=10, b=20)       # → send_request(client, "/math/add", {"a":10,"b":20})
```

The schema is fetched once at menu creation time. If the server's registry changes, call `remote_menu` again to rebuild.

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

## Schema Endpoint

Every served registry exposes a `/__schema__` endpoint that returns a flat `Dict{String, String}` mapping each path to its type:

```julia
client = Client("localhost", 8080)
connect(client)

schema = send_request(client, "/__schema__", nothing,
    query_format=QUERY_JSON_POINTER,
    body_format=BODY_JSON)
# Dict("/name" => "callable", "/counter" => "data", "/math/add" => "callable")
```

Navigable structs are expanded to their individual fields in the schema. For example, a struct with fields `timeout` and `name` registered at `/config` produces `/config/timeout => "data"` and `/config/name => "data"`.

## Functor Support

In addition to plain functions, REPE supports callable structs (functors) as registry endpoints:

```julia
struct Multiplier
    factor::Int
end
(m::Multiplier)(x) = x * m.factor
(m::Multiplier)() = m.factor

registry = Registry(
    "triple" => Multiplier(3)
)

server = Server("localhost", 8080)
serve(server, registry)
listen(server)
```

```julia
client = Client("localhost", 8080)
connect(client)

send_request(client, "/triple", 7)        # 21
send_request(client, "/triple", nothing)   # returns function info (or invokes if call_on_read=true)

disconnect(client)
```

Functors are detected by checking for callable methods, while excluding container types (`AbstractDict`, `AbstractVector`, `AbstractString`) and primitives (`Number`).

## API Reference

### Serve Methods

| Signature | Description |
|-----------|-------------|
| `serve(server, registry; path_prefix, call_on_read=false)` | Serve a `Registry` (existing behavior) |
| `serve(server, flat_registry::AbstractDict; path_prefix, call_on_read=true, structs)` | Serve a flat `Dict{String,Any}` registry |
| `serve(server, menu::MenuBranch; path_prefix, call_on_read=true, structs)` | Serve a ReplTree `MenuBranch` (requires ReplTree) |

### Client Functions

| Function | Description |
|----------|-------------|
| `remote_menu(client)` | Build a client-side `MenuBranch` mirroring the remote registry (requires ReplTree) |

### Keyword Arguments

| Keyword | Default | Description |
|---------|---------|-------------|
| `path_prefix` | `""` | Strip this prefix from incoming request paths |
| `call_on_read` | varies | Invoke callables on empty-body reads |
| `structs` | `Dict{String,Any}()` | Map of paths to mutable structs for write-through registration |
