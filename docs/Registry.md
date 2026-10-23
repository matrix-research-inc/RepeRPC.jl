# Registry

The `Registry` serves variables and functions using JSON Pointer paths, matching the behavior of the Glaze C++ REPE registry:

- **Empty body**: READ the value at the path
- **Non-empty body + Function**: CALL the function with the body as arguments
- **Non-empty body + non-Function**: WRITE the value to the path

For flat `Dict{String, Any}` registries, mutable struct write-through, and serving ReplTree menus, see [ReplTree.md](ReplTree.md).

## Basic Usage

```julia
using RepeRPC

registry = Registry(
    "counter" => 0,
    "config" => Dict("timeout" => 30, "retries" => 3),
    "add" => (;a, b) -> a + b,           # kwargs-style function
    "multiply" => (x, y) -> x * y         # positional args (use array body)
)

server = Server("localhost", 8080)
serve(server, registry)
listen(server)
```

## Client Access

```julia
client = Client("localhost", 8080)
connect(client)

send_request(client, "/counter", nothing)                 # READ: 0
send_request(client, "/config/timeout", nothing)          # READ nested: 30
send_request(client, "/counter", 42)                      # WRITE
send_request(client, "/add", Dict("a" => 10, "b" => 20))  # CALL with kwargs: 30
send_request(client, "/multiply", [4, 7])                 # CALL with positional args: 28

disconnect(client)
```

## Building a Registry

```julia
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

## JSON Pointer Support

Paths use [JSON Pointer (RFC 6901)](https://datatracker.ietf.org/doc/html/rfc6901) syntax:

```julia
data = Dict(
    "users" => [
        Dict("name" => "Alice", "age" => 30),
        Dict("name" => "Bob", "age" => 25)
    ],
    "config" => Dict("timeout" => 30)
)
registry = Registry(data)

resolve_json_pointer(registry, "/users/0/name")      # "Alice" (0-based array index)
resolve_json_pointer(registry, "/config/timeout")    # 30

set_json_pointer!(registry, "/config/timeout", 60)
```

## Function Invocation

Functions are called based on how the body is structured:

| Body Type | Invocation Style | Example |
|-----------|------------------|---------|
| `Dict` | Keyword arguments | `(;a, b) -> a + b` called with `{"a": 5, "b": 3}` |
| `Array` | Positional arguments | `(x, y) -> x * y` called with `[4, 7]` |
| Empty `Dict`/`nothing` | No arguments | `() -> get_status()` |

```julia
registry = Registry(
    "greet" => (;name, greeting="Hello") -> "$greeting, $name!",   # keyword args
    "sum" => (args...) -> sum(args),                               # positional args
    "timestamp" => () -> time()                                    # no args
)
```

## Extending a Served Registry

`serve()` returns the underlying `Registry`, so you can keep registering endpoints after the server is wired up. Two distinct registration paths coexist:

- `register!(registry, "/path", value)` adds an entry to the registry that `serve` is forwarding requests into.
- `register(server, "/path", handler)` installs a direct handler on the server that bypasses the registry entirely. With `fall_through=true`, direct handlers are invoked when the registry has no entry at the requested path.

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

The `fall_through` keyword controls whether unresolved registry paths are passed on to subsequent middleware and direct handlers. The defaults are asymmetric to preserve compatibility:

| `serve` overload | `fall_through` default | Notes |
|------------------|------------------------|-------|
| `serve(server, registry::Registry)` | `false` | Preserves legacy auto-create on unresolved top-level writes. |
| `serve(server, flat_registry::AbstractDict)` | `true` | Dynamic auto-create was never an expected mode for flat-registry servers. |
| `serve(server, menu::MenuBranch)` | `true` | Same as flat-registry. |

When `fall_through=true`, any middleware you `use(server, ...)` after `serve` will run for requests the registry does not handle. Under `fall_through=false` that middleware would have been dead code, so register it deliberately.

## Path Prefix

Use `path_prefix` in `serve()` to strip a prefix from incoming requests:

```julia
registry = Registry("users" => [...], "posts" => [...])
server = Server("localhost", 8080)
serve(server, registry; path_prefix="/api/v1")
listen(server)

# Client requests /api/v1/users -> resolves to /users in registry
```
