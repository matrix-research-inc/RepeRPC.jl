module RepeRPCReplTreeExt

using RepeRPC
using ReplTree

"""
    RepeRPC.serve(server::RepeRPC.Server, menu::ReplTree.MenuBranch;
               path_prefix::String="",
               call_on_read::Bool=true,
               fall_through::Bool=true,
               structs::AbstractDict{<:AbstractString}=Dict{String,Any}()) -> Registry

Serve a ReplTree MenuBranch hierarchy through a REPE server. Returns the
constructed [`RepeRPC.Registry`](@ref) so that callers can keep registering
additional endpoints after `serve` has been wired up.

Converts the MenuBranch to a flat registry and serves all leaf endpoints.
Callable leaves are invoked when accessed with an empty body, data leaves are read.

When `fall_through=true` (the default), JSON pointers that the menu does not
expose are passed through to direct `register(server, ...)` handlers.

When `structs` is provided, each struct is registered directly at its path and
flat registry entries under those paths are skipped. This preserves mutable struct
references so that closures capturing the struct see writes made through REPE.

# Examples
```julia
using RepeRPC, ReplTree

config = KitchenConfig(stove_elements=4, oven_bays=2)
menu = registry_to_menu(example_kitchen_registry())
server = RepeRPC.Server("localhost", 8080)
registry = RepeRPC.serve(server, menu; structs=Dict("/config" => config))
RepeRPC.register!(registry, "/health", () -> "ok")
RepeRPC.listen(server)
```
"""
function RepeRPC.serve(server::RepeRPC.AbstractServer, menu::ReplTree.MenuBranch;
                    path_prefix::String="",
                    call_on_read::Bool=true,
                    fall_through::Bool=true,
                    structs::AbstractDict{<:AbstractString}=Dict{String,Any}())
    flat_registry = ReplTree.menu_to_registry(menu)
    return RepeRPC.serve(server, flat_registry;
                      path_prefix=path_prefix,
                      call_on_read=call_on_read,
                      fall_through=fall_through,
                      structs=structs)
end

"""
    RepeRPC.remote_menu(client::RepeRPC.Client) -> ReplTree.MenuBranch

Build a client-side ReplTree menu that mirrors a remote REPE registry.

Queries the server's `/__schema__` endpoint to discover all available paths,
then builds a MenuBranch where each leaf transparently proxies calls to the server.

# Examples
```julia
using RepeRPC, ReplTree

client = RepeRPC.Client("localhost", 8080)
RepeRPC.connect(client)

menu = RepeRPC.remote_menu(client)
# Now use the menu in the REPL:
# menu.name()        → calls server's /name endpoint
# menu.stove.cook.add("pasta") → calls server's /stove/cook/add with "pasta"
```
"""
# Map a leaf-proxy's (args, kwargs) into the params payload the registry
# function-invocation rules expect (see RepeRPC._invoke_function):
#   no args/kwargs  -> nothing (READ / no-arg call)
#   any kwargs      -> Dict{String,Any} for keyword-style invocation
#   one positional  -> the value itself
#   many positional -> Vector for positional invocation
function _proxy_params(args, kwargs)
    if isempty(args) && isempty(kwargs)
        return nothing
    elseif !isempty(kwargs)
        return Dict{String, Any}(string(k) => v for (k, v) in kwargs)
    elseif length(args) == 1
        return args[1]
    else
        return collect(args)
    end
end

function RepeRPC.remote_menu(client::RepeRPC.Client)
    schema = RepeRPC.send_request(client, "/__schema__", nothing,
        query_format=RepeRPC.QUERY_JSON_POINTER,
        body_format=RepeRPC.BODY_JSON)

    flat = Dict{String, Any}()
    for (pointer, _type) in schema
        p = String(pointer)
        flat[p] = (args...; kwargs...) -> begin
            params = _proxy_params(args, kwargs)
            return RepeRPC.send_request(client, p, params,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
        end
    end

    return ReplTree.registry_to_menu(flat)
end

"""
    RepeRPC.remote_menu(fleet::RepeRPC.Fleet; tags::Vector{String} = String[]) -> ReplTree.MenuBranch

Build a fleet-wide ReplTree menu where every leaf broadcasts across all
matching nodes via `RepeRPC.broadcast(fleet, ...)`. Each leaf invocation returns
the per-node `Dict{String, RepeRPC.RemoteResult}` so callers can inspect success
and failure on a node-by-node basis.

If `tags` is non-empty, leaves only broadcast to nodes whose tags include all
entries in `tags`. The schema is fetched from the first responsive node in
that same candidate set, going through `RepeRPC.call` so the fleet's retry and
timeout policy applies; if every candidate fails, an error is thrown.

# Examples
```julia
using RepeRPC, ReplTree

fleet = Fleet([NodeConfig("worker-1", 8080), NodeConfig("worker-2", 8080)])
connect!(fleet)

team = remote_menu(fleet)
team.run_sync()                 # broadcasts /run_sync to every node
team.debug.show_times()         # ditto, returns Dict{String, RemoteResult}
```

To target a single node, use the lower-level Fleet API:

    RepeRPC.call(fleet, "worker-1", "/run_sync")
"""
function RepeRPC.remote_menu(fleet::RepeRPC.Fleet; tags::Vector{String} = String[])
    live = RepeRPC.connected_nodes(fleet)
    candidates = if isempty(tags)
        live
    else
        tag_set = Set(tags)
        [n for n in live if tag_set ⊆ n.tags]
    end
    if isempty(candidates)
        if isempty(tags)
            error("Fleet has no connected nodes; call RepeRPC.connect!(fleet) first")
        else
            error("Fleet has no connected nodes matching tags $(tags); call RepeRPC.connect!(fleet) first")
        end
    end

    # Walk candidates so a transient drop on one node doesn't fail the whole
    # menu construction. Use RepeRPC.call so the fleet's retry/timeout policy
    # applies to the schema fetch itself.
    schema = nothing
    last_error = nothing
    for cand in candidates
        result = RepeRPC.call(fleet, cand.name, "/__schema__", nothing;
            query_format=RepeRPC.QUERY_JSON_POINTER,
            body_format=RepeRPC.BODY_JSON)
        if RepeRPC.succeeded(result)
            schema = result.value
            break
        end
        last_error = result.error
    end
    schema === nothing && error(
        "Failed to fetch /__schema__ from any candidate node ($(length(candidates)) tried); last error: $(last_error)")

    flat = Dict{String, Any}()
    for (pointer, _type) in schema
        p = String(pointer)
        flat[p] = (args...; kwargs...) -> begin
            params = _proxy_params(args, kwargs)
            return RepeRPC.broadcast(fleet, p, params;
                tags=tags,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
        end
    end

    return ReplTree.registry_to_menu(flat)
end

end # module
