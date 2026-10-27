"""
    Registry

A registry for serving variables and functions via REPE using JSON pointer syntax.

The registry allows:
- Reading variables via JSON pointer paths (empty body = read)
- Writing variables via JSON pointer paths (non-empty body = write)
- Calling functions with the body as input arguments

This matches the behavior of the Glaze C++ REPE registry.
"""
mutable struct Registry
    data::Dict{String, Any}

    function Registry()
        new(Dict{String, Any}())
    end

    function Registry(data::Dict{String, Any})
        new(data)
    end

    function Registry(pairs::Pair{String, <:Any}...)
        new(Dict{String, Any}(pairs...))
    end
end

# Allow indexing into the registry
Base.getindex(r::Registry, key::String) = r.data[key]
Base.setindex!(r::Registry, value, key::String) = (r.data[key] = value)
Base.haskey(r::Registry, key::String) = haskey(r.data, key)
Base.keys(r::Registry) = keys(r.data)
Base.values(r::Registry) = values(r.data)
Base.delete!(r::Registry, key::String) = delete!(r.data, key)
Base.length(r::Registry) = length(r.data)
Base.isempty(r::Registry) = isempty(r.data)

"""
    merge!(registry::Registry, dict::AbstractDict)
    merge!(registry::Registry, path::String, dict::AbstractDict)
    merge!(registry::Registry, pairs...)

Merge a dictionary or key-value pairs into the registry.

When a path is provided, the dictionary is merged at that path (creating nested
structure as needed). This allows registering an entire dictionary under a root path.

# Examples
```julia
registry = Registry("x" => 1)

# Merge at root
merge!(registry, Dict("y" => 2, "z" => 3))
merge!(registry, "a" => 10, "b" => 20)

# Merge at a specific path
api_handlers = Dict(
    "users" => Dict("list" => () -> [...]),
    "posts" => Dict("create" => (;title, body) -> ...)
)
merge!(registry, "/api", api_handlers)
# Now accessible as /api/users/list, /api/posts/create, etc.
```
"""
function Base.merge!(r::Registry, dict::AbstractDict)
    for (k, v) in dict
        r.data[string(k)] = v
    end
    return r
end

function Base.merge!(r::Registry, path::String, dict::AbstractDict)
    # Parse the path to get segments
    segments = parse_json_pointer("/" * lstrip(path, '/'))

    if isempty(segments)
        # Merging at root - same as merge!(r, dict)
        return merge!(r, dict)
    end

    # Navigate to or create the target location
    current = r.data
    for segment in segments
        if !haskey(current, segment)
            current[segment] = Dict{String, Any}()
        end
        target = current[segment]
        if !(target isa Dict)
            # Convert non-dict to dict if needed, or error
            current[segment] = Dict{String, Any}()
        end
        current = current[segment]
    end

    # Merge the dict entries at this location
    for (k, v) in dict
        current[string(k)] = v
    end

    return r
end

function Base.merge!(r::Registry, pairs::Pair...)
    for (k, v) in pairs
        r.data[string(k)] = v
    end
    return r
end

"""
    merge(registry::Registry, dict::AbstractDict)

Create a new registry by merging a dictionary into the existing registry.

# Examples
```julia
registry = Registry("x" => 1)
new_registry = merge(registry, Dict("y" => 2))
```
"""
function Base.merge(r::Registry, dict::AbstractDict)
    new_registry = Registry(copy(r.data))
    merge!(new_registry, dict)
    return new_registry
end

"""
    parse_json_pointer(pointer::String) -> Vector{String}

Parse a JSON pointer string (RFC 6901) into path segments.

# Examples
```julia
parse_json_pointer("/employees/0/name")  # Returns ["employees", "0", "name"]
parse_json_pointer("/")                   # Returns []
parse_json_pointer("")                    # Returns []
```
"""
function parse_json_pointer(pointer::String)::Vector{String}
    if isempty(pointer) || pointer == "/"
        return String[]
    end

    # JSON pointer must start with /
    if !startswith(pointer, "/")
        throw(ArgumentError("JSON pointer must start with '/' or be empty"))
    end

    # Remove leading slash and split by '/'
    segments = split(pointer[2:end], '/')

    # Unescape JSON pointer special characters (RFC 6901)
    # ~1 -> /  (must be done first)
    # ~0 -> ~
    return [replace(replace(String(segment), "~1" => "/"), "~0" => "~") for segment in segments]
end

"""
    resolve_json_pointer(obj, pointer::String)
    resolve_json_pointer(obj, segments::Vector{String})

Resolve a JSON pointer path against an object, returning the value at that path.

Supports:
- Dict access by key
- Array/Vector access by 0-based index
- Struct field access by name

# Examples
```julia
data = Dict("users" => [Dict("name" => "Alice"), Dict("name" => "Bob")])
resolve_json_pointer(data, "/users/0/name")  # Returns "Alice"
```
"""
function resolve_json_pointer(obj, pointer::String)
    segments = parse_json_pointer(pointer)
    return resolve_json_pointer(obj, segments)
end

function resolve_json_pointer(obj, segments::Vector{String})
    current = obj

    # For Registry, access the underlying data dict
    if current isa Registry
        current = current.data
    end

    # If segments is empty, return the data (for Registry this is the data dict, not the struct)
    if isempty(segments)
        return current
    end

    for segment in segments
        current = _access_segment(current, segment)
    end

    return current
end

"""
    _access_segment(obj, segment::String)

Access a single segment (key, index, or field) on an object.
"""
function _access_segment(obj, segment::String)
    if obj isa Dict
        # Try string key first, then symbol
        if haskey(obj, segment)
            return obj[segment]
        elseif haskey(obj, Symbol(segment))
            return obj[Symbol(segment)]
        else
            throw(KeyError("Key '$segment' not found in Dict"))
        end
    elseif obj isa AbstractVector || obj isa AbstractArray
        # JSON pointer uses 0-based indexing
        index = tryparse(Int, segment)
        if index === nothing
            throw(ArgumentError("Invalid array index '$segment'"))
        end
        julia_index = index + 1  # Convert to 1-based
        if julia_index < 1 || julia_index > length(obj)
            throw(BoundsError(obj, julia_index))
        end
        return obj[julia_index]
    elseif obj isa Registry
        return obj.data[segment]
    else
        # Try struct field access
        sym = Symbol(segment)
        if hasfield(typeof(obj), sym)
            return getfield(obj, sym)
        else
            throw(ArgumentError("Cannot access '$segment' on object of type $(typeof(obj))"))
        end
    end
end

"""
    set_json_pointer!(obj, pointer::String, value)
    set_json_pointer!(obj, segments::Vector{String}, value)

Set a value at the given JSON pointer path in an object.

# Examples
```julia
data = Dict("config" => Dict("timeout" => 30))
set_json_pointer!(data, "/config/timeout", 60)
```
"""
function set_json_pointer!(obj, pointer::String, value)
    segments = parse_json_pointer(pointer)
    return set_json_pointer!(obj, segments, value)
end

function set_json_pointer!(obj, segments::Vector{String}, value)
    if isempty(segments)
        throw(ArgumentError("Cannot set root object via JSON pointer"))
    end

    # Navigate to the parent of the target
    parent = obj

    # For Registry, access the underlying data dict
    if parent isa Registry
        parent = parent.data
    end

    for segment in segments[1:end-1]
        parent = _access_segment(parent, segment)
    end

    # Set the final value
    final_segment = segments[end]
    _set_segment!(parent, final_segment, value)

    return value
end

"""
    _update_mutable_struct!(obj, params)

Update fields of a mutable struct from a dict, preserving the original object reference.
Only updates fields that exist in both the struct and the dict.
"""
function _update_mutable_struct!(obj, params)
    for (k, v) in pairs(params)
        sym = Symbol(k)
        if hasfield(typeof(obj), sym)
            setfield!(obj, sym, convert(fieldtype(typeof(obj), sym), v))
        end
    end
end

"""
    _set_segment!(obj, segment::String, value)

Set a value at a single segment (key, index, or field) on an object.
"""
function _set_segment!(obj, segment::String, value)
    if obj isa Dict
        obj[segment] = value
    elseif obj isa AbstractVector || obj isa AbstractArray
        index = tryparse(Int, segment)
        if index === nothing
            throw(ArgumentError("Invalid array index '$segment'"))
        end
        julia_index = index + 1  # Convert to 1-based
        if julia_index < 1 || julia_index > length(obj)
            throw(BoundsError(obj, julia_index))
        end
        obj[julia_index] = value
    elseif obj isa Registry
        obj.data[segment] = value
    else
        # Try to set struct field - only works for mutable structs
        sym = Symbol(segment)
        if hasfield(typeof(obj), sym)
            if ismutabletype(typeof(obj))
                setfield!(obj, sym, value)
            else
                throw(ArgumentError("Cannot set field '$segment' on immutable struct $(typeof(obj))"))
            end
        else
            throw(ArgumentError("Cannot set '$segment' on object of type $(typeof(obj))"))
        end
    end
end

"""
    handle_registry_request(registry::Registry, request::Message;
                            call_on_read::Bool=false,
                            fall_through::Bool=false)::Union{Message, Nothing}

Process a REPE request against a registry.

Behavior:
- Empty body: READ the value at the JSON pointer path
- Non-empty body with Function at path: CALL the function with body as arguments
- Non-empty body with non-Function at path: WRITE the body value to that path

When `fall_through=true`, JSON pointers that do not resolve to an existing
registry entry return `nothing` for *both* reads and writes, instead of
returning an error response or (for writes) auto-creating an entry. Returning
`nothing` lets the REPE server pipeline continue past the registry middleware
and reach direct handlers added with `register(server, ...)`. With
`fall_through=false` (the default), unresolved reads return an error response
and unresolved writes to top-level pointers auto-create entries via
`set_json_pointer!`.

Resolution probes are scoped to `KeyError`, `ArgumentError`, and `BoundsError`,
which are what the JSON pointer helpers throw when a segment is missing or out
of bounds. Other exceptions (e.g., a bug in a registered struct's accessor)
propagate so they don't masquerade as "path not found."

This matches the Glaze C++ REPE registry semantics.
"""
function handle_registry_request(registry::Registry, request::Message;
                                 call_on_read::Bool=false,
                                 fall_through::Bool=false)::Union{Message, Nothing}
    pointer = parse_query(request)
    is_root = isempty(pointer) || pointer == "/"

    try
        body_empty = isempty(request.body)

        if body_empty
            # READ operation
            result = try
                resolve_json_pointer(registry, pointer)
            catch e
                e isa Union{KeyError, ArgumentError, BoundsError} || rethrow()
                fall_through && !is_root && return nothing
                rethrow()
            end

            if call_on_read && _is_callable(result)
                # ReplTree mode: invoke callable endpoints on read
                result = _invoke_function(result, nothing)
                return create_response(request, result)
            elseif result isa Function
                # Standard mode: return info about the function
                return create_response(request, Dict(
                    "type" => "function",
                    "path" => pointer
                ))
            end

            return create_response(request, result)
        else
            # Non-empty body - check what's at the path
            params = parse_body(request)

            # Resolve once. Missing entries either fall through or fall into the
            # WRITE branch below for legacy auto-create.
            target = try
                resolve_json_pointer(registry, pointer)
            catch e
                e isa Union{KeyError, ArgumentError, BoundsError} || rethrow()
                fall_through && !is_root && return nothing
                nothing
            end

            if _is_callable(target)
                # CALL the function with body as arguments
                result = _invoke_function(target, params)
                return create_response(request, result)
            else
                # WRITE operation - set the value at the path
                if is_root
                    # Writing to root - params must be a Dict to merge/replace
                    if params isa Dict
                        for (k, v) in params
                            registry.data[string(k)] = v
                        end
                    else
                        throw(ArgumentError("Cannot write non-Dict to registry root"))
                    end
                elseif target !== nothing && ismutabletype(typeof(target)) && _is_dict_like(params)
                    # Update mutable struct fields in place to preserve references
                    _update_mutable_struct!(target, params)
                else
                    set_json_pointer!(registry, pointer, params)
                end

                # Return the value now at the path
                written = resolve_json_pointer(registry, pointer)
                return create_response(request, written)
            end
        end

    catch e
        @error "Registry request error" exception = e
        return _exception_response(request, e)
    end
end

"""
    _is_callable(x) -> Bool

Check if a value is callable (a function or functor), excluding container and primitive types.
"""
function _is_callable(x)
    x isa Function && return true
    (x isa AbstractDict || x isa AbstractVector || x isa AbstractString || x isa Number) && return false
    try
        return length(methods(x)) > 0
    catch
        return false
    end
end

"""
    _invoke_function(f, params)

Invoke a callable (function or functor) with the given parameters.

Supports:
- Dict params: passed as keyword arguments or single dict argument
- Array params: passed as positional arguments
- Single value: passed as single argument
- nothing: called with no arguments
"""
function _invoke_function(f, params)
    if params === nothing
        return f()
    elseif _is_dict_like(params)
        # Check if dict is empty
        if isempty(params)
            return f()
        end
        # Try to call with keyword arguments first
        try
            kwargs = Dict{Symbol, Any}(Symbol(k) => v for (k, v) in pairs(params))
            return f(; kwargs...)
        catch e
            # If kwargs fails, try passing dict as single argument
            if e isa MethodError
                return f(params)
            end
            rethrow(e)
        end
    elseif params isa AbstractVector
        return f(params...)
    else
        return f(params)
    end
end

# Helper to check if something is dict-like (handles JSON.Object etc.)
function _is_dict_like(x)
    return x isa AbstractDict || (hasproperty(x, :keys) && hasproperty(x, :values) && applicable(pairs, x))
end

"""
    _build_registry_schema(registry::Registry) -> Dict{String,String}

Build a flat schema of all accessible endpoints in the registry.
Each key is a JSON Pointer path, each value is "callable" or "data".

Navigable structs and `AbstractDict`s are recursively expanded so that every
JSON-pointer-reachable leaf appears in the schema. The visited set is scoped
to the current call path (pushed on entry, removed on unwind), so a struct
that references itself (or two structs that reference each other) collapses
to a "data" leaf at the cycle point, while independent fan-in (the same
instance appearing under two unrelated parents) is still expanded under each
parent.

`AbstractVector`/`AbstractArray` fields are left as "data" leaves rather than
enumerated by index. Their length is dynamic and per-index entries would bloat
the schema; the JSON-pointer resolver still navigates into them at runtime.

Non-string dict keys are coerced via `string(key)` to match how struct
fieldnames are surfaced.
"""
function _build_registry_schema(registry::Registry)::Dict{String,String}
    schema = Dict{String,String}()
    _collect_schema!(schema, registry.data, "", IdDict{Any,Nothing}())
    return schema
end

function _collect_schema!(schema::Dict{String,String}, obj::AbstractDict, prefix::String,
                          visited::IdDict{Any,Nothing})
    if haskey(visited, obj)
        # Cycle on the current path; surface the dict as a data leaf so the
        # caller can still reach it via raw JSON pointer.
        schema[prefix] = "data"
        return
    end
    visited[obj] = nothing
    try
        for (key, value) in obj
            seg = string(key)
            path = isempty(prefix) ? "/" * seg : prefix * "/" * seg
            _collect_schema!(schema, value, path, visited)
        end
    finally
        delete!(visited, obj)
    end
end

function _collect_schema!(schema::Dict{String,String}, obj, prefix::String,
                          visited::IdDict{Any,Nothing})
    if _is_callable(obj)
        schema[prefix] = "callable"
    elseif _is_navigable_struct(obj)
        if haskey(visited, obj)
            schema[prefix] = "data"
            return
        end
        visited[obj] = nothing
        try
            for fname in fieldnames(typeof(obj))
                field_path = prefix * "/" * string(fname)
                _collect_schema!(schema, getfield(obj, fname), field_path, visited)
            end
        finally
            delete!(visited, obj)
        end
    else
        # Leaf: numbers, strings, symbols, vectors/arrays, nothing, etc.
        schema[prefix] = "data"
    end
end

"""
    _is_navigable_struct(obj) -> Bool

Check if an object is a user-defined struct whose fields can be navigated via JSON pointer.
"""
function _is_navigable_struct(obj)
    obj === nothing && return false
    obj isa Function && return false
    obj isa Number && return false
    obj isa AbstractString && return false
    obj isa Symbol && return false
    obj isa AbstractVector && return false
    obj isa AbstractArray && return false
    obj isa AbstractDict && return false
    return !isempty(fieldnames(typeof(obj)))
end

"""
    serve(server::Server, registry::Registry;
          path_prefix::String="",
          call_on_read::Bool=false,
          fall_through::Bool=false) -> Registry

Configure a Server to serve a Registry. Returns the `registry` so that callers
can keep registering additional endpoints after `serve` has been wired up.

All requests to the server will be handled by the registry using JSON pointer
semantics. The query field is treated as a JSON pointer into the registry.

# Arguments
- `server`: The REPE server to configure
- `registry`: The registry to serve

# Keyword Arguments
- `path_prefix`: Optional prefix to strip from queries (e.g., "/api")
- `call_on_read`: When true, invoke callable endpoints on empty-body reads
- `fall_through`: When true, unresolved JSON pointers return `nothing` from
  the registry middleware so direct handlers added with `register(server, ...)`
  (and any middleware registered after `serve`) can serve them. Defaults to
  `false` for the `Registry` overload to preserve the legacy auto-create on
  unresolved top-level writes; the flat-registry and `MenuBranch` overloads
  default to `true` since dynamic auto-create was never expected there.

Note: with `fall_through=true`, middleware that you `use(server, ...)` after
`serve(server, registry)` will run for requests the registry does not handle.
Register such middleware deliberately, since it would be dead code under the
default `false` behavior.

# Examples
```julia
registry = Registry(
    "config" => Dict("timeout" => 30, "retries" => 3),
    "add" => (a, b) -> a + b,
    "multiply" => (x, y) -> x * y
)

server = Server("localhost", 8080)
registry = serve(server, registry)
register!(registry, "/health", () -> "ok")   # add later, still served
listen(server)
```
"""
function serve(server::AbstractServer, registry::Registry;
               path_prefix::String="",
               call_on_read::Bool=false,
               fall_through::Bool=false)
    # Use middleware to intercept all requests
    use(server, function(request::Message)
        query = parse_query(request)

        # Determine effective query after prefix stripping
        effective_query = query
        if !isempty(path_prefix) && startswith(query, path_prefix)
            effective_query = query[length(path_prefix)+1:end]
            if isempty(effective_query)
                effective_query = "/"
            end
        end

        # Schema endpoint for client-side menu building
        if effective_query == "/__schema__"
            return create_response(request, _build_registry_schema(registry))
        end

        # Create modified request if prefix was stripped
        req = request
        if effective_query != query
            req = Message(
                id=request.header.id,
                query=effective_query,
                body=request.body,
                query_format=request.header.query_format,
                body_format=request.header.body_format,
                notify=request.header.notify != 0,
                ec=request.header.ec
            )
        end

        return handle_registry_request(registry, req;
                                       call_on_read=call_on_read,
                                       fall_through=fall_through)
    end)

    return registry
end

"""
    serve(server::Server, flat_registry::AbstractDict{<:AbstractString};
          path_prefix::String="",
          call_on_read::Bool=true,
          fall_through::Bool=true,
          structs::AbstractDict{<:AbstractString}=Dict{String,Any}()) -> Registry

Serve a flat registry (e.g., from ReplTree.jl) through a REPE server. Returns
the constructed [`Registry`](@ref) so that callers can keep registering
additional endpoints after `serve` has been wired up.

The flat registry maps JSON Pointer strings to values (functions, callables, or data).
Each pointer key becomes an endpoint accessible via the server.

By default, `call_on_read=true` so that callable endpoints are invoked when
accessed with an empty request body.

By default, `fall_through=true` so that paths not in the registry are passed
through to direct handlers added via `register(server, ...)`. Dynamic
auto-create on writes was never an expected mode for flat-registry/menu
servers, so the default differs from the `Registry` overload.

When `structs` is provided, each struct is registered directly at its path and
flat registry entries under those paths are skipped. This preserves mutable struct
references so that closures capturing the struct see writes made through REPE.
REPE's JSON pointer resolution navigates into struct fields automatically.

# Examples
```julia
config = MyConfig(timeout=30, retries=3)
flat_registry = Dict{String, Any}(
    "/name" => () -> "My App",
    "/config/timeout" => () -> config.timeout,
    "/config/retries" => () -> config.retries,
)

server = Server("localhost", 8080)
# Register config struct directly. Field closures are skipped.
registry = serve(server, flat_registry; structs=Dict("/config" => config))
register!(registry, "/health", () -> "ok")
listen(server)
```
"""
function serve(server::AbstractServer, flat_registry::AbstractDict{<:AbstractString};
               path_prefix::String="",
               call_on_read::Bool=true,
               fall_through::Bool=true,
               structs::AbstractDict{<:AbstractString}=Dict{String,Any}())
    registry = Registry()

    # Register structs first
    for (path, obj) in structs
        register!(registry, path, obj)
    end

    # Build normalized prefix set for filtering
    struct_prefixes = Set{String}()
    for path in keys(structs)
        push!(struct_prefixes, "/" * lstrip(String(path), '/'))
    end

    for (pointer, value) in flat_registry
        # Skip entries that fall under a struct path
        if !isempty(struct_prefixes)
            normalized = "/" * lstrip(String(pointer), '/')
            skip = false
            for prefix in struct_prefixes
                if normalized == prefix || startswith(normalized, prefix * "/")
                    skip = true
                    break
                end
            end
            skip && continue
        end
        register!(registry, pointer, value)
    end

    return serve(server, registry;
                 path_prefix=path_prefix,
                 call_on_read=call_on_read,
                 fall_through=fall_through)
end

"""
    register!(registry::Registry, path::String, value)

Register a value or function at the given path in the registry.

# Examples
```julia
registry = Registry()
register!(registry, "counter", 0)
register!(registry, "increment", () -> (registry["counter"] += 1; registry["counter"]))
register!(registry, "add", (a, b) -> a + b)
```
"""
function register!(registry::Registry, path::String, value)
    # Handle nested paths
    segments = parse_json_pointer("/" * lstrip(path, '/'))

    if isempty(segments)
        throw(ArgumentError("Cannot register at root path"))
    end

    if length(segments) == 1
        registry.data[segments[1]] = value
    else
        # Create nested structure if needed
        current = registry.data
        for segment in segments[1:end-1]
            if !haskey(current, segment)
                current[segment] = Dict{String, Any}()
            end
            current = current[segment]
        end
        current[segments[end]] = value
    end

    return registry
end

# Pretty printing
function Base.show(io::IO, r::Registry)
    n_entries = length(r.data)
    entry_text = n_entries == 1 ? "1 entry" : "$n_entries entries"
    print(io, "Registry($entry_text)")
end

function Base.show(io::IO, ::MIME"text/plain", r::Registry)
    println(io, "Registry with $(length(r.data)) entries:")
    for (k, v) in r.data
        if v isa Function
            println(io, "  /$k => <function>")
        elseif v isa Dict
            println(io, "  /$k => Dict($(length(v)) entries)")
        elseif v isa AbstractVector
            println(io, "  /$k => $(typeof(v))($(length(v)) elements)")
        else
            println(io, "  /$k => $v")
        end
    end
end
