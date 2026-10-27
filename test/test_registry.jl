using Test
using RepeRPC

@testset "Registry" begin

    @testset "JSON Pointer Parsing" begin
        # Basic parsing
        @test RepeRPC.parse_json_pointer("") == String[]
        @test RepeRPC.parse_json_pointer("/") == String[]
        @test RepeRPC.parse_json_pointer("/foo") == ["foo"]
        @test RepeRPC.parse_json_pointer("/foo/bar") == ["foo", "bar"]
        @test RepeRPC.parse_json_pointer("/foo/0/bar") == ["foo", "0", "bar"]

        # Escape sequences (RFC 6901)
        @test RepeRPC.parse_json_pointer("/a~1b") == ["a/b"]      # ~1 -> /
        @test RepeRPC.parse_json_pointer("/m~0n") == ["m~n"]      # ~0 -> ~
        @test RepeRPC.parse_json_pointer("/a~0~1b") == ["a~/b"]   # Combined

        # Invalid pointer (no leading /)
        @test_throws ArgumentError RepeRPC.parse_json_pointer("foo")
    end

    @testset "JSON Pointer Resolution" begin
        data = Dict{String, Any}(
            "name" => "test",
            "config" => Dict{String, Any}(
                "timeout" => 30,
                "retries" => 3
            ),
            "users" => [
                Dict{String, Any}("name" => "Alice", "age" => 30),
                Dict{String, Any}("name" => "Bob", "age" => 25)
            ]
        )

        # Root access
        @test RepeRPC.resolve_json_pointer(data, "") === data
        @test RepeRPC.resolve_json_pointer(data, "/") === data

        # Simple key access
        @test RepeRPC.resolve_json_pointer(data, "/name") == "test"

        # Nested access
        @test RepeRPC.resolve_json_pointer(data, "/config/timeout") == 30

        # Array access (0-based indexing)
        @test RepeRPC.resolve_json_pointer(data, "/users/0/name") == "Alice"
        @test RepeRPC.resolve_json_pointer(data, "/users/1/age") == 25

        # Key not found
        @test_throws KeyError RepeRPC.resolve_json_pointer(data, "/nonexistent")

        # Index out of bounds
        @test_throws BoundsError RepeRPC.resolve_json_pointer(data, "/users/10")
    end

    @testset "JSON Pointer Setting" begin
        data = Dict{String, Any}(
            "name" => "test",
            "config" => Dict{String, Any}(
                "timeout" => 30
            ),
            "items" => [1, 2, 3]
        )

        # Set simple value
        RepeRPC.set_json_pointer!(data, "/name", "updated")
        @test data["name"] == "updated"

        # Set nested value
        RepeRPC.set_json_pointer!(data, "/config/timeout", 60)
        @test data["config"]["timeout"] == 60

        # Set array element (0-based)
        RepeRPC.set_json_pointer!(data, "/items/1", 20)
        @test data["items"][2] == 20  # Julia is 1-based

        # Add new key
        RepeRPC.set_json_pointer!(data, "/config/new_key", "value")
        @test data["config"]["new_key"] == "value"

        # Cannot set root
        @test_throws ArgumentError RepeRPC.set_json_pointer!(data, "", "bad")
    end

    @testset "Registry Creation" begin
        # Empty registry
        r1 = RepeRPC.Registry()
        @test length(collect(keys(r1))) == 0

        # Registry from dict
        r2 = RepeRPC.Registry(Dict{String, Any}("a" => 1, "b" => 2))
        @test r2["a"] == 1
        @test r2["b"] == 2

        # Registry from pairs
        r3 = RepeRPC.Registry("x" => 10, "y" => 20)
        @test r3["x"] == 10
        @test r3["y"] == 20
    end

    @testset "Registry Operations" begin
        r = RepeRPC.Registry()

        # Set and get
        r["counter"] = 0
        @test r["counter"] == 0

        # Check existence
        @test haskey(r, "counter")
        @test !haskey(r, "nonexistent")

        # Delete
        delete!(r, "counter")
        @test !haskey(r, "counter")

        # Length and isempty
        @test isempty(r)
        r["x"] = 1
        @test !isempty(r)
        @test length(r) == 1
    end

    @testset "Registry merge!" begin
        r = RepeRPC.Registry("x" => 1, "y" => 2)

        # Merge a Dict
        merge!(r, Dict("z" => 3, "w" => 4))
        @test r["x"] == 1
        @test r["y"] == 2
        @test r["z"] == 3
        @test r["w"] == 4
        @test length(r) == 4

        # Merge overwrites existing keys
        merge!(r, Dict("x" => 100))
        @test r["x"] == 100

        # Merge with pairs
        merge!(r, "a" => 10, "b" => 20)
        @test r["a"] == 10
        @test r["b"] == 20

        # Merge with Symbol keys (converted to String)
        merge!(r, Dict(:sym_key => "value"))
        @test r["sym_key"] == "value"

        # Merge functions
        merge!(r, Dict("add" => (;a, b) -> a + b))
        @test r["add"](a=2, b=3) == 5
    end

    @testset "Registry merge! with path" begin
        r = RepeRPC.Registry()

        # Merge dict at a path
        api_dict = Dict(
            "users" => Dict("count" => 42),
            "posts" => Dict("count" => 100)
        )
        merge!(r, "/api", api_dict)

        # Check structure was created
        @test haskey(r, "api")
        @test r["api"]["users"]["count"] == 42
        @test r["api"]["posts"]["count"] == 100

        # Access via JSON pointer
        @test RepeRPC.resolve_json_pointer(r, "/api/users/count") == 42
        @test RepeRPC.resolve_json_pointer(r, "/api/posts/count") == 100

        # Merge more at same path (should add to existing)
        merge!(r, "/api", Dict("config" => Dict("timeout" => 30)))
        @test r["api"]["config"]["timeout"] == 30
        @test r["api"]["users"]["count"] == 42  # Still exists

        # Merge at deeper path
        merge!(r, "/api/v2/endpoints", Dict("health" => "/health", "status" => "/status"))
        @test RepeRPC.resolve_json_pointer(r, "/api/v2/endpoints/health") == "/health"

        # Merge with leading slash or without
        r2 = RepeRPC.Registry()
        merge!(r2, "foo/bar", Dict("x" => 1))
        merge!(r2, "/foo/baz", Dict("y" => 2))
        @test RepeRPC.resolve_json_pointer(r2, "/foo/bar/x") == 1
        @test RepeRPC.resolve_json_pointer(r2, "/foo/baz/y") == 2

        # Merge at root path "/" is same as merge at root
        r3 = RepeRPC.Registry()
        merge!(r3, "/", Dict("root_key" => "root_value"))
        @test r3["root_key"] == "root_value"

        # Merge functions at a path
        r4 = RepeRPC.Registry()
        merge!(r4, "/math", Dict(
            "add" => (;a, b) -> a + b,
            "multiply" => (x, y) -> x * y
        ))
        @test r4["math"]["add"](a=2, b=3) == 5
        @test r4["math"]["multiply"](4, 5) == 20
    end

    @testset "Registry merge (non-mutating)" begin
        r1 = RepeRPC.Registry("x" => 1)
        r2 = merge(r1, Dict("y" => 2))

        # Original unchanged
        @test length(r1) == 1
        @test !haskey(r1, "y")

        # New registry has both
        @test r2["x"] == 1
        @test r2["y"] == 2
        @test length(r2) == 2
    end

    @testset "Registry register!" begin
        r = RepeRPC.Registry()

        # Register simple value
        RepeRPC.register!(r, "value", 42)
        @test r["value"] == 42

        # Register with leading slash
        RepeRPC.register!(r, "/other", "test")
        @test r["other"] == "test"

        # Register nested path
        RepeRPC.register!(r, "nested/deep/value", 100)
        @test r["nested"]["deep"]["value"] == 100

        # Register function
        RepeRPC.register!(r, "add", (a, b) -> a + b)
        @test r["add"](2, 3) == 5
    end

    @testset "Registry Request Handling - Read" begin
        r = RepeRPC.Registry(
            "counter" => 42,
            "config" => Dict{String, Any}("timeout" => 30),
            "items" => [1, 2, 3]
        )

        # Read root-level value (empty body)
        req = RepeRPC.Message(
            query="/counter",
            body=nothing,
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        )
        resp = RepeRPC.handle_registry_request(r, req)
        @test resp.header.ec == UInt32(RepeRPC.EC_OK)
        result = RepeRPC.parse_body(resp)
        @test result == 42

        # Read nested value
        req2 = RepeRPC.Message(
            query="/config/timeout",
            body=nothing,
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        )
        resp2 = RepeRPC.handle_registry_request(r, req2)
        result2 = RepeRPC.parse_body(resp2)
        @test result2 == 30

        # Read array element
        req3 = RepeRPC.Message(
            query="/items/0",
            body=nothing,
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        )
        resp3 = RepeRPC.handle_registry_request(r, req3)
        result3 = RepeRPC.parse_body(resp3)
        @test result3 == 1
    end

    @testset "Registry Request Handling - Write" begin
        r = RepeRPC.Registry(
            "counter" => 0,
            "config" => Dict{String, Any}("timeout" => 30)
        )

        # Write to existing path
        req = RepeRPC.Message(
            query="/counter",
            body=Dict("value" => 100),  # Body will be the value
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        )
        # Actually for write, the body IS the new value
        req = RepeRPC.Message(
            query="/counter",
            body=100,
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        )
        resp = RepeRPC.handle_registry_request(r, req)
        @test resp.header.ec == UInt32(RepeRPC.EC_OK)
        @test r["counter"] == 100

        # Write to nested path
        req2 = RepeRPC.Message(
            query="/config/timeout",
            body=60,
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        )
        resp2 = RepeRPC.handle_registry_request(r, req2)
        @test resp2.header.ec == UInt32(RepeRPC.EC_OK)
        @test r["config"]["timeout"] == 60
    end

    @testset "Registry Request Handling - Function Calls" begin
        r = RepeRPC.Registry()

        # Function with keyword args
        RepeRPC.register!(r, "add", (;a, b) -> a + b)

        # Call with dict args (kwargs style)
        req = RepeRPC.Message(
            query="/add",
            body=Dict("a" => 5, "b" => 3),
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        )
        resp = RepeRPC.handle_registry_request(r, req)
        @test resp.header.ec == UInt32(RepeRPC.EC_OK)
        result = RepeRPC.parse_body(resp)
        @test result == 8

        # Function with positional args (array input)
        RepeRPC.register!(r, "multiply", (x, y) -> x * y)
        req2 = RepeRPC.Message(
            query="/multiply",
            body=[4, 7],
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        )
        resp2 = RepeRPC.handle_registry_request(r, req2)
        result2 = RepeRPC.parse_body(resp2)
        @test result2 == 28

        # Function returning complex data (no args)
        RepeRPC.register!(r, "get_info", () -> Dict("name" => "test", "value" => 42))
        req3 = RepeRPC.Message(
            query="/get_info",
            body=nothing,  # Empty body to read function, returns function info
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        )
        resp3 = RepeRPC.handle_registry_request(r, req3)
        result3 = RepeRPC.parse_body(resp3)
        @test result3["type"] == "function"  # Reading a function returns info about it

        # Now call it with empty dict
        req4 = RepeRPC.Message(
            query="/get_info",
            body=Dict{String,Any}(),  # Empty dict triggers function call
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        )
        resp4 = RepeRPC.handle_registry_request(r, req4)
        result4 = RepeRPC.parse_body(resp4)
        @test result4["name"] == "test"
        @test result4["value"] == 42
    end

    @testset "Registry Request Handling - Function Errors" begin
        r = RepeRPC.Registry()
        RepeRPC.register!(r, "reject", x -> throw(RepeRPC.REPEError(RepeRPC.EC_INVALID_BODY, "rejected $x")))
        RepeRPC.register!(r, "fail", x -> error("failed $x"))

        call(path) = RepeRPC.handle_registry_request(r, RepeRPC.Message(
            query=path,
            body=[1],
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        ))

        # A thrown REPEError keeps its code and message
        resp = call("/reject")
        @test resp.header.ec == UInt32(RepeRPC.EC_INVALID_BODY)
        @test String(resp.body) == "rejected 1"

        # Any other exception is a parse error carrying its showerror text
        resp = call("/fail")
        @test resp.header.ec == UInt32(RepeRPC.EC_PARSE_ERROR)
        @test String(resp.body) == "failed 1"
    end

    @testset "Registry with Server Integration" begin
        r = RepeRPC.Registry(
            "counter" => 0,
            "add" => (;a, b) -> a + b  # Use kwargs for dict-based calls
        )

        server = RepeRPC.Server("localhost", 18090)
        RepeRPC.serve(server, r)

        # Start server async
        RepeRPC.listen(server, async=true)
        RepeRPC.wait_for_server("localhost", 18090)

        try
            client = RepeRPC.Client("localhost", 18090)
            RepeRPC.connect(client)

            # Read value
            result = RepeRPC.send_request(client, "/counter", nothing,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test result == 0

            # Write value — returns the written value
            result2 = RepeRPC.send_request(client, "/counter", 42,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test result2 == 42

            # Read back
            result3 = RepeRPC.send_request(client, "/counter", nothing,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test result3 == 42

            # Call function with kwargs
            result4 = RepeRPC.send_request(client, "/add", Dict("a" => 10, "b" => 20),
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test result4 == 30

            RepeRPC.disconnect(client)
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "_is_callable" begin
        # Functions are callable
        @test RepeRPC._is_callable(() -> 42) == true
        @test RepeRPC._is_callable((x) -> x + 1) == true
        @test RepeRPC._is_callable(sin) == true

        # Containers and primitives are not callable
        @test RepeRPC._is_callable(Dict("a" => 1)) == false
        @test RepeRPC._is_callable([1, 2, 3]) == false
        @test RepeRPC._is_callable("hello") == false
        @test RepeRPC._is_callable(42) == false
        @test RepeRPC._is_callable(3.14) == false
    end

    @testset "Callable Struct (Functor) Support" begin
        # Define a callable struct
        struct TestFunctor
            multiplier::Int
        end
        (f::TestFunctor)(x) = x * f.multiplier
        (f::TestFunctor)() = f.multiplier

        functor = TestFunctor(3)
        @test RepeRPC._is_callable(functor) == true

        # Test _invoke_function with functor
        @test RepeRPC._invoke_function(functor, nothing) == 3
        @test RepeRPC._invoke_function(functor, 7) == 21
    end

    @testset "handle_registry_request with call_on_read" begin
        r = RepeRPC.Registry()
        RepeRPC.register!(r, "name", () -> "TestApp")
        RepeRPC.register!(r, "add", (;a, b) -> a + b)
        RepeRPC.register!(r, "counter", 0)

        # Default behavior (call_on_read=false): empty body + function → info
        req_info = RepeRPC.Message(
            query="/name",
            body=nothing,
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        )
        resp_info = RepeRPC.handle_registry_request(r, req_info)
        result_info = RepeRPC.parse_body(resp_info)
        @test result_info["type"] == "function"

        # call_on_read=true: empty body + function → invoke it
        resp_call = RepeRPC.handle_registry_request(r, req_info; call_on_read=true)
        result_call = RepeRPC.parse_body(resp_call)
        @test result_call == "TestApp"

        # call_on_read=true: non-empty body + function → invoke with args (same as default)
        req_args = RepeRPC.Message(
            query="/add",
            body=Dict("a" => 5, "b" => 3),
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        )
        resp_args = RepeRPC.handle_registry_request(r, req_args; call_on_read=true)
        result_args = RepeRPC.parse_body(resp_args)
        @test result_args == 8

        # call_on_read=true: empty body + data → read (unchanged)
        req_data = RepeRPC.Message(
            query="/counter",
            body=nothing,
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        )
        resp_data = RepeRPC.handle_registry_request(r, req_data; call_on_read=true)
        result_data = RepeRPC.parse_body(resp_data)
        @test result_data == 0
    end

    @testset "Mutable Struct Write Preserves References" begin
        mutable struct DeviceConfig
            temperature::Int
            label::String
        end

        config = DeviceConfig(100, "sensor")
        # Closure captures config by reference
        get_temp = () -> config.temperature

        r = RepeRPC.Registry()
        RepeRPC.register!(r, "config", config)
        RepeRPC.register!(r, "get_temp", get_temp)

        # Write to mutable struct via dict — should update in place
        req_write = RepeRPC.Message(
            query="/config",
            body=Dict("temperature" => 200, "label" => "updated"),
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        )
        resp_write = RepeRPC.handle_registry_request(r, req_write)
        @test resp_write.header.ec == UInt32(RepeRPC.EC_OK)

        # The original config object should be updated in place
        @test config.temperature == 200
        @test config.label == "updated"

        # Closure that captured config should see the update
        req_call = RepeRPC.Message(
            query="/get_temp",
            body=Dict{String,Any}(),
            query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
            body_format=UInt16(RepeRPC.BODY_JSON)
        )
        resp_call = RepeRPC.handle_registry_request(r, req_call)
        result = RepeRPC.parse_body(resp_call)
        @test result == 200
    end

    @testset "Flat Registry Serving" begin
        # ReplTree-style flat registry with JSON pointer keys
        flat_registry = Dict{String, Any}(
            "/name" => () -> "My Kitchen",
            "/stove/cook/add" => (food) -> "Cooking $food",
            "/math/add" => (;a, b) -> a + b,
            "/counter" => 0
        )

        server = RepeRPC.Server("localhost", 18091)
        RepeRPC.serve(server, flat_registry)

        RepeRPC.listen(server, async=true)
        RepeRPC.wait_for_server("localhost", 18091)

        try
            client = RepeRPC.Client("localhost", 18091)
            RepeRPC.connect(client)

            # call_on_read is true by default: zero-arg callable invoked on empty body
            result_name = RepeRPC.send_request(client, "/name", nothing,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test result_name == "My Kitchen"

            # Callable with positional arg
            result_cook = RepeRPC.send_request(client, "/stove/cook/add", "pasta",
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test result_cook == "Cooking pasta"

            # Callable with keyword args
            result_add = RepeRPC.send_request(client, "/math/add", Dict("a" => 10, "b" => 20),
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test result_add == 30

            # Data leaf: read
            result_counter = RepeRPC.send_request(client, "/counter", nothing,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test result_counter == 0

            # Data leaf: write — returns the written value
            result_write = RepeRPC.send_request(client, "/counter", 42,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test result_write == 42

            # Read back written value
            result_read = RepeRPC.send_request(client, "/counter", nothing,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test result_read == 42

            RepeRPC.disconnect(client)
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "Structs kwarg preserves references" begin
        mutable struct SensorConfig
            temperature::Int
            label::String
        end

        config = SensorConfig(72, "main")

        # Flat registry has closures reading struct fields
        flat_registry = Dict{String, Any}(
            "/name" => () -> "Sensor App",
            "/config/temperature" => () -> config.temperature,
            "/config/label" => () -> config.label,
            "/get_temp" => () -> config.temperature,
        )

        server = RepeRPC.Server("localhost", 18092)
        # Register struct at /config — closures under /config/* are skipped
        RepeRPC.serve(server, flat_registry; structs=Dict("/config" => config))

        RepeRPC.listen(server, async=true)
        RepeRPC.wait_for_server("localhost", 18092)

        try
            client = RepeRPC.Client("localhost", 18092)
            RepeRPC.connect(client)

            # Non-struct endpoints still work
            result_name = RepeRPC.send_request(client, "/name", nothing,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test result_name == "Sensor App"

            # Read struct field via JSON pointer navigation
            result_temp = RepeRPC.send_request(client, "/config/temperature", nothing,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test result_temp == 72

            # Write to struct field — modifies the original object in place, returns written value
            result_write = RepeRPC.send_request(client, "/config/temperature", 99,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test result_write == 99

            # Original struct updated
            @test config.temperature == 99

            # Closure that captured the struct sees the update
            result_closure = RepeRPC.send_request(client, "/get_temp", nothing,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test result_closure == 99

            # Write entire struct via dict — updates fields in place, returns the struct
            result_struct_write = RepeRPC.send_request(client, "/config",
                Dict("temperature" => 50, "label" => "backup"),
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test result_struct_write["temperature"] == 50
            @test result_struct_write["label"] == "backup"
            @test config.temperature == 50
            @test config.label == "backup"

            RepeRPC.disconnect(client)
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "Schema Generation" begin
        r = RepeRPC.Registry()
        RepeRPC.register!(r, "name", () -> "App")
        RepeRPC.register!(r, "math/add", (;a, b) -> a + b)
        RepeRPC.register!(r, "counter", 0)

        schema = RepeRPC._build_registry_schema(r)

        @test schema["/name"] == "callable"
        @test schema["/math/add"] == "callable"
        @test schema["/counter"] == "data"
        @test length(schema) == 3
    end

    @testset "Schema with Navigable Struct" begin
        mutable struct SchemaTestConfig
            timeout::Int
            name::String
        end

        r = RepeRPC.Registry()
        RepeRPC.register!(r, "config", SchemaTestConfig(30, "test"))
        RepeRPC.register!(r, "action", () -> "done")

        schema = RepeRPC._build_registry_schema(r)

        # Struct fields expanded as individual endpoints
        @test schema["/config/timeout"] == "data"
        @test schema["/config/name"] == "data"
        @test schema["/action"] == "callable"
        # Struct itself is NOT a leaf (its fields are)
        @test !haskey(schema, "/config")
    end

    @testset "Schema with Nested Structs (recursive)" begin
        # Mirror the SyncMenu shape that motivated the recursion: a top-level
        # handle whose fields are themselves structs with primitive leaves.
        mutable struct InnerSyncCfg
            retry_delay_s::Float64
            comment::String
        end
        mutable struct OuterSyncHandle
            sync_config::InnerSyncCfg
            hostname::String
        end

        r = RepeRPC.Registry()
        RepeRPC.register!(r, "sync_handle",
            OuterSyncHandle(InnerSyncCfg(1.0, "hello"), "node-1"))

        schema = RepeRPC._build_registry_schema(r)

        # Deep field is reachable in the schema, not just one level down
        @test schema["/sync_handle/sync_config/retry_delay_s"] == "data"
        @test schema["/sync_handle/sync_config/comment"] == "data"
        @test schema["/sync_handle/hostname"] == "data"

        # Intermediate struct paths are NOT leaves (their fields are)
        @test !haskey(schema, "/sync_handle")
        @test !haskey(schema, "/sync_handle/sync_config")
    end

    @testset "Schema cycle detection" begin
        # Mutable struct that points at itself. Without cycle detection the
        # recursion would never terminate; with it, the cycling field should
        # surface as a data leaf and recursion stops.
        mutable struct CycleNode
            label::String
            self::Union{Nothing, CycleNode}
        end
        n = CycleNode("root", nothing)
        n.self = n

        r = RepeRPC.Registry()
        RepeRPC.register!(r, "node", n)

        schema = RepeRPC._build_registry_schema(r)

        @test schema["/node/label"] == "data"
        # Cycle on the self-pointing field collapses to a data leaf
        @test schema["/node/self"] == "data"
    end

    @testset "Schema recurses into nested AbstractDicts" begin
        # When a struct field is a Dict (or a Dict appears under a Dict), the
        # children should still be discoverable in the schema.
        r = RepeRPC.Registry()
        RepeRPC.register!(r, "api", Dict{String, Any}(
            "users" => Dict{String, Any}(
                "list"  => () -> ["a", "b"],
                "count" => 2,
            ),
            "ping" => () -> "pong",
        ))

        schema = RepeRPC._build_registry_schema(r)

        @test schema["/api/users/list"]  == "callable"
        @test schema["/api/users/count"] == "data"
        @test schema["/api/ping"]        == "callable"
    end

    @testset "Schema treats Vectors and primitives as data leaves" begin
        mutable struct WithVec
            xs::Vector{Int}
            label::String
        end

        r = RepeRPC.Registry()
        RepeRPC.register!(r, "v", WithVec([1, 2, 3], "x"))

        schema = RepeRPC._build_registry_schema(r)

        # Vector field is a leaf, not expanded into per-index entries
        @test schema["/v/xs"] == "data"
        @test schema["/v/label"] == "data"
        @test !haskey(schema, "/v/xs/0")  # would be wrong: arrays aren't expanded
    end

    @testset "Schema mutual cycle terminates" begin
        # Two structs pointing at each other. Recursion must terminate, and
        # the back-edge to a node already on the current path must collapse
        # to a "data" leaf.
        mutable struct CycleA
            child::Any
        end
        mutable struct CycleB
            child::Any
        end
        a = CycleA(nothing)
        b = CycleB(nothing)
        a.child = b
        b.child = a

        r = RepeRPC.Registry()
        RepeRPC.register!(r, "a", a)

        schema = RepeRPC._build_registry_schema(r)

        # Walking from /a, we descend through b, then hit a again on the
        # current path and collapse there.
        @test schema["/a/child/child"] == "data"
        # Intermediate paths are not leaves
        @test !haskey(schema, "/a")
        @test !haskey(schema, "/a/child")
    end

    @testset "Schema fan-in expands the shared instance under each parent" begin
        # Same instance referenced by two fields of the same parent. Cycle
        # detection is per-path, so neither path masks the other.
        mutable struct Shared
            v::Int
        end
        mutable struct Owner
            left::Shared
            right::Shared
        end
        shared = Shared(42)

        r = RepeRPC.Registry()
        RepeRPC.register!(r, "owner", Owner(shared, shared))

        schema = RepeRPC._build_registry_schema(r)

        @test schema["/owner/left/v"]  == "data"
        @test schema["/owner/right/v"] == "data"
    end

    @testset "Schema labels callable struct fields as callable" begin
        # Pre-recursion this branch unconditionally wrote "data" for every
        # field; recursing through _collect_schema! lets _is_callable fire on
        # the field and label it correctly.
        mutable struct Handler
            do_it::Function
            note::String
        end

        r = RepeRPC.Registry()
        RepeRPC.register!(r, "h", Handler(() -> "done", "hello"))

        schema = RepeRPC._build_registry_schema(r)

        @test schema["/h/do_it"] == "callable"
        @test schema["/h/note"]  == "data"
    end

    @testset "Schema handles AbstractDict with non-string keys" begin
        # The pointer resolver accepts symbol keys; the schema collector now
        # stringifies them so the path is well-formed and consistent with how
        # struct fieldnames are surfaced.
        r = RepeRPC.Registry()
        RepeRPC.register!(r, "things", Dict{Symbol, Any}(
            :foo => 1,
            :bar => () -> "baz",
        ))

        schema = RepeRPC._build_registry_schema(r)

        @test schema["/things/foo"] == "data"
        @test schema["/things/bar"] == "callable"
    end

    @testset "Schema Endpoint via Server" begin
        r = RepeRPC.Registry(
            "name" => () -> "TestApp",
            "add" => (;a, b) -> a + b,
            "counter" => 0
        )

        server = RepeRPC.Server("localhost", 18093)
        RepeRPC.serve(server, r)

        RepeRPC.listen(server, async=true)
        RepeRPC.wait_for_server("localhost", 18093)

        try
            client = RepeRPC.Client("localhost", 18093)
            RepeRPC.connect(client)

            schema = RepeRPC.send_request(client, "/__schema__", nothing,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)

            @test schema["/name"] == "callable"
            @test schema["/add"] == "callable"
            @test schema["/counter"] == "data"

            RepeRPC.disconnect(client)
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "serve returns the Registry" begin
        # Registry overload returns the same registry instance
        r = RepeRPC.Registry("counter" => 0)
        server = RepeRPC.Server("localhost", 18094)
        returned = RepeRPC.serve(server, r)
        @test returned === r

        # Flat dict overload returns the constructed registry
        flat = Dict{String, Any}("/value" => 7)
        server2 = RepeRPC.Server("localhost", 18095)
        registry = RepeRPC.serve(server2, flat)
        @test registry isa RepeRPC.Registry
        @test registry["value"] == 7
    end

    @testset "handle_registry_request fall_through=true" begin
        r = RepeRPC.Registry("known" => 42)

        # Existing path resolves normally
        req_known = RepeRPC.Message(query="/known", body=nothing,
                                 query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
                                 body_format=UInt16(RepeRPC.BODY_JSON))
        resp_known = RepeRPC.handle_registry_request(r, req_known; fall_through=true)
        @test resp_known isa RepeRPC.Message
        @test RepeRPC.parse_body(resp_known) == 42

        # Unknown path returns nothing so the pipeline can continue
        req_missing = RepeRPC.Message(query="/missing", body=nothing,
                                   query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
                                   body_format=UInt16(RepeRPC.BODY_JSON))
        @test RepeRPC.handle_registry_request(r, req_missing; fall_through=true) === nothing

        # Unknown nested path also falls through
        req_nested = RepeRPC.Message(query="/missing/deep", body=nothing,
                                  query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
                                  body_format=UInt16(RepeRPC.BODY_JSON))
        @test RepeRPC.handle_registry_request(r, req_nested; fall_through=true) === nothing

        # Default fall_through=false preserves the legacy error response
        resp_legacy = RepeRPC.handle_registry_request(r, req_missing)
        @test resp_legacy isa RepeRPC.Message
        @test resp_legacy.header.ec != UInt32(RepeRPC.EC_OK)
    end

    @testset "Direct handler fallthrough alongside served registry" begin
        registry = RepeRPC.Registry("counter" => 99)

        server = RepeRPC.Server("localhost", 18096)
        # Registry overload defaults to fall_through=false; opt in here so that
        # direct handlers can serve paths the registry doesn't expose.
        returned = RepeRPC.serve(server, registry; fall_through=true)
        @test returned === registry

        # Add a direct handler at a path NOT in the registry
        ack = Ref(0)
        RepeRPC.register(server, "/direct/ping", function(params, request)
            ack[] += 1
            return Dict("pong" => true, "count" => ack[])
        end)

        RepeRPC.listen(server, async=true)
        RepeRPC.wait_for_server("localhost", 18096)

        try
            client = RepeRPC.Client("localhost", 18096)
            RepeRPC.connect(client)

            # Registry path still served by the registry
            @test RepeRPC.send_request(client, "/counter", nothing,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON) == 99

            # Path not in registry reaches the direct handler
            ping = RepeRPC.send_request(client, "/direct/ping", nothing,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test ping["pong"] == true
            @test ping["count"] == 1
            @test ack[] == 1

            # Truly unknown path returns EC_METHOD_NOT_FOUND from the pipeline tail
            id = UInt64(time_ns())
            req = RepeRPC.Message(id=id, query="/no_such_thing", body=nothing,
                               query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
                               body_format=UInt16(RepeRPC.BODY_JSON))
            resp = RepeRPC._process_request(server, req)
            @test resp.header.ec == UInt32(RepeRPC.EC_METHOD_NOT_FOUND)

            RepeRPC.disconnect(client)
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "fall_through=true disables auto-create and routes writes to handlers" begin
        # Exercises the exact behavior change introduced by fall_through=true on
        # the WRITE branch: writes to unknown paths must NOT auto-create entries
        # and must instead reach a direct handler if one is registered.
        registry = RepeRPC.Registry()
        server = RepeRPC.Server("localhost", 18099)
        RepeRPC.serve(server, registry; fall_through=true)

        captured = Ref{Any}(nothing)
        RepeRPC.register(server, "/dyn/value", function(params, request)
            captured[] = params
            return Dict("received" => params)
        end)

        RepeRPC.listen(server, async=true)
        RepeRPC.wait_for_server("localhost", 18099)

        try
            client = RepeRPC.Client("localhost", 18099)
            RepeRPC.connect(client)

            # Write with non-empty body to a path the registry doesn't have.
            # Must reach the direct handler, NOT auto-create in the registry.
            resp = RepeRPC.send_request(client, "/dyn/value", 7,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test resp["received"] == 7
            @test captured[] == 7
            @test !haskey(registry, "dyn")
            @test !haskey(registry, "value")

            # Top-level unknown write must also fall through (no auto-create)
            id = UInt64(time_ns())
            req = RepeRPC.Message(id=id, query="/no_handler_here", body=42,
                               query_format=UInt16(RepeRPC.QUERY_JSON_POINTER),
                               body_format=UInt16(RepeRPC.BODY_JSON))
            resp_unknown = RepeRPC._process_request(server, req)
            @test resp_unknown.header.ec == UInt32(RepeRPC.EC_METHOD_NOT_FOUND)
            @test !haskey(registry, "no_handler_here")

            RepeRPC.disconnect(client)
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "register! after serve adds a live endpoint" begin
        registry = RepeRPC.Registry("first" => 1)
        server = RepeRPC.Server("localhost", 18097)
        returned = RepeRPC.serve(server, registry)

        # Mutate the registry after serve has been wired up
        RepeRPC.register!(returned, "/added/later", () -> "lazy")

        RepeRPC.listen(server, async=true)
        RepeRPC.wait_for_server("localhost", 18097)

        try
            client = RepeRPC.Client("localhost", 18097)
            RepeRPC.connect(client)

            # call_on_read=false is the default for the Registry overload, so
            # an empty body on a callable returns the function info dict.
            info = RepeRPC.send_request(client, "/added/later", nothing,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test info["type"] == "function"
            @test info["path"] == "/added/later"

            # Calling with an empty Dict invokes the function
            value = RepeRPC.send_request(client, "/added/later", Dict{String,Any}(),
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test value == "lazy"

            RepeRPC.disconnect(client)
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "fall_through=false preserves legacy auto-create on write" begin
        registry = RepeRPC.Registry()
        server = RepeRPC.Server("localhost", 18098)
        RepeRPC.serve(server, registry; fall_through=false)

        RepeRPC.listen(server, async=true)
        RepeRPC.wait_for_server("localhost", 18098)

        try
            client = RepeRPC.Client("localhost", 18098)
            RepeRPC.connect(client)

            # Write to a previously-unknown top-level path. Legacy behavior
            # creates the entry on first write.
            written = RepeRPC.send_request(client, "/created_on_write", 123,
                query_format=RepeRPC.QUERY_JSON_POINTER,
                body_format=RepeRPC.BODY_JSON)
            @test written == 123
            @test registry["created_on_write"] == 123

            RepeRPC.disconnect(client)
        finally
            RepeRPC.stop(server)
        end
    end

end
