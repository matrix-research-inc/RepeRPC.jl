@testset "Fleet Tests" begin

    #==========================================================================#
    # NodeConfig Tests
    #==========================================================================#
    @testset "NodeConfig" begin
        @testset "Basic construction" begin
            config = RepeRPC.NodeConfig("localhost", 8080)
            @test config.name == "localhost"
            @test config.host == "localhost"
            @test config.port == 8080
            @test config.tags == String[]
            @test config.timeout == 30.0
        end

        @testset "Custom name" begin
            config = RepeRPC.NodeConfig("192.168.1.10", 8080; name="compute-1")
            @test config.name == "compute-1"
            @test config.host == "192.168.1.10"
        end

        @testset "With tags" begin
            config = RepeRPC.NodeConfig("localhost", 8080; tags=["compute", "gpu"])
            @test config.tags == ["compute", "gpu"]
        end

        @testset "Custom timeout" begin
            config = RepeRPC.NodeConfig("localhost", 8080; timeout=60.0)
            @test config.timeout == 60.0
        end

        @testset "Invalid port" begin
            @test_throws ArgumentError RepeRPC.NodeConfig("localhost", 0)
            @test_throws ArgumentError RepeRPC.NodeConfig("localhost", 70000)
        end

        @testset "Invalid timeout" begin
            @test_throws ArgumentError RepeRPC.NodeConfig("localhost", 8080; timeout=0.0)
            @test_throws ArgumentError RepeRPC.NodeConfig("localhost", 8080; timeout=-1.0)
        end
    end

    #==========================================================================#
    # Fleet Construction Tests
    #==========================================================================#
    @testset "Fleet Construction" begin
        @testset "Empty fleet" begin
            fleet = RepeRPC.Fleet()
            @test length(fleet) == 0
        end

        @testset "Fleet from configs" begin
            configs = [
                RepeRPC.NodeConfig("localhost", 9101; name="node-1"),
                RepeRPC.NodeConfig("localhost", 9102; name="node-2"),
            ]
            fleet = RepeRPC.Fleet(configs)
            @test length(fleet) == 2
        end

        @testset "Duplicate names throw error" begin
            configs = [
                RepeRPC.NodeConfig("localhost", 9101),
                RepeRPC.NodeConfig("localhost", 9102),  # Same name (defaults to host)
            ]
            @test_throws ArgumentError RepeRPC.Fleet(configs)
        end

        @testset "Duplicate names - explicit" begin
            configs = [
                RepeRPC.NodeConfig("host1", 9101; name="same-name"),
                RepeRPC.NodeConfig("host2", 9102; name="same-name"),
            ]
            @test_throws ArgumentError RepeRPC.Fleet(configs)
        end

        @testset "Constructor parameters" begin
            configs = [RepeRPC.NodeConfig("localhost", 9101; name="node-1")]
            fleet = RepeRPC.Fleet(configs; timeout=60.0, max_retry_attempts=5, retry_delay=2.0)
            @test fleet.default_timeout == 60.0
            @test fleet.retry_policy.max_attempts == 5
            @test fleet.retry_policy.delay == 2.0
        end

        @testset "Invalid constructor parameters" begin
            configs = [RepeRPC.NodeConfig("localhost", 9101; name="node-1")]
            @test_throws ArgumentError RepeRPC.Fleet(configs; timeout=0.0)
            @test_throws ArgumentError RepeRPC.Fleet(configs; max_retry_attempts=0)
            @test_throws ArgumentError RepeRPC.Fleet(configs; retry_delay=-1.0)
        end
    end

    #==========================================================================#
    # Node Access Tests
    #==========================================================================#
    @testset "Node Access" begin
        configs = [
            RepeRPC.NodeConfig("localhost", 9101; name="node-1", tags=["compute", "primary"]),
            RepeRPC.NodeConfig("localhost", 9102; name="node-2", tags=["compute"]),
            RepeRPC.NodeConfig("localhost", 9103; name="node-3", tags=["storage"]),
        ]
        fleet = RepeRPC.Fleet(configs)

        @testset "nodes()" begin
            all_nodes = RepeRPC.nodes(fleet)
            @test length(all_nodes) == 3
            @test all(n -> n isa RepeRPC.Node, all_nodes)
        end

        @testset "getindex" begin
            node = fleet["node-1"]
            @test node.name == "node-1"
            @test node.port == 9101
        end

        @testset "getindex - not found" begin
            @test_throws KeyError fleet["nonexistent"]
        end

        @testset "keys()" begin
            k = keys(fleet)
            @test length(k) == 3
            @test "node-1" in k
            @test "node-2" in k
            @test "node-3" in k
        end

        @testset "filter_nodes by tags" begin
            compute_nodes = RepeRPC.filter_nodes(fleet; tags=["compute"])
            @test length(compute_nodes) == 2
            @test all(n -> "compute" in n.tags, compute_nodes)

            storage_nodes = RepeRPC.filter_nodes(fleet; tags=["storage"])
            @test length(storage_nodes) == 1
            @test storage_nodes[1].name == "node-3"

            primary_nodes = RepeRPC.filter_nodes(fleet; tags=["compute", "primary"])
            @test length(primary_nodes) == 1
            @test primary_nodes[1].name == "node-1"

            # Empty result
            no_nodes = RepeRPC.filter_nodes(fleet; tags=["nonexistent"])
            @test length(no_nodes) == 0
        end
    end

    #==========================================================================#
    # Dynamic Node Management Tests
    #==========================================================================#
    @testset "Dynamic Node Management" begin
        @testset "add_node!" begin
            fleet = RepeRPC.Fleet()
            @test length(fleet) == 0

            RepeRPC.add_node!(fleet, RepeRPC.NodeConfig("localhost", 9101; name="node-1"))
            @test length(fleet) == 1
            @test "node-1" in keys(fleet)

            RepeRPC.add_node!(fleet, RepeRPC.NodeConfig("localhost", 9102; name="node-2"))
            @test length(fleet) == 2
        end

        @testset "add_node! duplicate throws" begin
            fleet = RepeRPC.Fleet()
            RepeRPC.add_node!(fleet, RepeRPC.NodeConfig("localhost", 9101; name="node-1"))
            @test_throws ArgumentError RepeRPC.add_node!(fleet, RepeRPC.NodeConfig("localhost", 9102; name="node-1"))
        end

        @testset "remove_node!" begin
            configs = [
                RepeRPC.NodeConfig("localhost", 9101; name="node-1"),
                RepeRPC.NodeConfig("localhost", 9102; name="node-2"),
            ]
            fleet = RepeRPC.Fleet(configs)
            @test length(fleet) == 2

            RepeRPC.remove_node!(fleet, "node-1")
            @test length(fleet) == 1
            @test !("node-1" in keys(fleet))
            @test "node-2" in keys(fleet)
        end

        @testset "remove_node! nonexistent is no-op" begin
            fleet = RepeRPC.Fleet()
            RepeRPC.remove_node!(fleet, "nonexistent")  # Should not throw
            @test length(fleet) == 0
        end
    end

    #==========================================================================#
    # RemoteResult Tests
    #==========================================================================#
    @testset "RemoteResult" begin
        @testset "Success result" begin
            result = RepeRPC.RemoteResult{Dict}("node-1", Dict("value" => 42), nothing, 0.1)
            @test RepeRPC.succeeded(result)
            @test !RepeRPC.failed(result)
            @test result[] == Dict("value" => 42)
            @test result.node == "node-1"
            @test result.elapsed == 0.1
        end

        @testset "Failed result" begin
            err = ErrorException("Connection failed")
            result = RepeRPC.RemoteResult{Dict}("node-1", nothing, err, 0.5)
            @test !RepeRPC.succeeded(result)
            @test RepeRPC.failed(result)
            @test_throws ErrorException result[]
        end
    end

    #==========================================================================#
    # Connection Management Tests (with real servers)
    #==========================================================================#
    @testset "Connection Management" begin
        # Start test servers
        port1 = 9201
        port2 = 9202
        port3 = 9203

        server1 = RepeRPC.Server("localhost", port1)
        server2 = RepeRPC.Server("localhost", port2)

        RepeRPC.register(server1, "/status", (params, req) -> Dict("status" => "ok", "node" => 1))
        RepeRPC.register(server2, "/status", (params, req) -> Dict("status" => "ok", "node" => 2))

        RepeRPC.listen(server1; async=true)
        RepeRPC.listen(server2; async=true)
        RepeRPC.wait_for_server("localhost", port1)
        RepeRPC.wait_for_server("localhost", port2)

        try
            configs = [
                RepeRPC.NodeConfig("localhost", port1; name="server-1"),
                RepeRPC.NodeConfig("localhost", port2; name="server-2"),
                RepeRPC.NodeConfig("localhost", port3; name="server-3"),  # Not running
            ]
            fleet = RepeRPC.Fleet(configs)

            @testset "connect!" begin
                result = RepeRPC.connect!(fleet)
                @test "server-1" in result.connected
                @test "server-2" in result.connected
                @test "server-3" in result.failed
            end

            @testset "isconnected variants" begin
                @test !RepeRPC.isconnected(fleet)  # Not all connected (server-3 failed)
                @test RepeRPC.isconnected(fleet, "server-1")
                @test RepeRPC.isconnected(fleet, "server-2")
                @test !RepeRPC.isconnected(fleet, "server-3")

                node1 = fleet["server-1"]
                @test RepeRPC.isconnected(node1)
            end

            @testset "connected_nodes" begin
                connected = RepeRPC.connected_nodes(fleet)
                @test length(connected) == 2
                names = [n.name for n in connected]
                @test "server-1" in names
                @test "server-2" in names
            end

            @testset "disconnect!" begin
                result = RepeRPC.disconnect!(fleet)
                @test "server-1" in result.disconnected
                @test "server-2" in result.disconnected
                @test !RepeRPC.isconnected(fleet, "server-1")
                @test !RepeRPC.isconnected(fleet, "server-2")
            end

            @testset "reconnect!" begin
                # Reconnect to the servers that were disconnected
                result = RepeRPC.reconnect!(fleet)
                @test "server-1" in result.reconnected
                @test "server-2" in result.reconnected
                @test RepeRPC.isconnected(fleet, "server-1")
                @test RepeRPC.isconnected(fleet, "server-2")
            end

        finally
            RepeRPC.stop(server1)
            RepeRPC.stop(server2)
        end
    end

    #==========================================================================#
    # Remote Invocation Tests
    #==========================================================================#
    @testset "Remote Invocation" begin
        port1 = 9301
        port2 = 9302

        server1 = RepeRPC.Server("localhost", port1)
        server2 = RepeRPC.Server("localhost", port2)

        RepeRPC.register(server1, "/compute", (params, req) -> Dict("result" => params["value"] * 2, "node" => 1))
        RepeRPC.register(server2, "/compute", (params, req) -> Dict("result" => params["value"] * 3, "node" => 2))
        RepeRPC.register(server1, "/status", (params, req) -> Dict("status" => "ok"))
        RepeRPC.register(server2, "/status", (params, req) -> Dict("status" => "ok"))

        RepeRPC.listen(server1; async=true)
        RepeRPC.listen(server2; async=true)
        RepeRPC.wait_for_server("localhost", port1)
        RepeRPC.wait_for_server("localhost", port2)

        try
            configs = [
                RepeRPC.NodeConfig("localhost", port1; name="server-1", tags=["compute"]),
                RepeRPC.NodeConfig("localhost", port2; name="server-2", tags=["compute", "primary"]),
            ]
            fleet = RepeRPC.Fleet(configs)
            RepeRPC.connect!(fleet)

            @testset "call" begin
                result = RepeRPC.call(fleet, "server-1", "/compute", Dict("value" => 10))
                @test RepeRPC.succeeded(result)
                @test result.value["result"] == 20
                @test result.node == "server-1"

                result2 = RepeRPC.call(fleet, "server-2", "/compute", Dict("value" => 10))
                @test RepeRPC.succeeded(result2)
                @test result2.value["result"] == 30
            end

            @testset "call - node not found captured, not thrown" begin
                # An unknown/removed node is a per-node failure, captured into the
                # RemoteResult like any other; `r[]` rethrows it for callers who
                # want the miss loud.
                r = RepeRPC.call(fleet, "nonexistent", "/status", nothing)
                @test RepeRPC.failed(r)
                @test r.error isa KeyError
                @test r.node == "nonexistent"
                @test_throws KeyError r[]
            end

            @testset "broadcast" begin
                results = RepeRPC.broadcast(fleet, "/status")
                @test length(results) == 2
                @test haskey(results, "server-1")
                @test haskey(results, "server-2")
                @test all(RepeRPC.succeeded, values(results))
            end

            @testset "broadcast with tags" begin
                results = RepeRPC.broadcast(fleet, "/status"; tags=["primary"])
                @test length(results) == 1
                @test haskey(results, "server-2")

                results_compute = RepeRPC.broadcast(fleet, "/compute", Dict("value" => 5); tags=["compute"])
                @test length(results_compute) == 2
            end

            @testset "map_reduce" begin
                total = RepeRPC.map_reduce(fleet, "/compute", Dict("value" => 10); tags=["compute"]) do results
                    sum(r.value["result"] for r in results if RepeRPC.succeeded(r))
                end
                @test total == 50  # 20 + 30
            end

            @testset "callable syntax - broadcast" begin
                results = fleet("/status")
                @test length(results) == 2
                @test all(RepeRPC.succeeded, values(results))
            end

            @testset "callable syntax - single node" begin
                result = fleet("server-1", "/compute", Dict("value" => 7))
                @test RepeRPC.succeeded(result)
                @test result.value["result"] == 14
            end

        finally
            RepeRPC.stop(server1)
            RepeRPC.stop(server2)
        end
    end

    #==========================================================================#
    # Health Check Tests
    #==========================================================================#
    @testset "Health Check" begin
        port1 = 9401
        port2 = 9402

        server1 = RepeRPC.Server("localhost", port1)
        RepeRPC.register(server1, "/status", (params, req) -> Dict("status" => "healthy"))
        RepeRPC.listen(server1; async=true)
        RepeRPC.wait_for_server("localhost", port1)

        try
            configs = [
                RepeRPC.NodeConfig("localhost", port1; name="healthy-server"),
                RepeRPC.NodeConfig("localhost", port2; name="dead-server"),  # Not running
            ]
            fleet = RepeRPC.Fleet(configs)

            health = RepeRPC.health_check(fleet)

            @test haskey(health, "healthy-server")
            @test haskey(health, "dead-server")

            @test health["healthy-server"].healthy == true
            @test health["healthy-server"].error === nothing
            @test health["healthy-server"].latency > 0

            @test health["dead-server"].healthy == false
            @test health["dead-server"].error !== nothing

        finally
            RepeRPC.stop(server1)
        end
    end

    #==========================================================================#
    # Retry Logic Tests
    #==========================================================================#
    @testset "Retry Logic" begin
        port = 9501

        # Handler that always raises. The server wraps that into a structured
        # error response, which is reported back as an exception on the client.
        # Under the whitelist retry predicate, server-processed errors are not
        # retried (the request reached the server and was handled).
        call_count = Ref(0)
        server = RepeRPC.Server("localhost", port)
        RepeRPC.register(server, "/always_fails", function(params, req)
            call_count[] += 1
            throw(ErrorException("handler refused"))
        end)

        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)

        try
            configs = [RepeRPC.NodeConfig("localhost", port; name="strict-server")]
            fleet = RepeRPC.Fleet(configs; max_retry_attempts=5, retry_delay=0.05)
            RepeRPC.connect!(fleet)

            result = RepeRPC.call(fleet, "strict-server", "/always_fails", nothing)
            @test RepeRPC.failed(result)
            @test call_count[] == 1  # not retried; the server already processed it

        finally
            RepeRPC.stop(server)
        end
    end

    @testset "Retry on transport failure" begin
        # Lazily-started server: the first connect attempts fail with IOError
        # (port not bound yet), and the retry loop keeps trying until the
        # server comes up. Exercises the whitelist's happy path on
        # ConnectionError / Base.IOError.
        port = 9520
        server_ref = Ref{Any}(nothing)

        server_starter = @async begin
            sleep(0.25)  # give the fleet a couple of failed attempts first
            srv = RepeRPC.Server("localhost", port)
            RepeRPC.register(srv, "/ping", (params, req) -> "pong")
            server_ref[] = srv
            RepeRPC.listen(srv; async=true)
        end

        try
            configs = [RepeRPC.NodeConfig("localhost", port; name="lazy")]
            # Generous attempt budget so the lazy startup falls within it
            fleet = RepeRPC.Fleet(configs; max_retry_attempts=10, retry_delay=0.1, timeout=2.0)

            result = RepeRPC.call(fleet, "lazy", "/ping", nothing)
            @test RepeRPC.succeeded(result)
            @test result.value == "pong"
        finally
            wait(server_starter)
            server_ref[] !== nothing && RepeRPC.stop(server_ref[])
        end
    end

    @testset "Retry Classification" begin
        # TimeoutError must not retry: a slow handler that exceeds the client
        # timeout means the request reached the server and may already be
        # changing state. Retrying could double-trigger side effects (the
        # original motivation for stateful RPCs like a hardware capture).
        @testset "no retry on TimeoutError" begin
            port = 9510
            call_count = Ref(0)
            server = RepeRPC.Server("localhost", port)
            RepeRPC.register(server, "/slow", function(params, req)
                call_count[] += 1
                sleep(0.4)
                return Dict("done" => true)
            end)
            RepeRPC.listen(server; async=true)
            RepeRPC.wait_for_server("localhost", port)

            try
                # Per-node timeout shorter than the handler's sleep
                configs = [RepeRPC.NodeConfig("localhost", port; name="slow", timeout=0.1)]
                fleet = RepeRPC.Fleet(configs; max_retry_attempts=5, retry_delay=0.05)
                RepeRPC.connect!(fleet)

                result = RepeRPC.call(fleet, "slow", "/slow", nothing)
                @test RepeRPC.failed(result)
                @test result.error isa RepeRPC.TimeoutError

                # Wait long enough that any retried in-flight handler would
                # have completed. Worst-case retry budget is ~0.75s
                # (5 timeouts plus delays), plus one handler-sleep of 0.4s.
                sleep(1.0)
                @test call_count[] == 1
            finally
                RepeRPC.stop(server)
            end
        end

        # _is_retryable_error predicate, exercised directly so the contract is
        # asserted independently of wire-level scenarios.
        @testset "_is_retryable_error predicate" begin
            # Whitelist: only transport-level exceptions retry.
            @test RepeRPC._is_retryable_error(RepeRPC.ConnectionError("dropped"))
            @test RepeRPC._is_retryable_error(Base.IOError("write", -32))
            @test RepeRPC._is_retryable_error(EOFError())

            # Everything else defaults to non-retryable so future exception
            # classes don't silently inherit retry behavior.
            @test !RepeRPC._is_retryable_error(RepeRPC.TimeoutError(1.0))
            @test !RepeRPC._is_retryable_error(RepeRPC.REPEError(RepeRPC.EC_METHOD_NOT_FOUND, "x"))
            @test !RepeRPC._is_retryable_error(RepeRPC.ValidationError("body", "bad"))
            @test !RepeRPC._is_retryable_error(ErrorException("RPC Error (5): not found"))
            @test !RepeRPC._is_retryable_error(ArgumentError("oops"))
        end
    end

    #==========================================================================#
    # Thread Safety Tests
    #==========================================================================#
    @testset "Thread Safety" begin
        port = 9601

        server = RepeRPC.Server("localhost", port)
        RepeRPC.register(server, "/echo", (params, req) -> params)
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", port)

        try
            configs = [RepeRPC.NodeConfig("localhost", port; name="server-1")]
            fleet = RepeRPC.Fleet(configs)
            RepeRPC.connect!(fleet)

            # Concurrent broadcasts
            tasks = Task[]
            for i in 1:10
                push!(tasks, @async begin
                    RepeRPC.broadcast(fleet, "/echo", Dict("id" => i))
                end)
            end

            results = [fetch(t) for t in tasks]
            @test length(results) == 10
            @test all(r -> length(r) == 1, results)

        finally
            RepeRPC.stop(server)
        end
    end

    #==========================================================================#
    # Pretty Printing Tests
    #==========================================================================#
    @testset "Pretty Printing" begin
        config = RepeRPC.NodeConfig("localhost", 8080; name="test-node", tags=["compute"])
        @test occursin("NodeConfig", string(config))
        @test occursin("localhost", string(config))

        configs = [RepeRPC.NodeConfig("localhost", 9701; name="node-1")]
        fleet = RepeRPC.Fleet(configs)
        @test occursin("Fleet", string(fleet))

        result_success = RepeRPC.RemoteResult{Any}("node-1", Dict(), nothing, 0.1)
        @test occursin("success", string(result_success))

        result_failed = RepeRPC.RemoteResult{Any}("node-1", nothing, ErrorException("err"), 0.1)
        @test occursin("failed", string(result_failed))
    end

end
