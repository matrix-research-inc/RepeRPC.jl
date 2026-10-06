@testset "UniUDP Fleet Tests" begin

    #==========================================================================#
    # UniUDPNodeConfig Tests
    #==========================================================================#
    @testset "UniUDPNodeConfig" begin
        @testset "Basic construction" begin
            config = RepeRPC.UniUDPNodeConfig("localhost", 5000)
            @test config.name == "localhost"
            @test config.host == "localhost"
            @test config.port == 5000
            @test config.tags == String[]
            @test config.redundancy == 1
            @test config.chunk_size == 1024
            @test config.fec_group_size == 4
        end

        @testset "Custom name" begin
            config = RepeRPC.UniUDPNodeConfig("192.168.1.10", 5000; name="gateway-1")
            @test config.name == "gateway-1"
            @test config.host == "192.168.1.10"
        end

        @testset "With tags" begin
            config = RepeRPC.UniUDPNodeConfig("localhost", 5000; tags=["sensor", "primary"])
            @test config.tags == ["sensor", "primary"]
        end

        @testset "Reliability settings" begin
            config = RepeRPC.UniUDPNodeConfig("localhost", 5000;
                                           redundancy=3,
                                           chunk_size=512,
                                           fec_group_size=8)
            @test config.redundancy == 3
            @test config.chunk_size == 512
            @test config.fec_group_size == 8
        end

        @testset "Invalid parameters" begin
            @test_throws ArgumentError RepeRPC.UniUDPNodeConfig("localhost", 0)
            @test_throws ArgumentError RepeRPC.UniUDPNodeConfig("localhost", 70000)
            @test_throws ArgumentError RepeRPC.UniUDPNodeConfig("localhost", 5000; redundancy=0)
            @test_throws ArgumentError RepeRPC.UniUDPNodeConfig("localhost", 5000; chunk_size=0)
            @test_throws ArgumentError RepeRPC.UniUDPNodeConfig("localhost", 5000; fec_group_size=0)
        end
    end

    #==========================================================================#
    # UniUDPFleet Construction Tests
    #==========================================================================#
    @testset "UniUDPFleet Construction" begin
        @testset "Empty fleet" begin
            fleet = RepeRPC.UniUDPFleet()
            @test length(fleet) == 0
        end

        @testset "Fleet from configs" begin
            configs = [
                RepeRPC.UniUDPNodeConfig("localhost", 5001; name="node-1"),
                RepeRPC.UniUDPNodeConfig("localhost", 5002; name="node-2"),
            ]
            fleet = RepeRPC.UniUDPFleet(configs)
            @test length(fleet) == 2
        end

        @testset "Duplicate names throw error" begin
            configs = [
                RepeRPC.UniUDPNodeConfig("localhost", 5001),
                RepeRPC.UniUDPNodeConfig("localhost", 5002),  # Same name (defaults to host)
            ]
            @test_throws ArgumentError RepeRPC.UniUDPFleet(configs)
        end

        @testset "Duplicate names - explicit" begin
            configs = [
                RepeRPC.UniUDPNodeConfig("host1", 5001; name="same-name"),
                RepeRPC.UniUDPNodeConfig("host2", 5002; name="same-name"),
            ]
            @test_throws ArgumentError RepeRPC.UniUDPFleet(configs)
        end
    end

    #==========================================================================#
    # Node Access Tests
    #==========================================================================#
    @testset "Node Access" begin
        configs = [
            RepeRPC.UniUDPNodeConfig("localhost", 5001; name="node-1", tags=["sensor", "primary"]),
            RepeRPC.UniUDPNodeConfig("localhost", 5002; name="node-2", tags=["sensor"]),
            RepeRPC.UniUDPNodeConfig("localhost", 5003; name="node-3", tags=["gateway"]),
        ]
        fleet = RepeRPC.UniUDPFleet(configs)

        @testset "nodes()" begin
            all_nodes = RepeRPC.nodes(fleet)
            @test length(all_nodes) == 3
            @test all(n -> n isa RepeRPC.UniUDPNode, all_nodes)
        end

        @testset "getindex" begin
            node = fleet["node-1"]
            @test node.name == "node-1"
            @test node.port == 5001
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
            sensor_nodes = RepeRPC.filter_nodes(fleet; tags=["sensor"])
            @test length(sensor_nodes) == 2
            @test all(n -> "sensor" in n.tags, sensor_nodes)

            gateway_nodes = RepeRPC.filter_nodes(fleet; tags=["gateway"])
            @test length(gateway_nodes) == 1
            @test gateway_nodes[1].name == "node-3"

            primary_nodes = RepeRPC.filter_nodes(fleet; tags=["sensor", "primary"])
            @test length(primary_nodes) == 1
            @test primary_nodes[1].name == "node-1"

            # Empty result
            no_nodes = RepeRPC.filter_nodes(fleet; tags=["nonexistent"])
            @test length(no_nodes) == 0
        end

        # Clean up
        close(fleet)
    end

    #==========================================================================#
    # Dynamic Node Management Tests
    #==========================================================================#
    @testset "Dynamic Node Management" begin
        @testset "add_node!" begin
            fleet = RepeRPC.UniUDPFleet()
            @test length(fleet) == 0

            RepeRPC.add_node!(fleet, RepeRPC.UniUDPNodeConfig("localhost", 5001; name="node-1"))
            @test length(fleet) == 1
            @test "node-1" in keys(fleet)

            RepeRPC.add_node!(fleet, RepeRPC.UniUDPNodeConfig("localhost", 5002; name="node-2"))
            @test length(fleet) == 2

            close(fleet)
        end

        @testset "add_node! duplicate throws" begin
            fleet = RepeRPC.UniUDPFleet()
            RepeRPC.add_node!(fleet, RepeRPC.UniUDPNodeConfig("localhost", 5001; name="node-1"))
            @test_throws ArgumentError RepeRPC.add_node!(fleet, RepeRPC.UniUDPNodeConfig("localhost", 5002; name="node-1"))
            close(fleet)
        end

        @testset "remove_node!" begin
            configs = [
                RepeRPC.UniUDPNodeConfig("localhost", 5001; name="node-1"),
                RepeRPC.UniUDPNodeConfig("localhost", 5002; name="node-2"),
            ]
            fleet = RepeRPC.UniUDPFleet(configs)
            @test length(fleet) == 2

            RepeRPC.remove_node!(fleet, "node-1")
            @test length(fleet) == 1
            @test !("node-1" in keys(fleet))
            @test "node-2" in keys(fleet)

            close(fleet)
        end

        @testset "remove_node! nonexistent is no-op" begin
            fleet = RepeRPC.UniUDPFleet()
            RepeRPC.remove_node!(fleet, "nonexistent")  # Should not throw
            @test length(fleet) == 0
        end
    end

    #==========================================================================#
    # SendResult Tests
    #==========================================================================#
    @testset "SendResult" begin
        @testset "Success result" begin
            result = RepeRPC.SendResult("node-1", UInt64(12345), nothing, 0.01)
            @test RepeRPC.succeeded(result)
            @test !RepeRPC.failed(result)
            @test result.node == "node-1"
            @test result.message_id == UInt64(12345)
            @test result.elapsed == 0.01
        end

        @testset "Failed result" begin
            err = ErrorException("Send failed")
            result = RepeRPC.SendResult("node-1", UInt64(0), err, 0.05)
            @test !RepeRPC.succeeded(result)
            @test RepeRPC.failed(result)
            @test result.error === err
        end
    end

    #==========================================================================#
    # Send Operations Tests (with real server)
    #==========================================================================#
    @testset "Send Operations" begin
        # Start a UniUDP server to receive messages
        port = 5101
        received_messages = Channel{Tuple{String, Any}}(100)

        server = RepeRPC.UniUDPServer(port;
            inactivity_timeout = 0.5,
            overall_timeout = 5.0,
            response_callback = (method, result, msg) -> nothing
        )

        RepeRPC.register(server, "/sensor/reading") do params, msg
            put!(received_messages, ("/sensor/reading", params))
            return nothing
        end

        RepeRPC.register(server, "/alert") do params, msg
            put!(received_messages, ("/alert", params))
            return nothing
        end

        RepeRPC.serve(server; async=true)
        sleep(0.2)  # Give server time to start

        try
            configs = [
                RepeRPC.UniUDPNodeConfig("127.0.0.1", port; name="server-1", tags=["sensor"]),
            ]
            fleet = RepeRPC.UniUDPFleet(configs)

            @testset "send_notify" begin
                results = RepeRPC.send_notify(fleet, "/sensor/reading", Dict("value" => 23.5))
                @test length(results) == 1
                @test haskey(results, "server-1")
                @test RepeRPC.succeeded(results["server-1"])
                @test results["server-1"].message_id > 0

                # Wait for message to be received
                sleep(0.3)

                # Check if message was received
                if isready(received_messages)
                    method, params = take!(received_messages)
                    @test method == "/sensor/reading"
                    @test params["value"] == 23.5
                end
            end

            @testset "send_notify with tags" begin
                results = RepeRPC.send_notify(fleet, "/alert", Dict("level" => "warning"); tags=["sensor"])
                @test length(results) == 1
                @test RepeRPC.succeeded(results["server-1"])

                # Non-matching tags should return empty
                results_empty = RepeRPC.send_notify(fleet, "/alert", Dict(); tags=["nonexistent"])
                @test length(results_empty) == 0
            end

            @testset "notify_all" begin
                results = RepeRPC.notify_all(fleet, "/sensor/reading", Dict("value" => 100))
                @test length(results) == 1
                @test RepeRPC.succeeded(results["server-1"])
            end

            @testset "callable syntax - broadcast" begin
                results = fleet("/sensor/reading", Dict("value" => 50))
                @test length(results) == 1
                @test RepeRPC.succeeded(results["server-1"])
            end

            @testset "callable syntax - single node" begin
                result = fleet("server-1", "/alert", Dict("level" => "info"))
                @test RepeRPC.succeeded(result)
                @test result.message_id > 0
            end

            close(fleet)

        finally
            RepeRPC.stop(server)
            close(server)
        end
    end

    #==========================================================================#
    # Multi-Node Send Tests
    #==========================================================================#
    @testset "Multi-Node Send" begin
        # Start multiple UniUDP servers
        port1 = 5201
        port2 = 5202

        received1 = Channel{Any}(10)
        received2 = Channel{Any}(10)

        server1 = RepeRPC.UniUDPServer(port1; inactivity_timeout=0.5, overall_timeout=5.0)
        server2 = RepeRPC.UniUDPServer(port2; inactivity_timeout=0.5, overall_timeout=5.0)

        RepeRPC.register(server1, "/ping") do params, msg
            put!(received1, params)
            return nothing
        end

        RepeRPC.register(server2, "/ping") do params, msg
            put!(received2, params)
            return nothing
        end

        RepeRPC.serve(server1; async=true)
        RepeRPC.serve(server2; async=true)
        sleep(0.2)

        try
            configs = [
                RepeRPC.UniUDPNodeConfig("127.0.0.1", port1; name="server-1", tags=["group-a"]),
                RepeRPC.UniUDPNodeConfig("127.0.0.1", port2; name="server-2", tags=["group-b"]),
            ]
            fleet = RepeRPC.UniUDPFleet(configs)

            @testset "Broadcast to all" begin
                results = RepeRPC.send_notify(fleet, "/ping", Dict("broadcast" => true))
                @test length(results) == 2
                @test RepeRPC.succeeded(results["server-1"])
                @test RepeRPC.succeeded(results["server-2"])
            end

            @testset "Filtered broadcast" begin
                results_a = RepeRPC.send_notify(fleet, "/ping", Dict("group" => "a"); tags=["group-a"])
                @test length(results_a) == 1
                @test haskey(results_a, "server-1")

                results_b = RepeRPC.send_notify(fleet, "/ping", Dict("group" => "b"); tags=["group-b"])
                @test length(results_b) == 1
                @test haskey(results_b, "server-2")
            end

            close(fleet)

        finally
            RepeRPC.stop(server1)
            RepeRPC.stop(server2)
            close(server1)
            close(server2)
        end
    end

    #==========================================================================#
    # Pretty Printing Tests
    #==========================================================================#
    @testset "Pretty Printing" begin
        config = RepeRPC.UniUDPNodeConfig("localhost", 5000; name="test-node", tags=["sensor"], redundancy=2)
        @test occursin("UniUDPNodeConfig", string(config))
        @test occursin("localhost", string(config))
        @test occursin("redundancy", string(config))

        configs = [RepeRPC.UniUDPNodeConfig("localhost", 5001; name="node-1")]
        fleet = RepeRPC.UniUDPFleet(configs)
        @test occursin("UniUDPFleet", string(fleet))

        result_success = RepeRPC.SendResult("node-1", UInt64(123), nothing, 0.01)
        @test occursin("msg_id", string(result_success))

        result_failed = RepeRPC.SendResult("node-1", UInt64(0), ErrorException("err"), 0.01)
        @test occursin("failed", string(result_failed))

        close(fleet)
    end

    #==========================================================================#
    # Thread Safety Tests
    #==========================================================================#
    @testset "Thread Safety" begin
        port = 5301

        server = RepeRPC.UniUDPServer(port; inactivity_timeout=0.5, overall_timeout=5.0)
        RepeRPC.register(server, "/test") do params, msg
            return nothing
        end
        RepeRPC.serve(server; async=true)
        sleep(0.2)

        try
            configs = [RepeRPC.UniUDPNodeConfig("127.0.0.1", port; name="server-1")]
            fleet = RepeRPC.UniUDPFleet(configs)

            # Concurrent sends
            tasks = Task[]
            for i in 1:10
                push!(tasks, @async begin
                    RepeRPC.send_notify(fleet, "/test", Dict("id" => i))
                end)
            end

            results = [fetch(t) for t in tasks]
            @test length(results) == 10
            @test all(r -> length(r) == 1, results)
            @test all(r -> RepeRPC.succeeded(r["server-1"]), results)

            close(fleet)

        finally
            RepeRPC.stop(server)
            close(server)
        end
    end

end
