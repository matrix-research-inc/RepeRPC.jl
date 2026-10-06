using Test
using RepeRPC
using ReplTree

@testset "remote_menu via ReplTree extension" begin

    @testset "remote_menu(client) proxies single-server calls" begin
        flat = Dict{String, Any}(
            "/name"        => () -> "kitchen",
            "/math/add"    => (;a, b) -> a + b,
            "/cook"        => food -> "cooking $(food)",
            "/sum"         => (xs...) -> sum(xs),
        )

        server = RepeRPC.Server("localhost", 18301)
        RepeRPC.serve(server, flat)
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", 18301)

        try
            client = RepeRPC.Client("localhost", 18301)
            RepeRPC.connect(client)

            menu = RepeRPC.remote_menu(client)

            @test menu.name() == "kitchen"
            @test menu.math.add(a=10, b=32) == 42
            @test menu.cook("pasta") == "cooking pasta"
            @test menu.sum(1, 2, 3, 4) == 10

            RepeRPC.disconnect(client)
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "remote_menu(fleet) broadcasts every leaf" begin
        # Two servers each tracking how many times they were called and
        # exposing the same registry shape so the schema is identical.
        function make_server(port::Int, label::String)
            calls = Ref(0)
            flat = Dict{String, Any}(
                "/label"    => () -> label,
                "/ping"     => () -> (calls[] += 1; Dict("label" => label, "count" => calls[])),
                "/echo"     => x -> Dict("label" => label, "echo" => x),
                "/sum"      => (;a, b) -> Dict("label" => label, "sum" => a + b),
                "/sum_pos"  => (xs...) -> Dict("label" => label, "sum_pos" => sum(xs)),
            )
            server = RepeRPC.Server("localhost", port)
            RepeRPC.serve(server, flat)
            RepeRPC.listen(server; async=true)
            RepeRPC.wait_for_server("localhost", port)
            return server, calls
        end

        server_a, calls_a = make_server(18302, "A")
        server_b, calls_b = make_server(18303, "B")

        configs = [
            RepeRPC.NodeConfig("localhost", 18302; name="a"),
            RepeRPC.NodeConfig("localhost", 18303; name="b"),
        ]
        fleet = RepeRPC.Fleet(configs; timeout=10.0, max_retry_attempts=1)

        try
            status = RepeRPC.connect!(fleet)
            @test sort(status.connected) == ["a", "b"]
            @test isempty(status.failed)

            menu = RepeRPC.remote_menu(fleet)

            # Zero-arg leaf broadcasts to both nodes
            results = menu.ping()
            @test results isa Dict{String, RepeRPC.RemoteResult}
            @test sort(collect(keys(results))) == ["a", "b"]
            @test all(RepeRPC.succeeded, values(results))
            @test results["a"].value["label"] == "A"
            @test results["b"].value["label"] == "B"
            @test calls_a[] == 1
            @test calls_b[] == 1

            # Single positional arg
            echo_results = menu.echo("hello")
            @test echo_results["a"].value["echo"] == "hello"
            @test echo_results["b"].value["echo"] == "hello"

            # Keyword args
            sum_results = menu.sum(a=10, b=32)
            @test sum_results["a"].value["sum"] == 42
            @test sum_results["b"].value["sum"] == 42

            # Multiple positional args -> Vector body
            sum_pos_results = menu.sum_pos(1, 2, 3, 4)
            @test sum_pos_results["a"].value["sum_pos"] == 10
            @test sum_pos_results["b"].value["sum_pos"] == 10

            RepeRPC.disconnect!(fleet)
        finally
            RepeRPC.stop(server_a)
            RepeRPC.stop(server_b)
        end
    end

    @testset "remote_menu(fleet; tags) restricts the broadcast" begin
        # Two servers; each tracks call count so we can prove the unmatched
        # node was actually skipped, not just missing from the result map.
        function make_tagged_server(port::Int, label::String)
            calls = Ref(0)
            flat = Dict{String, Any}("/who" => () -> (calls[] += 1; label))
            server = RepeRPC.Server("localhost", port)
            RepeRPC.serve(server, flat)
            RepeRPC.listen(server; async=true)
            RepeRPC.wait_for_server("localhost", port)
            return server, calls
        end

        server_a, calls_a = make_tagged_server(18304, "A")
        server_b, calls_b = make_tagged_server(18305, "B")

        configs = [
            RepeRPC.NodeConfig("localhost", 18304; name="a", tags=["primary"]),
            RepeRPC.NodeConfig("localhost", 18305; name="b", tags=["backup"]),
        ]
        fleet = RepeRPC.Fleet(configs; timeout=10.0, max_retry_attempts=1)

        try
            RepeRPC.connect!(fleet)
            primary_menu = RepeRPC.remote_menu(fleet; tags=["primary"])
            primary_results = primary_menu.who()
            @test collect(keys(primary_results)) == ["a"]
            @test primary_results["a"].value == "A"
            # /__schema__ is served by middleware without invoking handlers,
            # so calls_a only increments from the /who broadcast itself.
            @test calls_a[] == 1
            @test calls_b[] == 0  # backup tag never matched, never touched
        finally
            RepeRPC.disconnect!(fleet)
            RepeRPC.stop(server_a)
            RepeRPC.stop(server_b)
        end
    end

    @testset "remote_menu(fleet; tags) errors when no candidate matches" begin
        flat = Dict{String, Any}("/who" => () -> "A")
        server = RepeRPC.Server("localhost", 18307)
        RepeRPC.serve(server, flat)
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server("localhost", 18307)

        configs = [RepeRPC.NodeConfig("localhost", 18307; name="a", tags=["primary"])]
        fleet = RepeRPC.Fleet(configs; timeout=10.0, max_retry_attempts=1)

        try
            RepeRPC.connect!(fleet)
            @test_throws ErrorException RepeRPC.remote_menu(fleet; tags=["does-not-exist"])
        finally
            RepeRPC.disconnect!(fleet)
            RepeRPC.stop(server)
        end
    end

    @testset "remote_menu(fleet) errors with no connected nodes" begin
        configs = [RepeRPC.NodeConfig("localhost", 18306; name="dead")]
        fleet = RepeRPC.Fleet(configs; timeout=1.0, max_retry_attempts=1)
        # No server bound on 18306, so connect! will report failure
        RepeRPC.connect!(fleet)
        @test_throws ErrorException RepeRPC.remote_menu(fleet)
    end

end
