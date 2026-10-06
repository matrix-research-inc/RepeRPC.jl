@testset "Concurrent Client Tests" begin
    
    @testset "Thread-safe ID generation" begin
        client = RepeRPC.Client("localhost", 9876)
        
        # Generate IDs from multiple threads
        ids = Vector{UInt64}(undef, 1000)
        Threads.@threads for i in 1:1000
            ids[i] = RepeRPC._get_next_id(client)
        end
        
        # All IDs should be unique
        @test length(unique(ids)) == 1000
        @test minimum(ids) >= 1  # Starting from 1 (atomic_add returns old value)
        @test maximum(ids) <= 1000
    end
    
    @testset "Concurrent requests simulation" begin
        server = RepeRPC.Server("localhost", 9877)
        
        # Register a handler that simulates work
        RepeRPC.register(server, "/compute", function(params, request)
            # Simulate some computation time
            sleep(0.01)
            value = get(params, "value", 0)
            return Dict("result" => value * 2)
        end)
        
        # Start server in background
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server(server.host, server.port)
        
        client = RepeRPC.Client("localhost", 9877)
        RepeRPC.connect(client)
        
        try
            # Launch many concurrent requests
            n_requests = 50
            tasks = Task[]
            
            for i in 1:n_requests
                task = RepeRPC.send_request_async(client, "/compute", 
                                              Dict("value" => i),
                                              body_format = RepeRPC.BODY_JSON)
                push!(tasks, task)
            end
            
            # Wait for all tasks to complete
            results = [fetch(task) for task in tasks]
            
            # Verify all results are correct
            for i in 1:n_requests
                @test results[i]["result"] == i * 2
            end
            
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end
    
    @testset "Batch async requests" begin
        server = RepeRPC.Server("localhost", 9878)
        
        RepeRPC.register(server, "/add", function(params, request)
            return Dict("result" => params["a"] + params["b"])
        end)
        
        RepeRPC.register(server, "/multiply", function(params, request)
            return Dict("result" => params["x"] * params["y"])
        end)
        
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server(server.host, server.port)
        
        client = RepeRPC.Client("localhost", 9878)
        RepeRPC.connect(client)
        
        try
            # Create batch of different requests
            requests = [
                ("/add", Dict("a" => 1, "b" => 2)),
                ("/multiply", Dict("x" => 3, "y" => 4)),
                ("/add", Dict("a" => 5, "b" => 6)),
                ("/multiply", Dict("x" => 7, "y" => 8)),
            ]
            
            # Send all requests concurrently
            tasks = RepeRPC.batch(client, requests, body_format = RepeRPC.BODY_JSON)
            
            # Fetch all results
            results = RepeRPC.await_batch(tasks)
            
            @test results[1]["result"] == 3   # 1 + 2
            @test results[2]["result"] == 12  # 3 * 4
            @test results[3]["result"] == 11  # 5 + 6
            @test results[4]["result"] == 56  # 7 * 8
            
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end
    
    @testset "Thread safety under load" begin
        server = RepeRPC.Server("localhost", 9879)
        
        counter = Threads.Atomic{Int}(0)
        RepeRPC.register(server, "/increment", function(params, request)
            Threads.atomic_add!(counter, 1)
            return Dict("count" => counter[])
        end)
        
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server(server.host, server.port)
        
        client = RepeRPC.Client("localhost", 9879)
        RepeRPC.connect(client)
        
        try
            # Multiple threads making requests simultaneously
            n_threads = 4
            requests_per_thread = 25
            
            tasks = Task[]
            for t in 1:n_threads
                task = @async begin
                    thread_results = []
                    for i in 1:requests_per_thread
                        result = RepeRPC.send_request(client, "/increment", 
                                                  Dict(), 
                                                  body_format = RepeRPC.BODY_JSON)
                        push!(thread_results, result["count"])
                    end
                    thread_results
                end
                push!(tasks, task)
            end
            
            # Collect all results
            all_results = []
            for task in tasks
                thread_results = fetch(task)
                append!(all_results, thread_results)
            end
            
            # Should have received all numbers from 1 to total requests
            @test length(all_results) == n_threads * requests_per_thread
            @test counter[] == n_threads * requests_per_thread
            
            # All counts should be unique (no race conditions)
            @test length(unique(all_results)) == length(all_results)
            
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end
    
    @testset "Concurrent mixed operations" begin
        server = RepeRPC.Server("localhost", 9880)
        
        RepeRPC.register(server, "/echo", function(params, request)
            return params
        end)
        
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server(server.host, server.port)
        
        client = RepeRPC.Client("localhost", 9880)
        RepeRPC.connect(client)
        
        try
            # Mix of async requests and notifications
            tasks = Task[]
            
            # Async requests
            for i in 1:10
                task = RepeRPC.send_request_async(client, "/echo",
                                              Dict("id" => i),
                                              body_format = RepeRPC.BODY_JSON)
                push!(tasks, task)
            end
            
            # Notifications (fire and forget)
            for i in 1:10
                RepeRPC.send_notify(client, "/echo",
                               Dict("notify_id" => i),
                               body_format = RepeRPC.BODY_JSON)
            end
            
            # More async requests
            for i in 11:20
                task = RepeRPC.send_request_async(client, "/echo",
                                              Dict("id" => i),
                                              body_format = RepeRPC.BODY_JSON)
                push!(tasks, task)
            end
            
            # Verify all async requests completed successfully
            results = [fetch(task) for task in tasks]
            for i in 1:20
                if i <= 10
                    @test results[i]["id"] == i
                else
                    @test results[i]["id"] == i
                end
            end
            
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end
    
    @testset "Error handling in concurrent requests" begin
        server = RepeRPC.Server("localhost", 9881)
        
        RepeRPC.register(server, "/maybe_fail", function(params, request)
            value = get(params, "value", 0)
            if value % 3 == 0
                throw(ErrorException("Simulated error for value $value"))
            end
            return Dict("result" => value * 2)
        end)
        
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server(server.host, server.port)
        
        client = RepeRPC.Client("localhost", 9881)
        RepeRPC.connect(client)
        
        try
            tasks = Task[]
            for i in 1:30
                task = RepeRPC.send_request_async(client, "/maybe_fail",
                                              Dict("value" => i),
                                              body_format = RepeRPC.BODY_JSON)
                push!(tasks, task)
            end
            
            # Check results and errors
            success_count = 0
            error_count = 0
            
            for (i, task) in enumerate(tasks)
                try
                    result = fetch(task)
                    @test result["result"] == i * 2
                    success_count += 1
                catch e
                    # Should fail for multiples of 3
                    @test i % 3 == 0
                    error_count += 1
                end
            end
            
            @test success_count == 20  # 30 - 10 multiples of 3
            @test error_count == 10    # 10 multiples of 3
            
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end
end
