# WebSocket transport tests
# Requires: import HTTP (loaded by runtests.jl to trigger extension)

@testset "WebSocket Transport" begin

    @testset "Basic RPC over WebSocket" begin
        server = RepeRPC.WSServer("127.0.0.1", 0)

        RepeRPC.register(server, "/add", function(params, request)
            return Dict("result" => params["a"] + params["b"])
        end)

        RepeRPC.register(server, "/echo", function(params, request)
            return params
        end)

        RepeRPC.listen(server; async=true)

        try
            client = RepeRPC.WSClient("127.0.0.1", server.port)
            RepeRPC.connect(client)

            try
                result = RepeRPC.send_request(client, "/add", Dict("a" => 10, "b" => 20))
                @test result["result"] == 30

                data = Dict("hello" => "world", "num" => 42)
                result = RepeRPC.send_request(client, "/echo", data)
                @test result["hello"] == "world"
                @test result["num"] == 42

                # A server error now surfaces as a structured REPEError (was a bare
                # ErrorException), matching the TCP demux, so callers can switch on `.code`.
                @test_throws RepeRPC.REPEError RepeRPC.send_request(client, "/nonexistent", nothing)
            finally
                RepeRPC.disconnect(client)
            end
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "WebSocket Notifications" begin
        received = Ref(false)

        server = RepeRPC.WSServer("127.0.0.1", 0)
        RepeRPC.register(server, "/notify_test", function(params, request)
            received[] = true
            return Dict("ok" => true)
        end)

        RepeRPC.listen(server; async=true)

        try
            client = RepeRPC.WSClient("127.0.0.1", server.port)
            RepeRPC.connect(client)

            try
                RepeRPC.send_notify(client, "/notify_test", Dict("data" => "test"))
                sleep(0.3)
                @test received[]
            finally
                RepeRPC.disconnect(client)
            end
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "WebSocket Body Formats" begin
        server = RepeRPC.WSServer("127.0.0.1", 0)

        RepeRPC.register(server, "/echo", function(params, request)
            return params
        end)

        RepeRPC.listen(server; async=true)

        try
            client = RepeRPC.WSClient("127.0.0.1", server.port)
            RepeRPC.connect(client)

            try
                # JSON format
                result = RepeRPC.send_request(client, "/echo", Dict("format" => "json"),
                                           body_format=RepeRPC.BODY_JSON)
                @test result["format"] == "json"

                # BEVE format
                result = RepeRPC.send_request(client, "/echo", Dict("format" => "beve"),
                                           body_format=RepeRPC.BEVE)
                @test result["format"] == "beve"

                # UTF8 format
                result = RepeRPC.send_request(client, "/echo", "hello utf8",
                                           body_format=RepeRPC.BODY_UTF8)
                @test result == "hello utf8"
            finally
                RepeRPC.disconnect(client)
            end
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "WebSocket Registry" begin
        server = RepeRPC.WSServer("127.0.0.1", 0)

        registry = RepeRPC.Registry(
            "counter" => 0,
            "config" => Dict("timeout" => 30, "retries" => 3),
            "add" => (;a, b) -> a + b
        )

        RepeRPC.serve(server, registry)
        RepeRPC.listen(server; async=true)

        try
            client = RepeRPC.WSClient("127.0.0.1", server.port)
            RepeRPC.connect(client)

            try
                # Read value
                result = RepeRPC.send_request(client, "/counter", nothing)
                @test result == 0

                # Read nested value
                result = RepeRPC.send_request(client, "/config/timeout", nothing)
                @test result == 30

                # Call function
                result = RepeRPC.send_request(client, "/add", Dict("a" => 5, "b" => 3))
                @test result == 8

                # Write value
                RepeRPC.send_request(client, "/counter", 42)
                result = RepeRPC.send_request(client, "/counter", nothing)
                @test result == 42
            finally
                RepeRPC.disconnect(client)
            end
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "WebSocket Async and Batch" begin
        server = RepeRPC.WSServer("127.0.0.1", 0)

        RepeRPC.register(server, "/square", function(params, request)
            return Dict("result" => params["x"]^2)
        end)

        RepeRPC.listen(server; async=true)

        try
            client = RepeRPC.WSClient("127.0.0.1", server.port)
            RepeRPC.connect(client)

            try
                # Async request
                task = RepeRPC.send_request_async(client, "/square", Dict("x" => 5))
                result = fetch(task)
                @test result["result"] == 25

                # Batch requests
                requests = [
                    ("/square", Dict("x" => 2)),
                    ("/square", Dict("x" => 3)),
                    ("/square", Dict("x" => 4))
                ]
                tasks = RepeRPC.batch(client, requests)
                results = RepeRPC.await_batch(tasks)
                @test results[1]["result"] == 4
                @test results[2]["result"] == 9
                @test results[3]["result"] == 16
            finally
                RepeRPC.disconnect(client)
            end
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "WebSocket Multiple Clients" begin
        server = RepeRPC.WSServer("127.0.0.1", 0)

        RepeRPC.register(server, "/identity", function(params, request)
            return params
        end)

        RepeRPC.listen(server; async=true)

        try
            client1 = RepeRPC.WSClient("127.0.0.1", server.port)
            client2 = RepeRPC.WSClient("127.0.0.1", server.port)
            RepeRPC.connect(client1)
            RepeRPC.connect(client2)

            try
                result1 = RepeRPC.send_request(client1, "/identity", Dict("client" => 1))
                result2 = RepeRPC.send_request(client2, "/identity", Dict("client" => 2))
                @test result1["client"] == 1
                @test result2["client"] == 2
            finally
                RepeRPC.disconnect(client1)
                RepeRPC.disconnect(client2)
            end
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "WebSocket Concurrent Requests Across Clients" begin
        server = RepeRPC.WSServer("127.0.0.1", 0)

        RepeRPC.register(server, "/slow", function(params, request)
            sleep(params["duration"])
            return Dict("client" => params["client"], "done" => true)
        end)

        RepeRPC.listen(server; async=true)

        try
            client1 = RepeRPC.WSClient("127.0.0.1", server.port; timeout=10.0)
            client2 = RepeRPC.WSClient("127.0.0.1", server.port; timeout=10.0)
            RepeRPC.connect(client1)
            RepeRPC.connect(client2)

            try
                # Client 1 sends a slow request; client 2 should not be blocked by it
                task1 = @async RepeRPC.send_request(client1, "/slow", Dict("client" => 1, "duration" => 1.0))
                sleep(0.05) # Ensure slow request is in-flight
                task2 = @async RepeRPC.send_request(client2, "/slow", Dict("client" => 2, "duration" => 0.0))

                result2 = fetch(task2)
                @test result2["client"] == 2
                @test result2["done"] == true

                result1 = fetch(task1)
                @test result1["client"] == 1
                @test result1["done"] == true
            finally
                RepeRPC.disconnect(client1)
                RepeRPC.disconnect(client2)
            end
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "WebSocket Concurrent Requests Same Client" begin
        server = RepeRPC.WSServer("127.0.0.1", 0)

        RepeRPC.register(server, "/slow", function(params, request)
            sleep(params["duration"])
            return Dict("id" => params["id"], "done" => true)
        end)

        RepeRPC.listen(server; async=true)

        try
            client = RepeRPC.WSClient("127.0.0.1", server.port; timeout=10.0)
            RepeRPC.connect(client)

            try
                # Send a slow request and a fast request on the same connection.
                # With async processing, the fast request should complete first.
                task_slow = @async RepeRPC.send_request(client, "/slow", Dict("id" => "slow", "duration" => 1.0))
                sleep(0.05)
                task_fast = @async RepeRPC.send_request(client, "/slow", Dict("id" => "fast", "duration" => 0.0))

                result_fast = fetch(task_fast)
                @test result_fast["id"] == "fast"

                result_slow = fetch(task_slow)
                @test result_slow["id"] == "slow"
            finally
                RepeRPC.disconnect(client)
            end
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "WebSocket Connection Lifecycle" begin
        server = RepeRPC.WSServer("127.0.0.1", 0)

        RepeRPC.register(server, "/ping", function(params, request)
            return Dict("pong" => true)
        end)

        RepeRPC.listen(server; async=true)

        try
            client = RepeRPC.WSClient("127.0.0.1", server.port)
            @test !RepeRPC.isconnected(client)

            RepeRPC.connect(client)
            @test RepeRPC.isconnected(client)

            result = RepeRPC.send_request(client, "/ping", nothing)
            @test result["pong"] == true

            RepeRPC.disconnect(client)
            @test !RepeRPC.isconnected(client)

            # Reconnect
            RepeRPC.connect(client)
            @test RepeRPC.isconnected(client)

            result = RepeRPC.send_request(client, "/ping", nothing)
            @test result["pong"] == true

            RepeRPC.disconnect(client)
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "WebSocket Middleware" begin
        server = RepeRPC.WSServer("127.0.0.1", 0)

        middleware_called = Ref(false)
        RepeRPC.use(server, function(request)
            middleware_called[] = true
            return nothing  # Continue processing
        end)

        RepeRPC.register(server, "/test", function(params, request)
            return Dict("ok" => true)
        end)

        RepeRPC.listen(server; async=true)

        try
            client = RepeRPC.WSClient("127.0.0.1", server.port)
            RepeRPC.connect(client)

            try
                result = RepeRPC.send_request(client, "/test", nothing)
                @test result["ok"] == true
                @test middleware_called[]
            finally
                RepeRPC.disconnect(client)
            end
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "WebSocket URL Constructor" begin
        client = RepeRPC.WSClient("ws://localhost:8080")
        @test !RepeRPC.isconnected(client)

        client2 = RepeRPC.WSClient("localhost", 8080)
        @test !RepeRPC.isconnected(client2)
    end

    @testset "HTTP Handler" begin
        handler_called = Ref(false)
        handler_path = Ref("")

        http_handler = function(http)
            target = http.message.target
            handler_path[] = target

            if startswith(target, "/custom/")
                handler_called[] = true
                HTTP.setstatus(http, 200)
                HTTP.setheader(http, "Content-Type" => "text/plain")
                HTTP.startwrite(http)
                Base.write(http, "custom response")
                return true
            end
            return false  # not handled, fall through to default
        end

        server = RepeRPC.WSServer("127.0.0.1", 0; html="<h1>Home</h1>", http_handler=http_handler)
        RepeRPC.register(server, "/ping", (params, request) -> Dict("pong" => true))
        RepeRPC.listen(server; async=true)

        try
            # Custom handler route returns custom response
            resp = HTTP.get("http://127.0.0.1:$(server.port)/custom/test"; status_exception=false)
            @test resp.status == 200
            @test String(resp.body) == "custom response"
            @test handler_called[]
            @test handler_path[] == "/custom/test"

            # Unhandled route falls through to HTML
            handler_called[] = false
            resp = HTTP.get("http://127.0.0.1:$(server.port)/other"; status_exception=false)
            @test resp.status == 200
            @test String(resp.body) == "<h1>Home</h1>"

            # WebSocket still works alongside HTTP handler
            client = RepeRPC.WSClient("127.0.0.1", server.port)
            RepeRPC.connect(client)
            try
                result = RepeRPC.send_request(client, "/ping", nothing)
                @test result["pong"] == true
            finally
                RepeRPC.disconnect(client)
            end
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "HTTP Handler Not Provided" begin
        # Default behavior: no http_handler, HTML served
        server = RepeRPC.WSServer("127.0.0.1", 0; html="<h1>Default</h1>")
        RepeRPC.listen(server; async=true)

        try
            resp = HTTP.get("http://127.0.0.1:$(server.port)/"; status_exception=false)
            @test resp.status == 200
            @test String(resp.body) == "<h1>Default</h1>"
        finally
            RepeRPC.stop(server)
        end
    end

    @testset "WebSocket Flat Registry" begin
        server = RepeRPC.WSServer("127.0.0.1", 0)

        flat_registry = Dict{String, Any}(
            "/name" => () -> "TestApp",
            "/add" => (;a, b) -> a + b,
            "/counter" => 0
        )

        RepeRPC.serve(server, flat_registry)
        RepeRPC.listen(server; async=true)

        try
            client = RepeRPC.WSClient("127.0.0.1", server.port)
            RepeRPC.connect(client)

            try
                # Read callable (call_on_read=true by default for flat registries)
                result = RepeRPC.send_request(client, "/name", nothing)
                @test result == "TestApp"

                # Call function
                result = RepeRPC.send_request(client, "/add", Dict("a" => 10, "b" => 5))
                @test result == 15

                # Read value
                result = RepeRPC.send_request(client, "/counter", nothing)
                @test result == 0
            finally
                RepeRPC.disconnect(client)
            end
        finally
            RepeRPC.stop(server)
        end
    end


    @testset "peer close surfaces ConnectionError, not a timeout" begin
        # The wait used to poll `isready(response_channel)` until the timeout
        # expired. A closed, empty Channel is never `isready`, so the listener's
        # teardown -- which reports a dropped connection by closing exactly that
        # channel -- was invisible: the caller sat out the full timeout and was
        # then told the request had timed out. It also mattered downstream, since
        # `Fleet` retries a ConnectionError and does not retry a timeout.
        #
        # A raw WebSocket peer is used rather than a RepeRPC.WSServer so the close is
        # genuinely peer-initiated and lands at a known point: after the request
        # frame has arrived and before any response.
        srv = HTTP.WebSockets.listen!("127.0.0.1", 0) do ws
            for _ in ws
                break                      # the request landed...
            end
            close(ws)                      # ...and the peer hangs up on it
        end
        port = parse(Int, rsplit(HTTP.WebSockets.server_addr(srv), ':'; limit = 2)[2])

        try
            client = RepeRPC.WSClient("127.0.0.1", port; timeout=30.0)
            RepeRPC.connect(client)
            t0 = time()
            err = try
                RepeRPC.send_request(client, "/anything", nothing)
                :no_error
            catch e
                e
            end
            elapsed = time() - t0

            @test err isa RepeRPC.ConnectionError
            @test elapsed < 5.0            # woken by the close, not by the 30 s budget

            try; RepeRPC.disconnect(client); catch; end
        finally
            close(srv)
        end
    end

end
