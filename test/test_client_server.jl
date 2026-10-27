@testset "Client-Server Tests" begin
    @testset "connect/listen extend Sockets" begin
        @test RepeRPC.connect === Sockets.connect
        @test RepeRPC.listen === Sockets.listen
        # Loading both packages must leave the unqualified names usable
        m = Module()
        @test Core.eval(m, :(using RepeRPC, Sockets; (connect, listen))) == (Sockets.connect, Sockets.listen)
    end

    @testset "Basic RPC" begin
        port = 9001
        server = RepeRPC.Server("localhost", port)
        
        RepeRPC.register(server, "/add", function(params, request)
            result = params["a"] + params["b"]
            return Dict("result" => result)
        end)
        
        RepeRPC.register(server, "/echo", function(params, request)
            return params
        end)
        
        # Start server in background and wait for it to be ready
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server(server.host, port)
        
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        
        try
            result = RepeRPC.send_request(client, "/add", Dict("a" => 5, "b" => 3))
            @test result["result"] == 8
            
            echo_result = RepeRPC.send_request(client, "/echo", "Hello, REPE!")
            @test echo_result == "Hello, REPE!"
            
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end
    
    @testset "Error Handling" begin
        port = 9002
        server = RepeRPC.Server("localhost", port)
        
        RepeRPC.register(server, "/divide", function(params, request)
            if params["b"] == 0
                throw(ErrorException("Division by zero"))
            end
            return Dict("result" => params["a"] / params["b"])
        end)

        RepeRPC.register(server, "/reject", function(params, request)
            throw(RepeRPC.REPEError(RepeRPC.EC_INVALID_BODY, "rejected: $(params["reason"])"))
        end)

        # Start server in background and wait for it to be ready
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server(server.host, port)

        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)

        remote_error(method, params) = try
            RepeRPC.send_request(client, method, params)
            nothing
        catch e
            e
        end

        try
            result = RepeRPC.send_request(client, "/divide", Dict("a" => 10, "b" => 2))
            @test result["result"] == 5.0

            # A generic handler exception is a parse error carrying its showerror text
            err = remote_error("/divide", Dict("a" => 1, "b" => 0))
            @test err isa RepeRPC.REPEError
            @test err.code == RepeRPC.EC_PARSE_ERROR
            @test err.message == "Division by zero"

            # A handler's REPEError reaches the client with its code and message intact
            err = remote_error("/reject", Dict("reason" => "quota"))
            @test err isa RepeRPC.REPEError
            @test err.code == RepeRPC.EC_INVALID_BODY
            @test err.message == "rejected: quota"

            err = remote_error("/nonexistent", Dict())
            @test err isa RepeRPC.REPEError
            @test err.code == RepeRPC.EC_METHOD_NOT_FOUND

        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end
    
    @testset "Notifications" begin
        port = 9003
        server = RepeRPC.Server("localhost", port)
        
        received = Ref(false)
        RepeRPC.register(server, "/notify", function(params, request)
            received[] = true
            return Dict("status" => "received")
        end)
        
        # Start server in background and wait for it to be ready
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server(server.host, port)
        
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        
        try
            RepeRPC.send_notify(client, "/notify", Dict("message" => "test"))
            sleep(1.0)
            @test received[] == true
            
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end
    
    @testset "Different Formats" begin
        port = 9004
        server = RepeRPC.Server("localhost", port)
        
        RepeRPC.register(server, "/process", function(params, request)
            if request.header.body_format == UInt16(RepeRPC.BODY_JSON)
                return Dict("format" => "json", "data" => params)
            elseif request.header.body_format == UInt16(RepeRPC.BODY_UTF8)
                return "Received text: $params"
            else
                return params
            end
        end)
        
        # Start server in background and wait for it to be ready
        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server(server.host, port)
        
        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)
        
        try
            json_result = RepeRPC.send_request(client, "/process", 
                                           Dict("test" => true),
                                           body_format = RepeRPC.BODY_JSON)
            @test json_result["format"] == "json"
            
            text_result = RepeRPC.send_request(client, "/process",
                                           "Hello",
                                           body_format = RepeRPC.BODY_UTF8)
            @test occursin("Received text", text_result)
            
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end

    @testset "Typed Responses" begin
        struct TypedResponse
            answer::Int
            message::String
        end

        port = 9005
        server = RepeRPC.Server("localhost", port)

        RepeRPC.register(server, "/typed", function(params, request)
            return Dict("answer" => 42, "message" => "life")
        end)

        RepeRPC.register(server, "/typed_beve", function(params, request)
            result = Dict("answer" => 24, "message" => "beve")
            return RepeRPC.create_response(request, result; body_format = RepeRPC.BODY_BEVE)
        end)

        RepeRPC.listen(server; async=true)
        RepeRPC.wait_for_server(server.host, port)

        client = RepeRPC.Client("localhost", port)
        RepeRPC.connect(client)

        try
            typed_result = RepeRPC.send_request(TypedResponse, client, "/typed", Dict())
            @test typed_result isa TypedResponse
            @test typed_result.answer == 42
            @test typed_result.message == "life"

            keyword_result = RepeRPC.send_request(client, "/typed", Dict(); result_type=TypedResponse)
            @test keyword_result isa TypedResponse

            task_json = RepeRPC.send_request_async(TypedResponse, client, "/typed", Dict())
            async_json_result = fetch(task_json)
            @test async_json_result isa TypedResponse

            beve_result = RepeRPC.send_request(TypedResponse, client, "/typed_beve", Dict())
            @test beve_result isa TypedResponse
            @test beve_result.answer == 24
            @test beve_result.message == "beve"

            beve_keyword = RepeRPC.send_request(client, "/typed_beve", Dict(); result_type = TypedResponse)
            @test beve_keyword isa TypedResponse

            task_beve = RepeRPC.send_request_async(client, "/typed_beve", Dict(); result_type = TypedResponse)
            async_beve_result = fetch(task_beve)
            @test async_beve_result isa TypedResponse
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(server)
        end
    end
end
