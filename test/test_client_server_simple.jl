@testset "Simple Client-Server Tests" begin
    @testset "Message Round-trip" begin
        # Test that a message can be serialized and deserialized correctly
        request = RepeRPC.Message(
            id = 123,
            query = "/test/method",
            body = Dict("param" => "value"),
            body_format = UInt16(RepeRPC.BODY_JSON)
        )
        
        bytes = serialize_message(request)
        response = deserialize_message(bytes)
        
        @test response.header.id == request.header.id
        @test String(response.query) == String(request.query)
        @test response.header.body_format == request.header.body_format
    end
    
    @testset "Handler Registration" begin
        server = RepeRPC.Server("localhost", 9999)
        
        handler_called = Ref(false)
        RepeRPC.register(server, "/test", function(params, request)
            handler_called[] = true
            return Dict("result" => "ok")
        end)
        
        @test haskey(server.handlers, "/test")
        
        # Simulate processing a request
        request = RepeRPC.Message(
            id = 456,
            query = "/test",
            body = Dict("data" => "test"),
            body_format = UInt16(RepeRPC.BODY_JSON)
        )
        
        response = RepeRPC._process_request(server, request)
        @test handler_called[] == true
        @test response.header.id == 456
        @test response.header.ec == UInt32(RepeRPC.EC_OK)
    end
    
    @testset "Error Response" begin
        server = RepeRPC.Server("localhost", 9998)
        
        # Request to non-existent handler
        request = RepeRPC.Message(
            id = 789,
            query = "/nonexistent",
            body = "",
            body_format = UInt16(RepeRPC.BODY_UTF8)
        )
        
        response = RepeRPC._process_request(server, request)
        @test response.header.id == 789
        @test response.header.ec == UInt32(RepeRPC.EC_METHOD_NOT_FOUND)
        @test occursin("not found", String(response.body))
    end
    
    @testset "Middleware" begin
        server = RepeRPC.Server("localhost", 9997)
        
        middleware_called = Ref(false)
        RepeRPC.use(server, function(request)
            middleware_called[] = true
            return nothing  # Continue processing
        end)
        
        RepeRPC.register(server, "/test", function(params, request)
            return Dict("result" => "ok")
        end)
        
        request = RepeRPC.Message(
            id = 101,
            query = "/test",
            body = "",
            body_format = UInt16(RepeRPC.BODY_UTF8)
        )
        
        response = RepeRPC._process_request(server, request)
        @test middleware_called[] == true
        @test response.header.ec == UInt32(RepeRPC.EC_OK)
    end
end