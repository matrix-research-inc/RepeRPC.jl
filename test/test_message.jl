@testset "Message Tests" begin
    @testset "Message Creation" begin
        msg = RepeRPC.Message(
            id = 123,
            query = "test/method",
            body = "hello world"
        )
        
        @test msg.header.id == 123
        @test String(msg.query) == "test/method"
        @test String(msg.body) == "hello world"
        @test msg.header.query_length == length("test/method")
        @test msg.header.body_length == length("hello world")
    end
    
    @testset "Message Serialization" begin
        msg = RepeRPC.Message(
            id = 456,
            query = "/api/test",
            body = Dict("key" => "value"),
            body_format = UInt16(RepeRPC.BODY_JSON)
        )
        
        bytes = serialize_message(msg)
        @test length(bytes) == msg.header.length
        
        msg2 = deserialize_message(bytes)
        @test msg2.header.id == msg.header.id
        @test String(msg2.query) == String(msg.query)
        @test msg2.header.body_format == UInt16(RepeRPC.BODY_JSON)
    end
    
    @testset "Body Encoding" begin
        json_data = Dict("test" => 123, "array" => [1, 2, 3])
        json_bytes = RepeRPC.encode_body(json_data, RepeRPC.BODY_JSON)
        @test !isempty(json_bytes)
        
        text_data = "Hello, REPE!"
        text_bytes = RepeRPC.encode_body(text_data, RepeRPC.BODY_UTF8)
        @test String(text_bytes) == text_data
        
        binary_data = UInt8[1, 2, 3, 4, 5]
        binary_bytes = RepeRPC.encode_body(binary_data, RepeRPC.BODY_RAW_BINARY)
        @test binary_bytes == binary_data
    end
    
    @testset "Body Parsing" begin
        json_msg = RepeRPC.Message(
            query = "test",
            body = Dict("result" => 42),
            body_format = UInt16(RepeRPC.BODY_JSON)
        )
        parsed = parse_body(json_msg)
        @test parsed["result"] == 42
        
        text_msg = RepeRPC.Message(
            query = "test",
            body = "plain text",
            body_format = UInt16(RepeRPC.BODY_UTF8)
        )
        parsed = parse_body(text_msg)
        @test parsed == "plain text"
    end
    
    @testset "Error Messages" begin
        error_msg = RepeRPC.create_error_message(RepeRPC.EC_METHOD_NOT_FOUND, "Custom error")
        @test error_msg.header.ec == UInt32(RepeRPC.EC_METHOD_NOT_FOUND)
        @test String(error_msg.body) == "Custom error"
        
        error_msg2 = RepeRPC.create_error_message(RepeRPC.EC_PARSE_ERROR)
        @test error_msg2.header.ec == UInt32(RepeRPC.EC_PARSE_ERROR)
        @test String(error_msg2.body) == "Parse error"
    end
    
    @testset "Notify Messages" begin
        msg = RepeRPC.Message(
            query = "notification",
            body = "data",
            notify = true
        )
        @test msg.header.notify == 0x01
        
        msg2 = RepeRPC.Message(
            query = "request",
            body = "data",
            notify = false
        )
        @test msg2.header.notify == 0x00
    end
end