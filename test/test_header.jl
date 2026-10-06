@testset "Header Tests" begin
    @testset "Header Creation" begin
        header = RepeRPC.Header()
        @test header.spec == RepeRPC.REPE_SPEC
        @test header.version == RepeRPC.REPE_VERSION
        @test header.notify == 0
        @test header.reserved == 0
        @test header.id == 0
        @test header.query_length == 0
        @test header.body_length == 0
        @test header.ec == UInt32(RepeRPC.EC_OK)
    end
    
    @testset "Header Serialization" begin
        header = RepeRPC.Header(
            length = 100,
            id = 42,
            query_length = 10,
            body_length = 42
        )
        
        bytes = RepeRPC.serialize_header(header)
        @test length(bytes) == RepeRPC.HEADER_SIZE
        
        header2 = RepeRPC.deserialize_header(bytes)
        @test header2.spec == header.spec
        @test header2.version == header.version
        @test header2.id == header.id
        @test header2.query_length == header.query_length
        @test header2.body_length == header.body_length
    end
    
    @testset "Header Validation" begin
        header = RepeRPC.Header(
            length = RepeRPC.HEADER_SIZE + 10 + 20,
            query_length = 10,
            body_length = 20
        )
        @test RepeRPC.validate_header(header) == true
        
        bad_header = RepeRPC.Header(
            length = 100,
            spec = 0x0000,
            query_length = 10,
            body_length = 20
        )
        @test RepeRPC.validate_header(bad_header) == false
        
        bad_version = RepeRPC.Header(
            length = RepeRPC.HEADER_SIZE,
            version = 0xFF
        )
        @test RepeRPC.validate_header(bad_version) == false
    end
    
    @testset "Header Endianness" begin
        header = RepeRPC.Header(
            length = 0x0102030405060708,
            id = 0x090A0B0C0D0E0F10
        )
        
        bytes = RepeRPC.serialize_header(header)
        
        @test bytes[1:8] == [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]
        
        @test bytes[17:24] == [0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0x09]
    end
end