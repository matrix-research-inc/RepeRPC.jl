using Test
using RepeRPC
using Sockets

@testset "RepeRPC.jl Tests" begin
    include("test_header.jl")
    include("test_message.jl")
    include("test_beve.jl")
    include("test_client_server_simple.jl")
    include("test_concurrent_simple.jl")
    include("test_client_server.jl")
    include("test_concurrent.jl")
    include("test_uniudp.jl")
    include("test_uniudp_integration.jl")
    include("test_fleet.jl")
    include("test_uniudp_fleet.jl")
    include("test_registry.jl")
    include("test_streaming.jl")
    include("test_value_stream.jl")
    include("test_file_stream.jl")
    include("test_connection_failure.jl")
end

# WebSocket tests (require HTTP.jl)
# Use `import` (not `using`) to avoid name conflicts with RepeRPC exports (connect, listen)
if Base.find_package("HTTP") !== nothing
    import HTTP
    include("test_websocket.jl")
    include("test_websocket_auth.jl")
    include("test_streaming_websocket.jl")
    include("test_value_stream_websocket.jl")
end

# ReplTree extension tests (require ReplTree.jl)
if Base.find_package("ReplTree") !== nothing
    include("test_repltree_remote_menu.jl")
end
