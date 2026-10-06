using RepeRPC
using Sockets

function connect_to_glaze_server(host::String = "localhost", port::Int = 8081)
    client = RepeRPC.Client(host, port)
    connect(client)
    
    println("Connected to Glaze C++ server at $host:$port")
    
    try
        result = send_request(client, "/test/echo", 
                            Dict("message" => "Hello from Julia!"),
                            body_format = RepeRPC.JSON)
        println("Echo response: ", result)
        
        result = send_request(client, "/math/add",
                            Dict("a" => 42, "b" => 58),
                            body_format = RepeRPC.JSON)
        println("Math result: ", result)
        
        data = send_request(client, "/status", nothing,
                          query_format = QUERY_JSON_POINTER,
                          body_format = RepeRPC.JSON)
        println("Server status: ", data)
        
    catch e
        println("Error: ", e)
    finally
        disconnect(client)
    end
end

function create_compatible_server(port::Int = 8082)
    server = RepeRPC.Server("0.0.0.0", port)
    
    RepeRPC.register(server, "/test/echo", function(params, request)
        return params
    end)
    
    RepeRPC.register(server, "/math/add", function(params, request)
        a = get(params, "a", 0)
        b = get(params, "b", 0)
        return Dict("result" => a + b)
    end)
    
    RepeRPC.register(server, "/status", function(params, request)
        return Dict(
            "status" => "online",
            "version" => "RepeRPC.jl",
            "language" => "Julia",
            "timestamp" => time()
        )
    end)
    
    println("Starting Julia REPE server on port $port (Glaze-compatible)...")
    listen(server)
end

if length(ARGS) > 0
    if ARGS[1] == "client"
        host = get(ARGS, 2, "localhost")
        port = parse(Int, get(ARGS, 3, "8081"))
        connect_to_glaze_server(host, port)
    elseif ARGS[1] == "server"
        port = parse(Int, get(ARGS, 2, "8082"))
        create_compatible_server(port)
    else
        println("Usage: julia glaze_interop.jl [client|server] [host/port] [port]")
    end
else
    println("Usage: julia glaze_interop.jl [client|server] [host/port] [port]")
    println("  client mode: Connect to a Glaze C++ server")
    println("  server mode: Start a Glaze-compatible Julia server")
end
