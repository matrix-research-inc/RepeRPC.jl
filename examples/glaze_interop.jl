using RepeRPC

# Both modes speak the method set of the Glaze C++ server in cpp_server/,
# so the client runs against either that server or this file's server mode.

function connect_to_glaze_server(host::String = "localhost", port::Int = 8081)
    client = RepeRPC.Client(host, port)
    connect(client)

    println("Connected to REPE server at $host:$port")

    try
        result = send_request(client, "/echo",
                            Dict("message" => "Hello from Julia!"),
                            body_format = RepeRPC.JSON)
        println("Echo response: ", result)

        result = send_request(client, "/add",
                            Dict("a" => 42.0, "b" => 58.0),
                            body_format = RepeRPC.BEVE)
        println("Add result: ", result)

        data = send_request(client, "/status", nothing,
                          body_format = RepeRPC.JSON)
        println("Server status: ", data)

        # Server-side failures surface as REPEError
        try
            send_request(client, "/divide",
                         Dict("numerator" => 1.0, "denominator" => 0.0),
                         body_format = RepeRPC.JSON)
        catch e
            e isa REPEError || rethrow()
            println("Divide by zero rejected: ", e.message)
        end

    finally
        disconnect(client)
    end
end

function create_compatible_server(port::Int = 8082)
    server = RepeRPC.Server("0.0.0.0", port)

    RepeRPC.register(server, "/echo", function(params, request)
        return Dict("result" => "Echo: " * params["message"])
    end)

    RepeRPC.register(server, "/add", function(params, request)
        return Dict("result" => params["a"] + params["b"])
    end)

    RepeRPC.register(server, "/divide", function(params, request)
        params["denominator"] == 0 && throw(REPEError(RepeRPC.EC_INVALID_BODY, "Division by zero"))
        return Dict("result" => params["numerator"] / params["denominator"])
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
