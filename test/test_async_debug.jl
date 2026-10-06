#!/usr/bin/env julia

using RepeRPC
using Base.Threads

println("Testing async functionality...")
println("Threads available: ", Threads.nthreads())

# Create server
server = RepeRPC.Server("localhost", 9999)

# Register a simple handler
RepeRPC.register(server, "/echo", function(params, request)
    println("Server received: ", params)
    return params
end)

# Start server
@async RepeRPC._start_server(server)
sleep(2)  # Give server more time to start

# Create client
client = RepeRPC.Client("localhost", 9999)
RepeRPC.connect(client)

try
    println("\n1. Testing synchronous request...")
    result = RepeRPC.send_request(client, "/echo", Dict("test" => "sync"), body_format = RepeRPC.BODY_JSON)
    println("Sync result: ", result)
    
    println("\n2. Testing asynchronous request...")
    task = RepeRPC.send_request_async(client, "/echo", Dict("test" => "async"), body_format = RepeRPC.BODY_JSON)
    println("Task created: ", task)
    result = fetch(task)
    println("Async result: ", result)
    
    println("\n3. Testing multiple async requests...")
    tasks = []
    for i in 1:3
        task = RepeRPC.send_request_async(client, "/echo", Dict("id" => i), body_format = RepeRPC.BODY_JSON)
        push!(tasks, task)
        println("Created task $i")
    end
    
    for (i, task) in enumerate(tasks)
        result = fetch(task)
        println("Task $i result: ", result)
    end
    
    println("\nAll tests completed successfully!")
    
finally
    RepeRPC.disconnect(client)
    RepeRPC._stop_server(server)
end