# Glaze C++ Interoperability

RepeRPC.jl is fully compatible with the C++ [Glaze](https://github.com/stephenberry/glaze) REPE implementation. The repository includes a C++ REPE server in `cpp_server/` built on Glaze's native REPE support (`glaze/rpc/repe/repe.hpp`). It demonstrates binary REPE protocol handling, JSON message parsing, error handling with exception mapping, and several RPC methods (`add`, `multiply`, `divide`, `echo`, `status`).

## Requirements

- C++20 compatible compiler
- CMake 3.15+
- Glaze (fetched automatically by CMake)

## Building the C++ Server

```bash
cd cpp_server
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

## Running Against It

```bash
# Start the C++ server
./cpp_server/build/repe_server 8081

# In another terminal, run the Julia client
julia --project=. examples/glaze_interop.jl client localhost 8081
```

```julia
client = Client("localhost", 8081)
connect(client)

result = send_request(client, "/add", Dict("a" => 10.0, "b" => 20.0); body_format = RepeRPC.JSON)
println(result["result"])  # 30.0

# C++ exceptions surface as REPE errors
try
    send_request(client, "/divide", Dict("numerator" => 1.0, "denominator" => 0.0);
                 body_format = RepeRPC.JSON)
catch e
    println("Caught division by zero error")
end
```

## Integration Tests

```bash
cd test
./run_glaze_test.sh
```

The script starts the C++ server, runs the Julia client tests against it to verify protocol compatibility, and cleans up the server process.
