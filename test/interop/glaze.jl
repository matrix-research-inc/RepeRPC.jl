# Interop suite against the Glaze C++ REPE server in cpp_server/.
# Opt-in (needs a C++20 toolchain); not run by `Pkg.test`. See docs/GlazeInterop.md.
#
#   julia --project=. test/interop/glaze.jl
#
# Set REPE_GLAZE_SERVER to use a server binary other than cpp_server/build/repe_server.

using RepeRPC
using Sockets: @ip_str, listenany
using Test

const SERVER_PATH = get(ENV, "REPE_GLAZE_SERVER",
                        joinpath(@__DIR__, "..", "..", "cpp_server", "build", "repe_server"))

isfile(SERVER_PATH) || error("""
    Glaze server binary not found at $SERVER_PATH. Build it with:
        cmake -S cpp_server -B cpp_server/build -DCMAKE_BUILD_TYPE=Release
        cmake --build cpp_server/build""")

function free_port()
    port, listener = listenany(ip"127.0.0.1", 20000)
    close(listener)
    return Int(port)
end

const HOST = "127.0.0.1"
const PORT = free_port()

server_process = run(pipeline(`$SERVER_PATH $PORT`; stdout = devnull, stderr = stderr); wait = false)

try
    wait_for_server(HOST, PORT)

    @testset "Glaze C++ interop" begin
        @testset "$(name)" for (name, fmt) in (("JSON", RepeRPC.BODY_JSON), ("BEVE", RepeRPC.BODY_BEVE))
            client = Client(HOST, PORT)
            connect(client)
            try
                request(method, params = nothing) = send_request(client, method, params; body_format = fmt)

                @test request("/add", Dict("a" => 10.0, "b" => 20.0))["result"] == 30.0
                @test request("/multiply", Dict("x" => 7.0, "y" => 6.0))["result"] == 42.0
                @test request("/divide", Dict("numerator" => 100.0, "denominator" => 4.0))["result"] == 25.0

                message = "Hello from Julia! 🎉"
                @test request("/echo", Dict("message" => message))["result"] == "Echo: $message"

                status = request("/status")
                @test status["status"] == "online"
                @test status["version"] == "1.0.0"

                # C++ exceptions and unknown methods surface as REPE errors
                err = try
                    request("/divide", Dict("numerator" => 1.0, "denominator" => 0.0))
                catch e
                    e
                end
                @test err isa REPEError
                @test err.code == RepeRPC.EC_INVALID_BODY
                @test occursin("Division by zero", err.message)

                err = try
                    request("/nonexistent", Dict{String,Float64}())
                catch e
                    e
                end
                @test err isa REPEError
                @test err.code == RepeRPC.EC_METHOD_NOT_FOUND

                # A notification gets no response and leaves the connection usable
                send_notify(client, "/echo", Dict("message" => "notification"); body_format = fmt)
                @test request("/add", Dict("a" => 1.0, "b" => 2.0))["result"] == 3.0

                tasks = [send_request_async(client, "/add", Dict("a" => Float64(i), "b" => Float64(2i));
                                            body_format = fmt) for i in 1:20]
                @test [fetch(t)["result"] for t in tasks] == [3.0i for i in 1:20]
            finally
                disconnect(client)
            end
        end
    end
finally
    kill(server_process)
    wait(server_process)
end
