"""
WebSocket REPE Demo

Run with:
    julia --project=. examples/websocket_demo.jl

Then open http://localhost:8080 in your browser.
"""

using RepeRPC
using Dates
import HTTP

const start_time = time()
const counter = Ref(0)

server = RepeRPC.WSServer("0.0.0.0", 8080;
    html = read(joinpath(@__DIR__, "index.html"), String))

RepeRPC.serve(server, Dict{String, Any}(
    "/counter"   => (val...) -> (isempty(val) ? counter[] : counter[] = Int(first(val))),
    "/increment" => () -> (counter[] += 1),
    "/decrement" => () -> (counter[] -= 1),
    "/add"       => (;a, b) -> a + b,
    "/echo"      => (params) -> params,
    "/time"      => () -> string(Dates.now()),
    "/uptime"    => () -> round(time() - start_time, digits=1),
))

println("Starting REPE WebSocket demo server...")
println("Open http://localhost:8080 in your browser")
println("Press Ctrl+C to stop")

RepeRPC.listen(server)
