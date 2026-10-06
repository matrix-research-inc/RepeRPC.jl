# WebSocket Transport

The WebSocket transport allows web browsers and other WebSocket clients to communicate with REPE servers using the native binary protocol. Each WebSocket binary frame carries a complete REPE message (48-byte header + query + body) with zero translation overhead.

## Features

- Binary WebSocket frames carrying raw REPE messages — no protocol translation
- Same handler/middleware pipeline as the TCP server
- Registry and flat registry serving via `serve()`
- Optional HTML serving for self-contained web applications
- Async and batch request support
- Multiple concurrent client connections
- Auto-assigned port support (port 0)
- Proper 426 Upgrade Required response for non-WebSocket HTTP requests

## Requirements

The WebSocket transport is provided as a Julia package extension that loads automatically when HTTP.jl is available:

```julia
using RepeRPC
import HTTP  # triggers the extension
```

## Server

### Quick Start

```julia
using RepeRPC
import HTTP

server = WSServer("0.0.0.0", 8080)

RepeRPC.register(server, "/add", function(params, request)
    return params["a"] + params["b"]
end)

RepeRPC.listen(server)
```

### Constructor

```julia
WSServer(host="localhost", port=8080; print_stacktrace=false, html="")
```

- `host`: Address to bind to. Use `"0.0.0.0"` to listen on all interfaces.
- `port`: Port number. Use `0` for OS-assigned port (the actual port is stored in `server.port` after `listen`).
- `print_stacktrace`: Log full stack traces on handler errors.
- `html`: HTML string to serve for non-WebSocket HTTP requests. When empty, returns 426 Upgrade Required.

### Registering Handlers

Handlers are registered the same way as the TCP server:

```julia
RepeRPC.register(server, "/method", function(params, request)
    # params: parsed request body
    # request: the raw REPE Message
    return result
end)
```

### Middleware

```julia
RepeRPC.use(server, function(request)
    # Return nothing to continue, ErrorCode to reject, or Message to short-circuit
    return nothing
end)
```

### Serving a Registry

```julia
registry = Registry(
    "counter" => 0,
    "add"     => (;a, b) -> a + b,
)

RepeRPC.serve(server, registry; call_on_read=true)
```

### Serving a Flat Registry (Dict)

```julia
counter = Ref(0)

RepeRPC.serve(server, Dict{String, Any}(
    "/counter"   => (val...) -> (isempty(val) ? counter[] : counter[] = Int(first(val))),
    "/increment" => () -> (counter[] += 1),
    "/add"       => (;a, b) -> a + b,
))
```

With flat registries, `call_on_read=true` is the default — callable endpoints are invoked when accessed with an empty body.

### Listening

```julia
# Blocking
RepeRPC.listen(server)

# Non-blocking
RepeRPC.listen(server; async=true)

# Stop the server
RepeRPC.stop(server)
```

### Auto-Assigned Ports

Use port `0` to let the OS assign a free port. After `listen`, the actual port is available on the server:

```julia
server = WSServer("127.0.0.1", 0)
RepeRPC.listen(server; async=true)
println("Listening on port: ", server.port)
```

## Client (Julia)

A Julia WebSocket client is provided for testing and non-browser use cases.

### Quick Start

```julia
using RepeRPC
import HTTP

client = WSClient("localhost", 8080)
RepeRPC.connect(client)

result = RepeRPC.send_request(client, "/add", Dict("a" => 10, "b" => 20))
println(result)  # 30

RepeRPC.disconnect(client)
```

### Constructor

```julia
WSClient(host, port; timeout=30.0)
WSClient(url; timeout=30.0)  # e.g. "ws://localhost:8080" or "wss://..."
```

### Requests

```julia
# Synchronous
result = RepeRPC.send_request(client, "/method", params)

# With options
result = RepeRPC.send_request(client, "/method", params;
    body_format=RepeRPC.BEVE,
    timeout=10.0)

# Async — returns a Task
task = RepeRPC.send_request_async(client, "/method", params)
result = fetch(task)

# Notification (fire-and-forget)
RepeRPC.send_notify(client, "/log", Dict("event" => "click"))

# Batch
requests = [("/add", Dict("a" => 1, "b" => 2)),
            ("/add", Dict("a" => 3, "b" => 4))]
tasks = RepeRPC.batch(client, requests)
results = RepeRPC.await_batch(tasks)
```

### Connection Lifecycle

```julia
RepeRPC.connect(client)
RepeRPC.isconnected(client)  # true
RepeRPC.disconnect(client)

# Reconnect is supported
RepeRPC.connect(client)
```

## Serving HTML

Pass an `html` string to `WSServer` to serve a web page on the same port. Non-WebSocket HTTP requests receive the HTML; WebSocket upgrade requests get the binary REPE protocol.

```julia
html = read("index.html", String)
server = WSServer("0.0.0.0", 8080; html=html)
RepeRPC.serve(server, handlers)
RepeRPC.listen(server)
```

Open `http://localhost:8080` in a browser to load the page. The page connects back via `ws://localhost:8080` for RPC.

When no `html` is set, non-WebSocket requests receive `426 Upgrade Required` with an `Upgrade: websocket` header.

## Browser JavaScript Client

The browser connects via WebSocket and sends raw REPE binary messages using `ArrayBuffer`. The REPE header is 48 bytes, little-endian:

| Offset | Size | Field | Value |
|--------|------|-------|-------|
| 0 | 8 | length | Total message length |
| 8 | 2 | spec | `0x1507` |
| 10 | 1 | version | `1` |
| 11 | 1 | notify | `0` (request) or `1` (fire-and-forget) |
| 12 | 4 | reserved | `0` |
| 16 | 8 | id | Request identifier |
| 24 | 8 | query_length | Length of query section |
| 32 | 8 | body_length | Length of body section |
| 40 | 2 | query_format | `1` (JSON Pointer) |
| 42 | 2 | body_format | `2` (JSON) |
| 44 | 4 | ec | Error code (`0` = OK) |

### Minimal JavaScript Example

```javascript
const HEADER_SIZE = 48;

function repe_request(ws, id, method, params) {
    const enc = new TextEncoder();
    const query = enc.encode(method);
    const body = params != null ? enc.encode(JSON.stringify(params)) : new Uint8Array(0);
    const total = HEADER_SIZE + query.length + body.length;
    const buf = new ArrayBuffer(total);
    const v = new DataView(buf);

    v.setUint32(0, total, true);         // length
    v.setUint16(8, 0x1507, true);        // spec
    v.setUint8(10, 1);                   // version
    v.setUint32(16, id, true);           // id
    v.setUint32(24, query.length, true); // query_length
    v.setUint32(32, body.length, true);  // body_length
    v.setUint16(40, 1, true);            // query_format (JSON Pointer)
    v.setUint16(42, 2, true);            // body_format (JSON)

    const out = new Uint8Array(buf);
    out.set(query, HEADER_SIZE);
    out.set(body, HEADER_SIZE + query.length);
    ws.send(buf);
}

const ws = new WebSocket('ws://localhost:8080');
ws.binaryType = 'arraybuffer';
ws.onopen = () => repe_request(ws, 1, '/add', {a: 10, b: 20});
ws.onmessage = (evt) => {
    const v = new DataView(evt.data);
    const qLen = v.getUint32(24, true);
    const bLen = v.getUint32(32, true);
    const body = new Uint8Array(evt.data, HEADER_SIZE + qLen, bLen);
    console.log(JSON.parse(new TextDecoder().decode(body)));
};
```

## Secure Deployment

REPE WebSocket servers terminate TLS natively and require a bearer token (or
custom verifier) on every upgrade request. No reverse proxy is required.

### Native TLS server

TLS termination uses [MbedTLS.jl](https://github.com/JuliaLang/MbedTLS.jl).
Add it to your project explicitly (`] add MbedTLS`); it is not a hard
dependency of RepeRPC.jl and HTTP.jl's transitive supply of it is not
guaranteed across future HTTP.jl releases.

```julia
using RepeRPC
import HTTP
using MbedTLS

ssl = MbedTLS.SSLConfig("server.crt", "server.key")

server = WSServer("0.0.0.0", 8443;
    sslconfig  = ssl,
    auth_token = ENV["REPE_TOKEN"])

RepeRPC.register(server, "/status", (params, request) -> Dict("ok" => true))
RepeRPC.listen(server; async = true)
```

The server returns `401 Unauthorized` with `WWW-Authenticate: Bearer realm="REPE"`
for any request missing or mismatching the token, and closes the connection
before the WebSocket upgrade. Token comparison is constant-time.

For custom auth (header-other-than-Authorization, IP allow-lists, scope
checks), pass an `auth_verifier::Function` instead:

```julia
server = WSServer("0.0.0.0", 8443;
    sslconfig     = ssl,
    auth_verifier = function(http)
        # HTTP.header is case-insensitive and returns the empty string
        # when the header is missing.
        return HTTP.header(http.message, "X-API-Key", "") == ENV["REPE_API_KEY"]
    end)
```

The verifier is treated as failure if it returns anything other than `true`
(including throwing). When `auth_verifier` is set it takes precedence over
`auth_token`.

### Client over WSS

```julia
client = WSClient("wss://data-1.example.com:8443/repe";
    auth_token = ENV["REPE_TOKEN"])
RepeRPC.connect(client)
```

For a Fleet, set the URL and auth token on each `NodeConfig`. TCP and WSS
nodes can coexist in the same Fleet:

```julia
configs = [
    NodeConfig("compute-1.local", 51507),                                     # plain TCP
    NodeConfig("wss://gateway-1.example.com:8443/repe";                       # WSS + auth
        name = "gateway-1", auth_token = ENV["REPE_TOKEN"], tags = ["wan"]),
]
fleet = Fleet(configs)
connect!(fleet)
```

### Private CA (recommended when you control both ends)

When you cannot get a public-CA-issued certificate (internal hostnames,
air-gapped network, fleet on a VPN), the right path is to run a one-time
private CA rather than disable verification. This costs about ten minutes of
`openssl` and gives you genuine identity verification, so a man-in-the-middle
cannot forge a cert your clients accept.

**Step 1: One-time CA creation.** Keep `ca.key` secret. Distribute `ca.crt`
to every client. The CA root is valid for 10 years here; pick whatever fits
your rotation cadence.

```bash
openssl req -x509 -newkey rsa:4096 -nodes -days 3650 \
    -keyout ca.key -out ca.crt -subj "/CN=My Internal CA"
```

**Step 2: Per-server certificate signed by your CA.** The `CN` (and any
`subjectAltName` entries) must match the hostname clients will use to
connect. Repeat for each server.

```bash
openssl req -new -newkey rsa:2048 -nodes \
    -keyout server.key -out server.csr \
    -subj "/CN=data-1.internal"

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out server.crt -days 365
```

For multiple hostnames or IPs on one cert, write a small openssl config with
a `[v3_req]` section listing `subjectAltName = DNS:host1,DNS:host2,IP:10.0.0.5`
and pass `-extensions v3_req -extfile server.cnf` to `openssl x509 -req`.

**Step 3: Server uses the signed cert.**

```julia
using MbedTLS

ssl = MbedTLS.SSLConfig("server.crt", "server.key")
server = WSServer("0.0.0.0", 8443;
    sslconfig  = ssl,
    auth_token = ENV["REPE_TOKEN"])
```

**Step 4: Clients trust your CA root.** Load `ca.crt` into the client's
`SSLConfig` so the WebSocket handshake verifies against it. Verification
stays on; only the trust anchor changes.

```julia
using MbedTLS

client_ssl = MbedTLS.SSLConfig(true)  # client mode, verification required
MbedTLS.ca_chain!(client_ssl, MbedTLS.crt_parse(read("ca.crt", String)))

client = WSClient("wss://data-1.internal:8443/repe";
    sslconfig  = client_ssl,
    auth_token = ENV["REPE_TOKEN"])
```

In a Fleet, pass the same `sslconfig` through `NodeConfig`:

```julia
NodeConfig("wss://data-1.internal:8443/repe";
    sslconfig  = client_ssl,
    auth_token = ENV["REPE_TOKEN"],
    tags       = ["team"])
```

### Self-signed for local development only

```bash
openssl req -x509 -nodes -newkey rsa:2048 \
    -keyout server.key -out server.crt -days 365 \
    -subj "/CN=localhost"
```

Then pair `WSServer(...; sslconfig = MbedTLS.SSLConfig("server.crt", "server.key"))`
on the server side with `WSClient(...; require_ssl_verification = false)` on
the client side. This is acceptable for a laptop running both endpoints. Do
not ship `require_ssl_verification = false` to production. Even when you
control both ends, an on-path attacker can present their own cert and your
client will accept it, so encryption without verification gives you no
defense against a man-in-the-middle. Use a private CA (above) for any real
deployment.

## Demo

A complete interactive demo is included in `examples/`:

```bash
julia --project=. examples/websocket_demo.jl
```

Then open http://localhost:8080. The demo page includes a counter, calculator, echo, and server info panel — all communicating over the REPE binary WebSocket protocol.

See `examples/index.html` for the full JavaScript REPE client implementation.
