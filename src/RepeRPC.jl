module RepeRPC

using Sockets
# Extended (not shadowed) so `using RepeRPC, Sockets` leaves both names unambiguous
import Sockets: connect, listen
import BeveFormat

module JSONInternal
using JSON
end

const JSONLib = JSONInternal.JSON

export AbstractServer, AbstractClient, Header, Message, Client, Server
export ErrorCode, QueryFormat, BodyFormat
export send_request, send_request_async, send_notify
export REPEError, ConnectionError, TimeoutError, ValidationError, CancelledError
export batch, await_batch
export connect, disconnect, listen, stop, isconnected, wait_for_server
export serialize_message, deserialize_message
export parse_query, parse_body, encode_body
export set_nodelay!
# Streaming exports. `download`/`stream` are intentionally NOT exported: they
# would shadow `Downloads.download` / `Base.download` in user code. Reach them
# as `RepeRPC.download(...)` / `RepeRPC.stream(...)`.
export StreamBody, FileBody, StreamedResponse
export stream_response, stream_request
# Serialized Value Stream (SVS) — streamed download of a serialized value.
export StreamOpts, serve_value_stream
export FileResource, serve_file_stream
export pull_value, pull_to_beve_file, pull_to_beve_zst_file, pull_to_file
# Per-connection teardown hook (handlers opting into `(params, request, conn)`).
export ServerConnection, on_close
# Registry exports
export Registry, serve, register, register!
export parse_json_pointer, resolve_json_pointer, set_json_pointer!
export remote_menu
# Constants exports
export HEADER_SIZE, QUERY_JSON_POINTER, BODY_JSON, BODY_BEVE, BODY_UTF8
# UniUDP exports
export UniUDP, UniUDPClient, UniUDPServer
# Fleet exports (TCP)
export NodeConfig, Node, Fleet, RemoteResult, HealthStatus
export connect!, disconnect!, reconnect!
export nodes, connected_nodes, filter_nodes
export add_node!, remove_node!
export call, map_reduce, health_check
export succeeded, failed, cancelled
# UniUDP Fleet exports
export UniUDPNodeConfig, UniUDPNode, UniUDPFleet, SendResult
export notify_all
# WebSocket exports (requires HTTP.jl extension)
export WSServer, WSClient

abstract type AbstractServer end
abstract type AbstractClient end

include("constants.jl")
include("header.jl")
include("message.jl")
include("streaming.jl")
include("client.jl")
include("server.jl")
include("registry.jl")
include("value_stream.jl")
include("errors.jl")

# UniUDP submodule - unidirectional UDP protocol with chunking, redundancy, and FEC
include("uniudp.jl")

# UniUDP client and server for REPE over unidirectional UDP
include("uniudp_client.jl")
include("uniudp_server.jl")

# Fleet API - multi-server control
include("fleet.jl")
include("uniudp_fleet.jl")

# Stub for extension-provided remote_menu (requires ReplTree)
function remote_menu end

# Default cap on an inbound WebSocket frame for WSServer, WSClient, and
# NodeConfig. Matches HTTP.jl's own default.
const _WS_DEFAULT_MAX_FRAME_SIZE = 16 * 1024 * 1024

# Construction-time checks shared by the WebSocket client, server, and NodeConfig.
_check_max_frame_size(max_frame_size::Integer) = max_frame_size > 0 ||
    throw(ArgumentError("max_frame_size must be positive, got $max_frame_size"))

# A TLS config only applies to wss://. HTTP.jl reads verification from the
# config and rejects a per-call override alongside one.
function _check_tls_config(url, tls_config, require_ssl_verification::Bool)
    tls_config === nothing && return
    url !== nothing && startswith(lowercase(url), "wss://") ||
        throw(ArgumentError("tls_config requires a wss:// URL"))
    require_ssl_verification ||
        throw(ArgumentError("require_ssl_verification = false cannot be combined with tls_config; set verify_peer = false on the config instead"))
    return
end

# Stubs for extension-provided WebSocket transport (requires HTTP.jl)
function WSServer(args...; kwargs...)
    error("WSServer requires HTTP.jl. Run `using HTTP` before creating a WSServer.")
end
function WSClient(args...; kwargs...)
    error("WSClient requires HTTP.jl. Run `using HTTP` before creating a WSClient.")
end

end
