const REPE_SPEC = 0x1507
const REPE_VERSION = 0x01
const HEADER_SIZE = 48

# Default chunk size for streamed transfers (socket <-> IO). Peak memory of a
# streamed body is bounded by this regardless of the body length. Overridable
# per-client/server (`buffer_size` keyword) and per-call (`stream_response`).
const DEFAULT_BUFFER_SIZE = 1024 * 1024  # 1 MiB

# How long a connection may sit idle before TCP keepalive starts probing the peer,
# and the cadence of those probes. A peer that vanished without sending a FIN is
# declared dead after `idle + interval * count` seconds -- 25 s at these defaults,
# inside the 30 s default request timeout, which is the point. Pass
# `keepalive = nothing` to `Client` to disable probing.
const DEFAULT_KEEPALIVE_IDLE = 15.0
const _KEEPALIVE_PROBE_INTERVAL = Cint(1)   # seconds between probes once idle expires
const _KEEPALIVE_PROBE_COUNT = Cint(10)     # unanswered probes before the peer is dead
const _IPPROTO_TCP = Cint(6)

@enum ErrorCode::UInt32 begin
    EC_OK = 0
    EC_VERSION_MISMATCH = 1
    EC_INVALID_HEADER = 2
    EC_INVALID_QUERY = 3
    EC_INVALID_BODY = 4
    EC_PARSE_ERROR = 5
    EC_METHOD_NOT_FOUND = 6
    EC_TIMEOUT = 7
    # Codes at or above this base are implementation/application-defined (mirroring
    # QUERY_CUSTOM_BASE / BODY_CUSTOM_BASE). A receiver that does not recognize a
    # specific code in this range can treat it as a generic application error.
    EC_APPLICATION_ERROR_BASE = 4096
    # SVS producer-side fault: the server's producer task failed mid-stream (e.g. a
    # file read or transcode error) rather than the request being malformed. Lets a
    # consumer distinguish a producer fault from a client/query error instead of
    # seeing it mislabeled as EC_PARSE_ERROR.
    EC_SVS_PRODUCER_ERROR = 4097
end

@enum QueryFormat::UInt16 begin
    QUERY_RAW_BINARY = 0
    QUERY_JSON_POINTER = 1
    QUERY_CUSTOM_BASE = 4096
end

@enum BodyFormat::UInt16 begin
    BODY_RAW_BINARY = 0
    BODY_BEVE = 1
    BODY_JSON = 2
    BODY_UTF8 = 3
    BODY_CUSTOM_BASE = 4096
end

const ERROR_MESSAGES = Dict{ErrorCode, String}(
    EC_OK => "OK",
    EC_VERSION_MISMATCH => "Version mismatch",
    EC_INVALID_HEADER => "Invalid header",
    EC_INVALID_QUERY => "Invalid query",
    EC_INVALID_BODY => "Invalid body",
    EC_PARSE_ERROR => "Parse error",
    EC_METHOD_NOT_FOUND => "Method not found",
    EC_TIMEOUT => "Timeout",
    EC_SVS_PRODUCER_ERROR => "SVS producer error"
)

const JSON = BODY_JSON
const BEVE = BODY_BEVE
const UTF8 = BODY_UTF8
const RAW_BINARY = BODY_RAW_BINARY
