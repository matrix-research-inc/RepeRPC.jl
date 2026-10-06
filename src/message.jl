struct Message
    header::Header
    query::String
    body::Vector{UInt8}

    function Message(header::Header, query::String, body::Vector{UInt8})
        if sizeof(query) != header.query_length
            throw(ArgumentError("Query length mismatch: expected $(header.query_length), got $(sizeof(query))"))
        end
        if length(body) != header.body_length
            throw(ArgumentError("Body length mismatch"))
        end
        new(header, query, body)
    end
end

function Message(;
    id::Union{UInt64,Int}=0,
    query::String="",
    body::Any=nothing,
    query_format::Union{UInt16,Int}=UInt16(QUERY_RAW_BINARY),
    body_format::Union{UInt16,Int}=UInt16(BODY_RAW_BINARY),
    notify::Bool=false,
    ec::Union{UInt32,Int}=UInt32(EC_OK)
)
    if body === nothing
        body_bytes = UInt8[]
    elseif body isa String
        body_bytes = Vector{UInt8}(body)
    elseif body isa Vector{UInt8}
        body_bytes = body
    else
        # Handle other types - encode based on format
        if body_format == UInt16(BODY_JSON)
            json_str = JSONLib.json(body)
            body_bytes = Vector{UInt8}(json_str)
        elseif body_format == UInt16(BODY_BEVE)
            body_bytes = BeveFormat.to_beve(body)
        else
            body_bytes = Vector{UInt8}(string(body))
        end
    end

    query_length = UInt64(sizeof(query))
    body_length = UInt64(length(body_bytes))
    total_length = HEADER_SIZE + query_length + body_length

    header = Header(
        length=total_length,
        id=UInt64(id),
        query_length=query_length,
        body_length=body_length,
        query_format=UInt16(query_format),
        body_format=UInt16(body_format),
        notify=notify ? 0x01 : 0x00,
        ec=UInt32(ec)
    )

    return Message(header, query, body_bytes)
end

function serialize_message(msg::Message)::Vector{UInt8}
    header_bytes = serialize_header(msg.header)

    query_bytes = Vector{UInt8}(msg.query)
    total_size = HEADER_SIZE + length(query_bytes) + length(msg.body)
    buffer = Vector{UInt8}(undef, total_size)

    buffer[1:HEADER_SIZE] = header_bytes

    offset = HEADER_SIZE + 1
    if !isempty(query_bytes)
        buffer[offset:offset+length(query_bytes)-1] = query_bytes
        offset += length(query_bytes)
    end

    if !isempty(msg.body)
        buffer[offset:offset+length(msg.body)-1] = msg.body
    end

    return buffer
end

function deserialize_message(buffer::Vector{UInt8})::Message
    if length(buffer) < HEADER_SIZE
        throw(ArgumentError("Buffer too small for REPE message"))
    end

    header = deserialize_header(buffer[1:HEADER_SIZE])

    expected_size = HEADER_SIZE + header.query_length + header.body_length
    if length(buffer) < expected_size
        throw(ArgumentError("Buffer too small: expected $expected_size, got $(length(buffer))"))
    end

    offset = HEADER_SIZE + 1
    query_bytes = buffer[offset:offset+header.query_length-1]
    query = String(query_bytes)

    offset += header.query_length
    body = buffer[offset:offset+header.body_length-1]

    return Message(header, query, body)
end

"""
    deserialize_message(header::Header, payload::AbstractVector{UInt8})::Message

Build a `Message` from an already-parsed `header` and the `payload` bytes that
follow it (query bytes immediately followed by body bytes, i.e. exactly
`header.query_length + header.body_length` bytes).

`payload` must hold at least `query_length + body_length` bytes.

A building block for callers that have framed a message and parsed its `header`
separately (e.g. to inspect `id`/`body_length` before consuming the rest) and
hold the remaining bytes in a buffer. REPE's own transports do not route through
it: the TCP receive path reads right-sized query/body buffers straight from the
socket (`_read_message`), and the WebSocket/UDP paths parse a whole frame with
the single-argument `deserialize_message(buffer)` above.
"""
function deserialize_message(header::Header, payload::AbstractVector{UInt8})::Message
    qlen = Int(header.query_length)
    blen = Int(header.body_length)
    if length(payload) < qlen + blen
        throw(ArgumentError("Payload too small: expected $(qlen + blen), got $(length(payload))"))
    end
    query = String(@view payload[1:qlen])
    body = payload[qlen+1:qlen+blen]
    return Message(header, query, body)
end

"""
    parse_query(msg::Message)::String

Returns the query string from a message. Since query is stored as String,
this simply returns it directly.
"""
parse_query(msg::Message)::String = msg.query

"""
    parse_body(msg::Message; copy::Bool = true)

Decode the body of `msg` according to its `body_format`.

For `BODY_UTF8`, `BODY_RAW_BINARY`, and unknown formats the result is derived
directly from `msg.body`. By default a fresh copy is returned so the caller owns
the bytes independently of `msg`. Pass `copy = false` to skip that allocation by
consuming `msg.body`: `BODY_RAW_BINARY` returns the very `Vector{UInt8}` (mutating
it mutates `msg.body`), and `BODY_UTF8` builds the `String` directly over that
buffer (which empties `msg.body`). Either way `msg` must be treated as consumed
after the call, so only use `copy = false` when `msg` is discarded right after
parsing, which is the common case on the receive path. `BODY_JSON`/`BODY_BEVE`
always allocate a fresh decoded value, so `copy` has no effect for them.
"""
function parse_body(msg::Message; copy::Bool = true)
    if isempty(msg.body)
        return nothing
    end
    if msg.header.body_format == UInt16(BODY_JSON)
        return JSONLib.parse(msg.body)
    elseif msg.header.body_format == UInt16(BODY_BEVE)
        return BeveFormat.from_beve(msg.body)
    elseif msg.header.body_format == UInt16(BODY_UTF8)
        return copy ? String(Base.copy(msg.body)) : String(msg.body)
    elseif msg.header.body_format == UInt16(BODY_RAW_BINARY)
        return copy ? Base.copy(msg.body) : msg.body
    else
        return copy ? Base.copy(msg.body) : msg.body
    end
end

"""
    parse_body(msg::Message, ::Type{Message}) -> Message

Deliver the response `Message` itself, unparsed. The format-based decoders strip
the response down to its decoded body and discard `msg.query`; a low-level caller
that needs the raw body *and* the response query (for example the SVS pull driver,
whose `next` response carries the terminal-chunk flag in a 1-byte raw query and an
opaque chunk in the body) requests `result_type = Message` to receive the whole
message. Unlike the generic typed `parse_body`, this does not short-circuit on an
empty body, so a legitimately empty body (a terminal zero-byte chunk) still yields
its `Message`.
"""
parse_body(msg::Message, ::Type{Message}) = msg

function parse_body(msg::Message, ::Type{T}) where T
    if isempty(msg.body)
        return nothing
    end
    format = msg.header.body_format
    if format == UInt16(BODY_JSON)
        return JSONLib.parse(msg.body, T)
    elseif format == UInt16(BODY_BEVE)
        return BeveFormat.deser_beve(T, msg.body)
    else
        throw(ArgumentError("Cannot parse body as $T with body format $format"))
    end
end

function encode_body(data, format::BodyFormat)::Vector{UInt8}
    if format == BODY_JSON
        json_str = JSONLib.json(data)
        return Vector{UInt8}(json_str)
    elseif format == BODY_BEVE
        return BeveFormat.to_beve(data)
    elseif format == BODY_UTF8
        return Vector{UInt8}(string(data))
    elseif format == BODY_RAW_BINARY
        if data isa Vector{UInt8}
            return data
        else
            throw(ArgumentError("Raw binary format requires Vector{UInt8} data"))
        end
    else
        if data isa Vector{UInt8}
            return data
        else
            return Vector{UInt8}(string(data))
        end
    end
end

function create_error_message(ec::ErrorCode, msg::String="")::Message
    error_msg = isempty(msg) ? get(ERROR_MESSAGES, ec, "Unknown error") : msg

    return Message(
        query="",
        body=error_msg,
        body_format=UInt16(BODY_UTF8),
        ec=UInt32(ec)
    )
end

function create_response(request::Message, result; body_format::BodyFormat=BODY_JSON)::Message
    body_bytes = encode_body(result, body_format)

    return Message(
        id=request.header.id,
        query=request.query,
        body=body_bytes,
        query_format=request.header.query_format,
        body_format=UInt16(body_format),
        ec=UInt32(EC_OK)
    )
end
