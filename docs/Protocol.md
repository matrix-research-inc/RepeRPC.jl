# Protocol and Formats

For the complete protocol specification, see the [official REPE documentation](https://github.com/repe-org/REPE).

REPE messages consist of a fixed 48-byte header followed by optional query and body sections, all little-endian:

```
[Header (48 bytes)] [Query (variable)] [Body (variable)]
```

## Header Structure

| Field | Size | Description |
|-------|------|-------------|
| `length` | 8 bytes | Total message length |
| `spec` | 2 bytes | Magic number (`0x1507`) |
| `version` | 1 byte | Protocol version (`1`) |
| `notify` | 1 byte | No-response flag |
| `reserved` | 4 bytes | Must be zero |
| `id` | 8 bytes | Request identifier |
| `query_length` | 8 bytes | Query section length |
| `body_length` | 8 bytes | Body section length |
| `query_format` | 2 bytes | Query format type |
| `body_format` | 2 bytes | Body format type |
| `ec` | 4 bytes | Error code |

## Query Formats

- `QUERY_RAW_BINARY` (0): Raw binary data
- `QUERY_JSON_POINTER` (1): JSON Pointer syntax

## Body Formats

- `RepeRPC.RAW_BINARY` (0): Raw binary data
- `RepeRPC.BEVE` (1): BEVE binary format
- `RepeRPC.JSON` (2): JSON data
- `RepeRPC.UTF8` (3): UTF-8 text

The underlying protocol constants are `BODY_RAW_BINARY`, `BODY_BEVE`, `BODY_JSON`, and `BODY_UTF8`; the aliases above are recommended in application code.

## Error Codes

Standard REPE error codes (0-4095 reserved):

- `EC_OK` (0): No error
- `EC_VERSION_MISMATCH` (1): Protocol version mismatch
- `EC_INVALID_HEADER` (2): Invalid header
- `EC_INVALID_QUERY` (3): Invalid query
- `EC_INVALID_BODY` (4): Invalid body
- `EC_PARSE_ERROR` (5): Parse error
- `EC_METHOD_NOT_FOUND` (6): Method not found
- `EC_TIMEOUT` (7): Request timeout

Application-specific error codes start at 4096.

## BEVE

RepeRPC.jl supports [BEVE (Binary Efficient Versatile Encoding)](https://github.com/beve-org/beve) via [BeveFormat.jl](https://github.com/matrix-research-inc/BeveFormat.jl). BEVE is a compact binary format that is often more efficient than JSON.

BEVE is typically better for numeric and structured data, arrays and matrices, high-frequency message passing, and interoperability with C++ BEVE libraries. JSON remains better for human-readable debugging, web API compatibility, and text-heavy data.

```julia
client = Client("localhost", 8080)
connect(client)

sensor_data = Dict(
    "timestamp" => 1641038400,
    "readings" => [Dict("sensor_id" => "temp_01", "value" => 23.5)],
)
send_request(client, "/sensors/upload", sensor_data; body_format = RepeRPC.BEVE)
```

Typed responses (see [Usage](Usage.md#typed-responses)) decode BEVE bodies automatically.

To compare encoded sizes:

```julia
data = Dict("matrix" => [[1,2,3], [4,5,6]], "numbers" => collect(1:100))
length(encode_body(data, RepeRPC.BEVE))
length(encode_body(data, RepeRPC.JSON))
```

See `examples/beve_demo.jl` for more BEVE examples.
