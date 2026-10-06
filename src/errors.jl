struct REPEError <: Exception
    code::ErrorCode
    message::String
    
    function REPEError(code::ErrorCode, message::String = "")
        msg = isempty(message) ? get(ERROR_MESSAGES, code, "Unknown error") : message
        new(code, msg)
    end
end

function Base.showerror(io::IO, e::REPEError)
    print(io, "REPEError($(Int(e.code))): $(e.message)")
end

# Map a wire error code onto an `ErrorCode`, tolerating application-defined codes
# (>= `EC_APPLICATION_ERROR_BASE`) that are not named in the enum: an unrecognized
# code collapses to `EC_APPLICATION_ERROR_BASE` rather than throwing, so a
# structured `REPEError` can always be built from any error response. Callers
# preserve the exact numeric code in the message when it is not a named code.
function _error_code(ec::Integer)
    try
        return ErrorCode(ec)
    catch
        return EC_APPLICATION_ERROR_BASE
    end
end

function check_error(msg::Message)
    if msg.header.ec != UInt32(EC_OK)
        error_msg = isempty(msg.body) ? "" : String(msg.body)
        throw(REPEError(_error_code(msg.header.ec), error_msg))
    end
end

struct ValidationError <: Exception
    field::String
    message::String
end

function Base.showerror(io::IO, e::ValidationError)
    print(io, "ValidationError in field '$(e.field)': $(e.message)")
end

function validate_message(msg::Message)
    if !validate_header(msg.header)
        throw(ValidationError("header", "Invalid REPE header"))
    end
    
    if length(msg.query) != msg.header.query_length
        throw(ValidationError("query", "Query length mismatch"))
    end
    
    if length(msg.body) != msg.header.body_length
        throw(ValidationError("body", "Body length mismatch"))
    end
    
    return true
end

mutable struct ConnectionError <: Exception
    message::String
    cause::Union{Exception, Nothing}
    
    ConnectionError(message::String) = new(message, nothing)
    ConnectionError(message::String, cause::Exception) = new(message, cause)
end

function Base.showerror(io::IO, e::ConnectionError)
    print(io, "ConnectionError: $(e.message)")
    if e.cause !== nothing
        print(io, "\nCaused by: ")
        showerror(io, e.cause)
    end
end

mutable struct TimeoutError <: Exception
    message::String
    timeout::Float64
    
    TimeoutError(timeout::Float64) = new("Request timed out after $(timeout) seconds", timeout)
    TimeoutError(message::String, timeout::Float64) = new(message, timeout)
end

function Base.showerror(io::IO, e::TimeoutError)
    print(io, "TimeoutError: $(e.message)")
end

"""
    CancelledError(stream_id; resource = nothing)

An SVS pull was aborted because the caller's `cancel` predicate returned `true`.
Distinct from [`TimeoutError`](@ref) (a deadline elapsed) and from
`InterruptException` (a process interrupt / Ctrl-C): a `CancelledError` is a
deliberate, caller-initiated abort. It is thrown at a `next` round-trip boundary,
so the pull's `try`/`finally` releases the producer session and removes any partial
file exactly as it does for a `TimeoutError`. `stream_id` is `0` when the cancel was
observed before the stream opened.
"""
struct CancelledError <: Exception
    message::String
    stream_id::UInt64
    resource::Union{String, Nothing}
end
CancelledError(stream_id::Integer; resource = nothing) =
    CancelledError("svs: transfer cancelled by caller", UInt64(stream_id),
                   resource === nothing ? nothing : String(resource))

function Base.showerror(io::IO, e::CancelledError)
    print(io, "CancelledError: ", e.message)
    e.resource === nothing || print(io, " [", repr(e.resource), "]")
    e.stream_id == 0 || print(io, " (stream_id=$(e.stream_id))")
end

function handle_connection_error(f::Function)
    try
        return f()
    catch e
        if isa(e, Base.IOError)
            throw(ConnectionError("Connection failed", e))
        else
            rethrow(e)
        end
    end
end

function with_timeout(f::Function, timeout::Float64)
    result = nothing
    done = Threads.Atomic{Bool}(false)
    error_ref = Ref{Union{Exception, Nothing}}(nothing)
    
    task = @async try
        result = f()
        done[] = true
    catch e
        error_ref[] = e
        done[] = true
    end
    
    start_time = time()
    while !done[] && (time() - start_time) < timeout
        sleep(0.001)
    end
    
    if !done[]
        Base.throwto(task, InterruptException())
        throw(TimeoutError(timeout))
    end
    
    if error_ref[] !== nothing
        throw(error_ref[])
    end
    
    return result
end