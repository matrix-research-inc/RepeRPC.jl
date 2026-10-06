"""
    RepeRPCCodecZstdExt

Package extension providing the streaming zstd codec for the SVS value-stream
feature. Loaded automatically when `CodecZstd` is available alongside `RepeRPC`. It
supplies the only methods of `RepeRPC._zstd_compressor_stream` /
`RepeRPC._zstd_decompressor_stream`; without it, the compressed SVS paths raise a
clear "requires CodecZstd" error (the uncompressed path and the raw `.beve.zst`
file mode need no codec).
"""
module RepeRPCCodecZstdExt

using RepeRPC
using CodecZstd

# Producer side: a write-through compressor over the bounded chunk sink. `close`
# flushes the zstd epilogue into the sink (REPE's BoundedSink.close is a no-op, so
# the cascade does not close the underlying channel).
RepeRPC._zstd_compressor_stream(sink, level) = ZstdCompressorStream(sink; level = level)

# Consumer side: a read-through decompressor over the on-demand chunk reader.
RepeRPC._zstd_decompressor_stream(io) = ZstdDecompressorStream(io)

end # module
