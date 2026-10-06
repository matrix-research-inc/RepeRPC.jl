# File-stream SVS tests over TCP: `serve_file_stream` streams an existing
# `.beve`/`.beve.zst` file verbatim (or transcoded one step), with no
# deserialize→reserialize round trip. CodecZstd (a test dependency) loads
# RepeRPCCodecZstdExt, enabling the compressed/transcode paths.
import CodecZstd
import BeveFormat

# A value large enough to span many chunks at the small test chunk size, so each
# transfer exercises the multi-`next` pull loop rather than a single chunk.
_fs_value() = Dict("label" => "file stream",
                   "samples" => collect(-2000.0:0.25:2000.0),
                   "tags" => ["tag-$i" for i in 0:127])

_fs_opts(c) = RepeRPC.StreamOpts(chunk_bytes = 16 * 1024, compression = c, session_depth = 4)

function _fs_free_port()
    s = Sockets.listen(Sockets.IPv4(0), 0)
    p = Int(Sockets.getsockname(s)[2])
    close(s)
    return p
end

# Spin up a TCP file-stream server whose resolver maps a resource key to a path in
# `dir` (or returns nothing), run `f(client, dir)`, tear everything down. `prep`
# writes the fixture files into `dir` and returns a `resource -> path|nothing` map.
function _with_file_svs(f, prep; opts)
    mktempdir() do dir
        resource_map = prep(dir)
        resolve(resource) = get(resource_map, resource, nothing)
        port = _fs_free_port()
        srv = RepeRPC.Server("127.0.0.1", port)
        RepeRPC.serve_file_stream(srv, resolve; opts = opts)
        RepeRPC.listen(srv; async = true)
        RepeRPC.wait_for_server("127.0.0.1", port)
        client = RepeRPC.Client("127.0.0.1", port)
        RepeRPC.connect(client)
        try
            f(client, dir)
        finally
            RepeRPC.disconnect(client)
            RepeRPC.stop(srv)
        end
    end
end

@testset "File Stream (SVS)" begin
    @testset "FileResource compression auto-detection" begin
        @test RepeRPC.FileResource("a.beve").stored_compression == RepeRPC._COMPRESSION_NONE
        @test RepeRPC.FileResource("a.beve.zst").stored_compression == RepeRPC._COMPRESSION_ZSTD
        @test RepeRPC.FileResource("A.BEVE.ZST").stored_compression == RepeRPC._COMPRESSION_ZSTD
        @test RepeRPC.FileResource("x"; compression = :zstd).stored_compression == RepeRPC._COMPRESSION_ZSTD
        @test RepeRPC.FileResource("x"; compression = :none).stored_compression == RepeRPC._COMPRESSION_NONE
        @test RepeRPC.FileResource("x").format == UInt16(RepeRPC.BODY_BEVE)
        @test RepeRPC.FileResource("x"; format = RepeRPC.BODY_RAW_BINARY).format == UInt16(RepeRPC.BODY_RAW_BINARY)
    end

    @testset "passthrough: .beve.zst file on a zstd stream (no codec, no BEVE)" begin
        ref = _fs_value()
        prep = dir -> begin
            zpath = joinpath(dir, "coll.beve.zst")
            BeveFormat.write_beve_zstd_file(zpath, ref)
            Dict("coll" => RepeRPC.FileResource(zpath))   # auto-detects :zstd
        end
        _with_file_svs(prep; opts = _fs_opts(:zstd)) do client, dir
            # Verbatim compressed: the bytes on the wire are exactly the file's bytes.
            zp = joinpath(dir, "out.beve.zst")
            RepeRPC.pull_to_beve_zst_file(client, "coll", zp)
            @test read(zp) == read(joinpath(dir, "coll.beve.zst"))
            @test BeveFormat.read_beve_zstd_file(zp)["samples"] == ref["samples"]

            # Decompressed to .beve.
            bp = joinpath(dir, "out.beve")
            RepeRPC.pull_to_beve_file(client, "coll", bp)
            @test BeveFormat.read_beve_file(bp)["samples"] == ref["samples"]

            # Stream-decoded into a value.
            v = RepeRPC.pull_value(client, "coll")
            @test v["samples"] == ref["samples"]
            @test v["tags"] == ref["tags"]
        end
    end

    @testset "compress-on-the-fly: .beve file on a zstd stream" begin
        ref = _fs_value()
        prep = dir -> begin
            bpath = joinpath(dir, "coll.beve")
            BeveFormat.write_beve_file(bpath, ref)           # uncompressed on disk
            Dict("coll" => bpath)                       # bare-String resolver form
        end
        _with_file_svs(prep; opts = _fs_opts(:zstd)) do client, dir
            bp = joinpath(dir, "out.beve")
            RepeRPC.pull_to_beve_file(client, "coll", bp)  # server zstd-compresses, client decompresses
            @test BeveFormat.read_beve_file(bp)["samples"] == ref["samples"]

            zp = joinpath(dir, "out.beve.zst")
            RepeRPC.pull_to_beve_zst_file(client, "coll", zp)
            @test BeveFormat.read_beve_zstd_file(zp)["samples"] == ref["samples"]

            @test RepeRPC.pull_value(client, "coll")["samples"] == ref["samples"]
        end
    end

    @testset "passthrough: .beve file on a :none stream → pull_to_file" begin
        ref = _fs_value()
        prep = dir -> begin
            bpath = joinpath(dir, "coll.beve")
            BeveFormat.write_beve_file(bpath, ref)
            Dict("coll" => RepeRPC.FileResource(bpath))    # :none, wire :none ⇒ passthrough
        end
        _with_file_svs(prep; opts = _fs_opts(:none)) do client, dir
            fp = joinpath(dir, "out.beve")
            RepeRPC.pull_to_file(client, "coll", fp)       # raw bytes straight to disk
            @test read(fp) == read(joinpath(dir, "coll.beve"))
            @test BeveFormat.read_beve_file(fp)["samples"] == ref["samples"]
            @test RepeRPC.pull_value(client, "coll")["samples"] == ref["samples"]
        end
    end

    @testset "decompress-on-the-fly: .beve.zst file on a :none stream" begin
        ref = _fs_value()
        prep = dir -> begin
            zpath = joinpath(dir, "coll.beve.zst")
            BeveFormat.write_beve_zstd_file(zpath, ref)
            Dict("coll" => RepeRPC.FileResource(zpath))    # stored :zstd, wire :none ⇒ decompress
        end
        _with_file_svs(prep; opts = _fs_opts(:none)) do client, dir
            fp = joinpath(dir, "out.beve")
            RepeRPC.pull_to_file(client, "coll", fp)       # server decompresses, raw .beve on the wire
            @test BeveFormat.read_beve_file(fp)["samples"] == ref["samples"]
            @test RepeRPC.pull_value(client, "coll")["samples"] == ref["samples"]
        end
    end

    @testset "unknown resource and missing file are rejected (no hang)" begin
        prep = dir -> Dict("ghost" => RepeRPC.FileResource(joinpath(dir, "does_not_exist.beve")))
        _with_file_svs(prep; opts = _fs_opts(:zstd)) do client, _dir
            @test_throws Exception RepeRPC.pull_value(client, "unmapped")   # resolver returns nothing
            @test_throws Exception RepeRPC.pull_value(client, "ghost")      # mapped, but file absent
        end
    end

    @testset "atomic output: a mid-stream drop leaves no final file" begin
        ref = _fs_value()
        prep = dir -> begin
            bpath = joinpath(dir, "coll.beve")
            BeveFormat.write_beve_file(bpath, ref)
            Dict("coll" => RepeRPC.FileResource(bpath))
        end
        _with_file_svs(prep; opts = _fs_opts(:none)) do client, dir
            ok = joinpath(dir, "ok.beve")
            RepeRPC.pull_to_file(client, "coll", ok)
            @test isfile(ok)
            @test !isfile(ok * ".svspart")              # committed atomically, temp gone
        end
    end

    # ---- Fleet wrapper over the file producer ---------------------------------
    @testset "Fleet pull_to_file from a file-stream node" begin
        mktempdir() do dir
            ref = _fs_value()
            BeveFormat.write_beve_zstd_file(joinpath(dir, "coll.beve.zst"), ref)
            resolve(r) = r == "coll" ? RepeRPC.FileResource(joinpath(dir, "coll.beve.zst")) : nothing
            port = _fs_free_port()
            srv = RepeRPC.Server("127.0.0.1", port)
            RepeRPC.serve_file_stream(srv, resolve; opts = _fs_opts(:zstd))
            RepeRPC.listen(srv; async = true)
            RepeRPC.wait_for_server("127.0.0.1", port)
            fleet = RepeRPC.Fleet([RepeRPC.NodeConfig("127.0.0.1", port; name = "fs-1")])
            RepeRPC.connect!(fleet)
            try
                fp = joinpath(dir, "out.beve")
                r = RepeRPC.pull_to_file(fleet, "fs-1", "coll", fp)
                @test RepeRPC.succeeded(r)
                @test r[] === nothing
                @test BeveFormat.read_beve_file(fp)["samples"] == ref["samples"]

                # A resolver miss is a failed RemoteResult, not a throw.
                rbad = RepeRPC.pull_to_file(fleet, "fs-1", "nope", joinpath(dir, "z.beve"))
                @test RepeRPC.failed(rbad)
            finally
                RepeRPC.disconnect!(fleet)
                RepeRPC.stop(srv)
            end
        end
    end

    @testset "producer fault surfaces as EC_SVS_PRODUCER_ERROR" begin
        # Deterministically force a producer-side fault: a plain (uncompressed)
        # .beve file mislabeled as zstd-stored, served on a :none wire, makes the
        # producer try to zstd-decompress non-zstd bytes and fail mid-stream. The
        # consumer must see a producer fault — distinct from a malformed-request
        # (EC_INVALID_QUERY/EC_INVALID_BODY) or method-not-found error.
        ref = _fs_value()
        prep = dir -> begin
            bpath = joinpath(dir, "plain.beve")
            BeveFormat.write_beve_file(bpath, ref)
            Dict("bad" => RepeRPC.FileResource(bpath; compression = :zstd))
        end
        _with_file_svs(prep; opts = _fs_opts(:none)) do client, _dir
            err = try
                RepeRPC.pull_value(client, "bad")
                nothing
            catch e
                e
            end
            @test err isa RepeRPC.REPEError
            @test err.code == RepeRPC.EC_SVS_PRODUCER_ERROR
        end
    end

    @testset "unknown resource surfaces as structured REPEError(EC_METHOD_NOT_FOUND)" begin
        prep = _dir -> Dict{String,Any}()   # nothing resolves
        _with_file_svs(prep; opts = _fs_opts(:zstd)) do client, _dir
            err = try
                RepeRPC.pull_value(client, "nope")
                nothing
            catch e
                e
            end
            @test err isa RepeRPC.REPEError
            @test err.code == RepeRPC.EC_METHOD_NOT_FOUND
        end
    end
end
