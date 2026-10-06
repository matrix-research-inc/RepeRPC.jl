# How a request that never receives a response is reported to the caller.
#
# These are diagnosis-quality regressions: each guards a case where REPE reported
# the wrong *kind* of failure, or reported the right kind with a stacktrace that
# pointed at REPE's internals rather than at the connection.
@testset "Connection failure reporting" begin

    # A bare TCP peer that accepts one connection, waits for the request to land,
    # then either hangs up or holds the socket open without ever answering. Used
    # in place of a RepeRPC.Server because every test here needs a server that does
    # not respond: this needs no fixed port, and cannot strand a handler task.
    function silent_peer(f; hangup::Bool)
        srv = Sockets.listen(Sockets.IPv4("127.0.0.1"), 0)
        port = Int(Sockets.getsockname(srv)[2])
        held = Ref{Any}(nothing)                 # keeps the socket off the finalizer
        peer = @async try
            sock = accept(srv)
            held[] = sock
            read(sock, RepeRPC.HEADER_SIZE)         # the request landed...
            hangup && close(sock)                # ...and the peer hangs up on it
        catch
            # `close(srv)` below unblocks a still-pending `accept`.
        end
        try
            f(port)
        finally
            close(srv)
            held[] === nothing || (try; close(held[]); catch; end)
            # Bounded: `close(srv)` cannot unblock a socket that was already accepted,
            # and a hung suite is worse than a failing one.
            @test timedwait(() -> istaskdone(peer), 10.0) == :ok
        end
    end

    @testset "TimeoutError carries no misleading cause chain" begin
        # A timeout must reach the caller as a bare `TimeoutError`. Raising it from
        # inside `_await_response`'s catch block appends
        # `caused by: InvalidStateException: Channel is closed` -- REPE's own wakeup
        # mechanism, presented as the root cause (see the comment there).
        silent_peer(hangup=false) do port
            client = RepeRPC.Client("127.0.0.1", port)
            RepeRPC.connect(client)
            try
                t = @task RepeRPC.send_request(client, "/stall", nothing; timeout=0.2)
                schedule(t)
                @test timedwait(() -> istaskdone(t), 10.0) == :ok

                stack = Base.current_exceptions(t)
                @test length(stack) == 1                    # no `caused by:` chain
                @test !isempty(stack) && stack[1][1] isa RepeRPC.TimeoutError
            finally
                RepeRPC.disconnect(client)
            end
        end
    end

    @testset "peer close surfaces ConnectionError, not a timeout" begin
        # A peer that hangs up without answering must wake the waiter immediately
        # with a ConnectionError. Reporting a TimeoutError here would be doubly
        # wrong: it names the wrong cause, and `Fleet` treats TimeoutError as
        # non-retryable while ConnectionError is retryable.
        silent_peer(hangup=true) do port
            client = RepeRPC.Client("127.0.0.1", port; timeout=30.0)
            RepeRPC.connect(client)
            try
                t0 = time()
                err = try
                    RepeRPC.send_request(client, "/anything", nothing)
                    :no_error
                catch e
                    e
                end
                @test err isa RepeRPC.ConnectionError
                @test time() - t0 < 5.0    # woken by the close, not by the 30 s budget
            finally
                RepeRPC.disconnect(client)
            end
        end
    end

    @testset "TCP keepalive" begin
        # Out-of-range values must be rejected where the error can name the keyword.
        # Left to `connect`, they surface as `InexactError: UInt32(Inf)` from a
        # `send_request` that auto-connected -- and `Fleet` does not retry that.
        for bad in (0, -1.0, Inf, NaN, 1e10)
            @test_throws ArgumentError RepeRPC.Client("localhost", 9641; keepalive=bad)
        end
        @test RepeRPC.Client("localhost", 9641).keepalive == RepeRPC.DEFAULT_KEEPALIVE_IDLE
        @test RepeRPC.Client("localhost", 9641; keepalive=nothing).keepalive === nothing
        @test RepeRPC.Client("localhost", 9641; keepalive=5).keepalive === 5.0

        # Keepalive is what breaks the half-open deadlock `_enable_keepalive!`
        # describes, and every part of it is silent when it fails: assert the settings
        # actually reach the socket rather than trusting the calls that set them.
        if Sys.islinux() || Sys.isapple()
            SOL_SOCKET   = Sys.islinux() ? Cint(1) : Cint(0xffff)
            SO_KEEPALIVE = Sys.islinux() ? Cint(9) : Cint(0x0008)
            IPPROTO_TCP   = Cint(6)
            TCP_KEEPIDLE  = Sys.islinux() ? Cint(4) : Cint(0x10)
            TCP_KEEPINTVL = Sys.islinux() ? Cint(5) : Cint(0x101)
            TCP_KEEPCNT   = Sys.islinux() ? Cint(6) : Cint(0x102)

            sockopt = function(client, level, opt)
                val = Ref{Cint}(0)
                len = Ref{Cuint}(sizeof(Cint))
                rc = ccall(:getsockopt, Cint, (Cint, Cint, Cint, Ptr{Cvoid}, Ptr{Cuint}),
                           Base._fd(client.socket), level, opt, val, len)
                rc == 0 || error("getsockopt(level=$level, opt=$opt) failed")
                return val[]
            end

            # No `accept` task: the kernel completes each handshake out of the listen
            # backlog, which is all these clients need to have a connected socket.
            srv = Sockets.listen(Sockets.IPv4("127.0.0.1"), 0)
            port = Int(Sockets.getsockname(srv)[2])

            try
                # The `0.4 -> 1` case is the sub-second clamp; without it libuv
                # rejects the zero delay only *after* enabling SO_KEEPALIVE, leaving
                # keepalive on with the OS default two-hour idle.
                for (requested, expected) in ((20.0, 20), (0.4, 1))
                    on = RepeRPC.Client("127.0.0.1", port; keepalive=requested)
                    RepeRPC.connect(on)
                    @test sockopt(on, SOL_SOCKET, SO_KEEPALIVE) != 0
                    @test sockopt(on, IPPROTO_TCP, TCP_KEEPIDLE) == expected
                    # The probe cadence is the half libuv leaves at the OS default on
                    # Julia 1.9/1.10 (75 s x 8 -> a 615 s detection window). Asserting
                    # only SO_KEEPALIVE and the idle time would pass there while the
                    # documented 25 s detection was false by a factor of 25.
                    @test sockopt(on, IPPROTO_TCP, TCP_KEEPINTVL) == RepeRPC._KEEPALIVE_PROBE_INTERVAL
                    @test sockopt(on, IPPROTO_TCP, TCP_KEEPCNT) == RepeRPC._KEEPALIVE_PROBE_COUNT
                    RepeRPC.disconnect(on)
                end

                off = RepeRPC.Client("127.0.0.1", port; keepalive=nothing)
                RepeRPC.connect(off)
                @test sockopt(off, SOL_SOCKET, SO_KEEPALIVE) == 0
                RepeRPC.disconnect(off)
            finally
                close(srv)
            end

            # The server has the same exposure in the other direction: a client that
            # vanishes without a FIN leaves `_handle_client` parked in `eof(client)`
            # forever, holding a socket. `Server` therefore applies keepalive to each
            # accepted connection -- which is a socket the public API never hands
            # back, so assert against an accepted socket directly.
            @test RepeRPC.Server("localhost", 9642).keepalive == RepeRPC.DEFAULT_KEEPALIVE_IDLE
            @test RepeRPC.Server("localhost", 9642; keepalive=nothing).keepalive === nothing
            for bad in (0, -1.0, Inf, NaN, 1e10)
                @test_throws ArgumentError RepeRPC.Server("localhost", 9642; keepalive=bad)
            end

            listener = Sockets.listen(Sockets.IPv4("127.0.0.1"), 0)
            lport = Int(Sockets.getsockname(listener)[2])
            dialer = @async Sockets.connect(Sockets.IPv4("127.0.0.1"), lport)
            accepted = accept(listener)
            try
                RepeRPC._enable_keepalive!(accepted, 20.0)
                sock = (level, opt) -> begin
                    val = Ref{Cint}(0)
                    len = Ref{Cuint}(sizeof(Cint))
                    rc = ccall(:getsockopt, Cint, (Cint, Cint, Cint, Ptr{Cvoid}, Ptr{Cuint}),
                               Base._fd(accepted), level, opt, val, len)
                    rc == 0 || error("getsockopt(level=$level, opt=$opt) failed")
                    return val[]
                end
                @test sock(SOL_SOCKET, SO_KEEPALIVE) != 0
                @test sock(IPPROTO_TCP, TCP_KEEPIDLE) == 20
                @test sock(IPPROTO_TCP, TCP_KEEPINTVL) == RepeRPC._KEEPALIVE_PROBE_INTERVAL
                @test sock(IPPROTO_TCP, TCP_KEEPCNT) == RepeRPC._KEEPALIVE_PROBE_COUNT
            finally
                close(accepted)
                close(fetch(dialer))
                close(listener)
            end
        end
    end
end
