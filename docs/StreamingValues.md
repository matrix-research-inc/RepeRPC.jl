# Streaming Serialized Values over REPE — Design & Implementation Notes

- Status: **implemented.** The client-pull SVS engine described here is in `src/value_stream.jl` (producer sessions, the generic-value and `.beve`/`.beve.zst` receive modes, TCP + WebSocket, the bounded-memory backpressure model, and session lifecycle: completion/`cancel`/connection-drop/idle-timeout). User-facing usage lives in [Streaming.md](Streaming.md#streaming-serialized-values-svs); this document is retained as the design record and rationale. **Deferred-item update:** the BeveFormat.jl streaming typed deserializer (§7.4) that typed-at-scale depends on has shipped as `deser_beve(T, io::IO)` (token-driven, peak memory = constructed `T` + `O(chunk)`), and REPE's typed `pull_value(T, …)` now feeds its `ChunkReader` straight to it — so typed mode 3 is memory-bounded like the generic and file modes. §11 lists the other intentionally-deferred extensions (pipelined push, resume, integrity check).
- Addendum: a **file-passthrough producer**, `serve_file_stream` (with `FileResource` and the `pull_to_file` consumer), was added alongside the value producer. When the payload already lives on disk as a `.beve`/`.beve.zst` file, it streams the file's bytes through the same session/chunk machinery **without** the deserialize→reserialize round trip the value producer would incur — passing bytes through untouched when the file's stored compression matches the wire (the `.beve.zst`-on-a-`:zstd`-stream case is a pure file read), and otherwise transcoding one step (zstd-compress a `.beve`, or zstd-decompress a `.beve.zst`) with no BEVE pass in any branch. This realizes the "stream an existing serialized file" case the value-general framing (§4) anticipated; only the producer's source differs (`read(file)` blocks vs. a `beve_value!` walk), so the wire, backpressure, and lifecycle are identical.
- Target: RepeRPC.jl + additions to BeveFormat.jl (one of them non-trivial — see §7). The generic and file receive modes use BEVE's streaming `BeveSerializer`/`BeveDeserializer` directly; only typed-at-scale (§7.4) is gated on new BeveFormat.jl work.
- Scope: RepeRPC.jl's engine for this feature. The wire it speaks is the shared cross-language **SVS** contract (§4), so a RepeRPC.jl peer interoperates with a repe-rs peer; only this side's engine is local. The example payload is an in-memory struct serialized as BEVE+zstd, but the contract and these helpers are value-general (§4).
- Revision note: an earlier draft proposed a *server-push* design backpressured by "blocking socket writes." Review found that false — a notify-push receiver drains the socket into an **unbounded** user-space queue, so transport flow control never reaches the producer (repe-rs documents exactly this hazard on its own notify channel). This revision uses a **client-pull (request/response)** model, where flow control is intrinsic and no push plumbing is needed. See §5.

## 1. Concrete requirements

Move a large in-memory struct from one peer to another over a **TCP or WebSocket** REPE connection, where:

- the payload is BEVE-serialized and **zstd-compressed**, and is **≥ 8 GB even compressed**;
- the **producer** must not materialize a full serialized or compressed copy (only the struct it already holds, plus small codec buffers);
- the **receiver** chooses, per transfer, one of these outputs, and for the file outputs must **never materialize the struct**:
  1. write a `.beve.zst` file (compressed, as received),
  2. write a `.beve` file (decompressed),
  3. deserialize into a struct in memory (generic value tree, or — see §7 — a typed struct).

For the struct output the decoded value is necessarily resident (it is the result), but the **full compressed buffer and the full decompressed buffer must never be materialized** — decode streams straight into the value.

## 2. Key finding: BEVE already streams (with one gap)

Inspecting BeveFormat.jl:

- **Serialize is streaming-capable.** `BeveSerializer(io::IO)` + `beve_value!(ser, data)` walk the struct emitting BEVE bytes into any `IO`, holding only small scratch buffers. `write_beve_zstd_file` already uses exactly this over a `ZstdCompressorStream`, with the comment *"peak RAM is independent of the data size."* The public `to_beve`/`to_beve_zstd` materialize, so we avoid them.
- **Generic deserialize is streaming-capable.** `BeveDeserializer{IOType<:IO}` reads incrementally from any `IO` (`read_byte!`, `read!`, `unsafe_read`, `eof`), and `parse_value` builds the generic value tree pulling bytes as needed. The public `from_beve(data)` materializes the input buffer first, but the engine underneath reads from a stream.
- **Typed deserialize is NOT streaming.** This is the gap the first draft missed. `deser_beve(T, data)` calls `from_beve(data)` to build the **entire generic `Dict`/`Vector` tree first**, then `StructUtils.make(style, T, parsed)` to convert. So a typed decode materializes the whole generic tree *and* the typed struct at once — for 8 GB that intermediate boxed tree is *larger* than either. There is no IO-based or token-driven typed path today. Streaming typed decode is real new work in BeveFormat.jl, not a thin wrapper (§7).
- **Streaming zstd is integrated.** `CodecZstdExt` provides `ZstdCompressorStream` / `ZstdDecompressorStream` (write- and read-side filters over any `IO`).

So the producer pipeline, both file receiver modes, and the **generic** struct mode are compositions of primitives BEVE already has; the **typed** struct mode needs a new BeveFormat.jl streaming typed deserializer before it meets the memory bound.

## 3. Architecture (client-pull)

```
SERVER (producer, holds struct)                 CLIENT (receiver, drives the pull)
  struct                                          loop: send `next(stream_id)` request
    │ beve_value!(BeveSerializer( … ))              │  ← response body = next chunk (+ last flag)
    ▼                                               ▼
  BEVE bytes                                      chunk bytes
    │ ZstdCompressorStream                          ├─► write raw ───────────────► tmp → rename .beve.zst (mode 1)
    ▼                                               ├─► ZstdDecompressorStream ──► tmp → rename .beve     (mode 2)
  zstd bytes                                        └─► ZstdDecompressorStream
    │ BoundedSink (blocks task when full)                 │ BeveDeserializer (generic)   (mode 3)
    ▼                                                      ▼   [typed: needs §7]
  bounded session buffer ──[ `next` request/response over TCP / WebSocket ]── pulled on demand
```

The server holds a per-stream **session**: a task running serialize→compress into a **bounded** buffer. The client pulls chunks with ordinary `next` requests at its own pace. Request/response is the flow control (§5.2): the bounded buffer stalls the serializer when the client is slow; the client only asks for more when it has consumed the last chunk. No notify push, no unbounded queue, no ACK window.

### 3.1 Producer side (server session)

```julia
# On `open`: start a streaming session for `data`, return a stream_id.
chan = Channel{Vector{UInt8}}(SESSION_DEPTH)        # bounded — backpressure lives here
task = @async begin
    sink = BoundedSink(chan; chunk_bytes = 1<<20)   # IO that `put!`s ~1 MiB chunks; blocks when full
    zstd = ZstdCompressorStream(sink; level = 3)
    BeveFormat.beve_value!(BeveFormat.BeveSerializer(zstd), data)   # walks struct → BEVE → zstd → bounded chunks
    close(zstd)                                      # flush zstd epilogue into sink
    close(sink)                                      # push final partial chunk, then close(chan)
end
# On `next(stream_id)`: take!(chan) → response body; when chan is closed and drained → last = true.
# On last / `cancel` / client disconnect: ensure the task is finished or stopped and the session is freed.
```

Peak server memory = the struct (already resident) + `SESSION_DEPTH` chunks + zstd buffers. The serializer **blocks** on `put!` when the buffer is full, so a slow client cannot make it race ahead. Session lifetime must be bounded: free it on `last`, on `cancel`, and on client disconnect/idle-timeout, or an abandoned pull leaks the task and the struct reference (§8, §11). Teardown must **`close(chan)`** to release a serializer task parked on a full `put!` — closing the channel wakes the blocked `put!` (it throws `InvalidStateException`, which the task treats as cancellation); merely abandoning the session would leave the task parked forever.

For the **upload** direction (client holds the struct, server writes files), the client runs the serialize→compress pipeline into a `ChunkSink` that sends each chunk as a `write` **request** and waits for the small ack response before reusing the buffer; the server handler feeds its output sink. Same request/response backpressure, mirror image, and still no new primitive.

### 3.2 Receiver pipelines (client, download direction)

The client opens its output sink once, then pulls:

| Mode | Output | Per-chunk pipeline | Extra materialization? |
|---|---|---|---|
| 1 | `.beve.zst` file | `write(tmp, chunk)`; on last `close`+`rename` | none |
| 2 | `.beve` file | `write(ZstdDecompressorStream(tmp), chunk)`; on last `close`+`rename` | none |
| 3 generic | value tree | `ChunkReader` (issues `next` on demand) → `ZstdDecompressorStream` → `BeveDeserializer` → `parse_value` | none beyond the result tree |
| 3 typed | struct `T` | **today:** generic tree → `StructUtils.make` ⇒ tree **and** struct held at once. Bounded only after §7. | full generic tree (until §7) |

- Modes 1 and 2 stream straight to disk and **never** materialize the struct.
- Mode 3 generic builds the value tree (which is the requested result) by pulling chunks on demand through a `ChunkReader <: IO` whose `read`/`readbytes!` issue `next` requests when its buffer drains. No full compressed or decompressed buffer is held — only the result tree plus O(chunk).
- Mode 3 typed is **memory-bounded** (§2): REPE's typed receive feeds the `ChunkReader` straight to the BeveFormat.jl streaming typed deserializer (§7, `deser_beve(T, io)`), which decodes token-by-token into `T` without ever materializing the payload — peak memory is the constructed `T` plus O(chunk), the same bound as the generic and file modes.

**Atomic output (mandatory).** Because the wire commits no total length (the compressed size is unknown up front), a dropped connection would otherwise leave a byte-truncated `.beve`/`.beve.zst` that looks like a complete file. File modes therefore write to a temp path and `rename` to the final name **only after** the `end`/`last` is received and the sink is flushed. A stream that ends without `last` is discarded, never renamed.

## 4. Wire contract — defined in the shared SVS spec

A RepeRPC.jl service and a repe-rs service will exchange these streams, so the wire is defined **once** in a shared cross-language contract — [**REPE Serialized Value Stream (SVS)**](https://github.com/repe-org/REPE/blob/main/serialized-stream-protocol.md), canonical in the REPE org repo. That document is the normative source for the routes, message shapes, and exact byte encodings; this plan implements it. RepeRPC.jl does **not** define its own wire.

In brief, all messages are ordinary REPE **request/response** messages (so each WebSocket frame stays small): `open` (client → server, returns `{ stream_id, format, compression }`), `next` (client → server with `{ stream_id }`; the response carries the `last` flag in its query and the raw chunk bytes in its body, zero-copy), and `cancel`. Because the compressed length is **unknown until produced**, nothing commits a total length; `last = 1` terminates. This rules out single-message `StreamBody` (which needs `body_length` up front). `format` reuses REPE `BodyFormat` values (raw binary / BEVE / JSON / UTF-8 / custom) and `compression` is orthogonal (0 = none, 1 = zstd), so the same protocol carries a BEVE+zstd struct, a plain JSON value, or a raw blob; this plan exercises the BEVE+zstd path.

**Sequencing:** repe-rs is the reference implementation; it validates the contract's provisional v0, which then freezes as v1, and RepeRPC.jl implements the frozen v1. Until freeze, treat the SVS doc, not this section, as the source of truth for any byte-level detail.

## 5. Backpressure and direction (the corrected core)

### 5.1 Why pull, not push

A server→client *notify push* does **not** get backpressure for free. The receive loop reads frames off the socket as fast as they arrive and hands them to a notify handler via an **unbounded** in-process queue; the socket therefore never stalls, TCP/WS flow control never reaches the producer, and a fast producer + slow disk grows the queue until OOM. (repe-rs documents this exact hazard on `subscribe_notifies` and names "ACK windows in a chunk protocol" as the fix.) The first draft's "blocking writes give free backpressure" was wrong for every path where the receiver decouples socket-read from chunk-handling — which includes both WebSocket paths.

The **client-pull** model sidesteps it: the client requests the next chunk only when it is ready for it, so its pull rate is its processing rate; and the server's bounded session buffer (§3.1) stalls the serializer when the client lags. Flow control is the request/response round trip itself — no ACK window, no credit accounting, and crucially **no new transport primitive**, since request/response already works identically on TCP and WebSocket.

### 5.2 Throughput vs. round trips

Pull costs one round trip per chunk. At 1 MiB chunks an 8 GB transfer is ~8k round trips: negligible on a LAN, but at WAN latency the serialized round trips dominate. Mitigation, in order of preference:

1. **Pipeline pulls.** Keep a small window (e.g. 4–8) of `next` requests in flight using REPE's existing multiplexing; tag each response with a sequence number and write in order. This hides RTT while keeping memory bounded by the window size, and still needs no new primitive.
2. **Larger chunks on TCP-only paths** (WebSocket must stay small, §9).

A fully push-based, credit-windowed variant (the repe-rs `repe::stream` shape) would also hide RTT, but it reintroduces the ACK window and a receive-side credit protocol. It is deferred (§11) unless WAN throughput proves pipelined pull insufficient.

### 5.3 No new client primitive required

Because the client *drives*, it uses ordinary `send_request`; responses match pending requests by `id` exactly as today. The push design's missing plumbing — a `Client.on_notify` hook, a `_WSClient` inbound-notify hook, and a server→client push API (none of which exist today) — is **not needed** for pull. This removes the single largest implementation risk in the first draft.

## 6. Memory accounting (8 GB+ payload)

| | Server (producer) | Client mode 1/2 (file) | Client mode 3 generic | Client mode 3 typed (v1) |
|---|---|---|---|---|
| Struct / result | resident (source) | — | resident (result tree) | result tree **+** typed struct |
| Full serialized buffer | never | never | never | never |
| Full compressed buffer | never | never | never | never |
| Full decompressed buffer | never | never | never | **yes (generic tree)** |
| Transient | `SESSION_DEPTH` chunks + zstd | 1 chunk + zstd + write buf | window chunks + zstd + parse | as generic + conversion |

File modes are bounded regardless of payload size. Generic struct mode is bounded to the result tree + O(window). Typed struct mode is bounded only once §7 lands; until then it carries a full extra generic-tree copy and is unsuitable for 8 GB.

## 7. BeveFormat.jl additions needed

Report these as core BeveFormat.jl friction points (they help every BEVE user, not just REPE):

1. **Streaming serialize to an `IO`** *(thin)* — a public `to_beve(io::IO, data)` (or export `BeveSerializer` + `beve_value!`). Wraps the existing streaming engine.
2. **Streaming generic deserialize from an `IO`** *(thin)* — public `from_beve(io::IO; …)` driving `BeveDeserializer` + `parse_value` over a stream instead of a materialized buffer.
3. **Streaming zstd variants over `IO`** *(thin)* — `write_beve_zstd(io, data)`, `from_beve_zstd(io::IO)` as compositions of (1)/(2) with the zstd streams, mirroring the existing `*_file` functions.
4. **Streaming *typed* deserialize from an `IO`** *(NOT thin — real work)* — **shipped in BeveFormat.jl** as `deser_beve(T, io::IO; …)` (and zstd/file IO variants, plus a streaming generic `from_beve(io)`). It decodes directly into `T` token-by-token without first building the generic `Dict`/`Vector` tree: object fields decode straight into their struct slots, keys with no matching field are skipped on the wire (never materialized), typed numeric arrays are bulk-read into their natural vectors, and only a value whose target needs whole-value lookahead (`@choosetype`/`Union`/abstract) is materialized for that one subtree and handed to the normal `StructUtils.make` pipeline. Peak memory is the constructed `T` plus `O(chunk)`. This was the one genuinely new engine piece; without it, typed mode 3 could not meet the memory bound (§2, §6). The Rust `beve` crate already did this (`from_reader_streaming::<R, T>`), so this closes that Julia/Rust capability gap.

(1)–(4) have all landed (BeveFormat.jl exposes streaming serialize via `BeveSerializer`/`beve_value!`, streaming generic deserialize via `from_beve(io)`, the zstd IO variants, and streaming typed `deser_beve(T, io)`). REPE feeds its `ChunkReader` directly to `deser_beve(T, io)`, so typed mode 3 is memory-bounded.

**Partial-read correctness (gate, not a nicety).** The generic deserializer over a streaming `IO` must block waiting for more bytes rather than treat a momentarily-drained buffer as EOF. BEVE's `read_byte!`/`eof` block correctly on `Pipe`/`BufferStream`, but a naive `IOBuffer`-backed bridge that reports `eof()==true` when transiently empty would corrupt the decode. Add a round-trip test over a `Pipe` that delivers a large value in small bursts and treat it as a merge gate for mode 3.

## 8. Components to build (RepeRPC.jl)

1. **`BoundedSink <: IO`** — buffers `write`s, emits ~`chunk_bytes` units into a bounded `Channel`, blocks the serializer task when full (§3.1). Producer-side backpressure.
2. **Session registry** — `stream_id → (task, channel)` with lifecycle: created on `open`, drained by `next`, freed on `last`/`cancel`/disconnect/idle-timeout. The disconnect+idle cleanup is mandatory (abandoned pulls otherwise leak the task and the struct).
3. **`ChunkReader <: IO`** — client-side pull reader whose `read`/`readbytes!` issue `next` requests on demand (optionally with a small in-flight pipeline, §5.2) and surface `last` as EOF. Feeds the zstd/BEVE chain for mode 3.
4. **Wire helpers** — `open`/`next`/`cancel` (+ upload dual) over the request/response `Message` path. *Sending* is unchanged on TCP and WebSocket. *Receiving* `next` must use the normal `_read_message` path (which retains `msg.query`) and read `last` from `msg.query[1]`: the existing zero-copy streamed-receive helper discards the response query into `devnull` and exposes only the body + `body_format`, so it cannot surface `last` and is **not** reusable for SVS. With ≤ a few-MiB chunk bodies the normal path is the right one anyway (and it is what allows pipelined `next`, §5.2, since the streamed-sink path occupies the reader for the whole transfer).
5. **Producer entry** — `serve_value_stream(server, resource_resolver)` registering the `open`/`next` handlers around §3.1.
6. **Receiver entry** — `pull_stream(client, resource; into = mode)` where `mode` is a `.beve.zst` path, a `.beve` path, or `:generic` / `(:typed, T)`. **File modes write to a temp path and rename on `last`** (§3.2). Returns the value for mode 3.
7. **`CodecZstd` as a (weak)dep** on the REPE side for the client's decompress modes (BEVE depends on it via its extension; REPE needs it directly for modes 2/3; mode 1 needs none).

No `Client.on_notify`, `_WSClient` hook, or server-push API is required (§5.3).

## 9. Chunk sizing

Keep `chunk_bytes` small enough that a WebSocket binary frame stays modest — **1–4 MiB**. Smaller favors WebSocket receiver memory and latency; larger favors TCP throughput by reducing per-message overhead. Default 1 MiB serves both transports; expose it as a parameter. Do **not** use tens-of-MiB chunks — that reintroduces large WS frames and undoes the receiver bound. For WAN throughput prefer pipelining (§5.2) over oversizing chunks.

## 10. Why not the heavier transfer machinery

This plan **does** share a cross-language wire contract (the lean SVS spec, §4) — interop is a confirmed requirement. What it deliberately does **not** build is the heavy resumable *engine*: a credit window, replay ring, or resume handshake. That machinery is justified only by **pipelined push throughput with backpressure** or **resume across drops**, both deferred (§11): pipelined pull (§5.2) covers WAN throughput, and correct backpressure here comes from request/response semantics (§5.1), not from a new protocol. The shared contract is intentionally lean (a small pull protocol), distinct from the heavy resumable transfer protocol an earlier exploration drafted and discarded.

If resume across a dropped connection or pipelined-push backpressure later becomes necessary, that is a separate, heavier contract layered alongside SVS — not retrofitted into it.

## 11. Deferred (not in v1)

- **Pipelined push with a credit window.** The throughput ceiling of pull is RTT (mitigated by §5.2 pipelining). If that proves insufficient over WAN, adopt a small credit/ACK window — at which point this is converging on `repe::stream` and is worth doing deliberately, not by accident.
- **Resume across a dropped connection.** v1 restarts a failed transfer (and discards the un-renamed temp file). The struct is replayable, so resume is addable, but resuming a stateful serialize+compress stream means re-running it to the resume offset — real CPU cost, deferred until a concrete flaky-link need appears.
- **Integrity check.** Transport already checksums (TCP/TLS/WS). If end-to-end verification is later wanted, carry a streaming CRC32C in the final chunk/`end`; the codec passes are single-pass so it folds in cheaply. (Atomic rename, §3.2, already prevents a *truncated* file from being accepted; CRC would additionally catch corruption.)
- **Streaming typed deserialize at scale** — *done.* The BeveFormat.jl §7.4 dependency (`deser_beve(T, io)`) shipped, and REPE's typed `pull_value(T, …)` feeds its `ChunkReader` to it, so typed mode 3 is memory-bounded. No longer deferred.

## 12. Testing

1. **Round-trip, all modes:** a medium struct pulled and received as `.beve.zst`, `.beve`, generic value, and (small) typed struct; assert byte-identity against `write_beve_zstd_file`/`write_beve_file` output and value-equality against the source.
2. **Memory bound:** pull a large generated struct (sparse/programmatic so CI can afford it); assert client RSS for file modes and server RSS both stay flat regardless of payload size, and that `SESSION_DEPTH`/window caps hold.
3. **Streaming decode under partial reads (gate):** mode 3 over a `Pipe` delivering bytes in small bursts, proving the generic `BeveDeserializer` path blocks correctly rather than seeing a short read as EOF (§7).
4. **Atomic output:** kill the connection mid-stream in modes 1/2; assert **no** final-named file exists (only an abandoned temp), i.e. a truncated stream is never presented as complete.
5. **WebSocket parity:** the same round-trips over WebSocket, asserting frame sizes stay ~`chunk_bytes`.
6. **Backpressure:** a deliberately slow client (slow disk/decoder); assert server memory stays bounded (the serializer task blocks on the bounded buffer) rather than growing.
7. **Session lifecycle:** client disconnects mid-pull; assert the server session task is stopped and the struct reference is released (no leak).
8. **Cancel/teardown:** a `cancel` mid-stream frees both sides and leaves no final-named file.

## 13. Build order

1. BeveFormat.jl: public IO-based **serialize** + **generic deserialize** + **zstd** entry points (§7.1–7.3) with the partial-read gate test. Defer §7.4 (streaming typed) unless typed-at-scale is needed in v1.
2. RepeRPC.jl: `BoundedSink` + session registry + `open`/`next` wire helpers + `serve_value_stream` (§8.1–8.5); test the producer session in isolation (bounded memory, clean shutdown).
3. RepeRPC.jl: `ChunkReader` + `pull_stream` with file modes (atomic rename) and generic struct mode (§8.3, §8.6); round-trip over TCP (§12.1–12.4).
4. WebSocket parity (§12.5) — should be free, since pull is plain request/response.
5. Backpressure, session-lifecycle, cancel, and large-payload tests (§12.2, §12.6–12.8).
6. BeveFormat.jl §7.4 streaming typed deserialize (`deser_beve(T, io)`) — **done**, and REPE's `ChunkReader` is wired to it, so typed mode 3 is memory-bounded at scale.
