# In-process compatibility channel

This public Rust API is enabled by the `inprocess-python` feature. It is separate
from the main Python node runner; no internal production caller was found in the
current source audit. Existing applications may still use it directly.

`create_channel` creates an independent `AllowCopies` ledger. Use
`create_channel_with_copy_context` to supply an existing caller/session authority.
Publishers and subscribers retain that exact context; their `copy_context()`
methods let the caller retain reports after channel destruction. An existing
channel name cannot be reopened to change its policy or configuration.

## Owning transfer

`send_owned(SharedBytes)` moves one immutable frame into the channel.
`receive_owned_bytes()` returns one frame retaining the same allocation and
range. Both work in strict mode. A producer can write directly into an exclusive
`BufferLoan<u8>`, freeze it, and send that owner. Multiple subscribers share the
owner; slices and received frames remain valid after channel destruction. The
last owner releases storage. This path does not serialize a typed RuntimeData.

## Compatibility operations

- `publish(&RuntimeData)` encodes one portable MessagePack frame directly into its
  final owning allocation. Nonempty bulk encoding counts logical payload bytes
  as `PortableSerialization` and rejects under `RequireZeroCopy`.
- `receive()` decodes exactly one frame with `PortableDeserialization` accounting.
  Malformed frames, trailing bytes and strict rejection return errors without
  consuming the frame. `receive_owned_bytes()` can retrieve a rejected frame for
  inspection or forwarding. The legacy audio MessagePack prefix is accepted.
- `send(&[u8])` makes one counted `BorrowedInput` copy of the complete wire slice.
- `receive_bytes()` preserves the legacy snapshot API: it concatenates all
  currently unread frames into a new Vec, counting their total wire bytes as
  `VecMaterialization`. Rejection does not advance the reader. Use the owning API
  to avoid this snapshot.

Metadata and empty payloads need no bulk copy authorization. An empty raw send is
still one frame, so `Some(empty)` is distinct from `None`. Codec counts exclude
metadata, parser scratch and allocator growth; diagnostic ChannelStats are best
effort and are not copy-accounting evidence.

## Framing, capacity and migration

The previous implementation appended bytes to an ever-growing Vec and could skip
later messages after decoding the first one. Every send/publish is now a complete
frame; callers must assemble externally fragmented input before sending, or pass
an already owning complete frame. Typed receive rejects concatenated messages
within one frame instead of silently dropping the trailing messages. The raw Vec
snapshot API still concatenates the unread frames deliberately.

Capacity is a nonzero **frame count**, not a byte budget. With backpressure enabled,
`try_publish` returns false before serialization when full; `publish` and raw send
return a full-channel error. These synchronous methods never wait for capacity.
Without backpressure, publishing evicts the oldest frame when full. A reader that
missed frames gets an explicit lag error; its next receive resumes at the earliest
retained frame. A rejected conversion does not evict an existing frame.

History is retained before a subscriber attaches, up to capacity. With active
subscribers, frames are released after all have advanced; dropping a slow reader
recovers capacity. A fresh subscriber starts at the current end without changing
another reader's backlog. Previously consumed history is not replayed to later
readers. `drain_channel` returns discarded wire bytes; lagging readers are informed
and can then receive new frames without the old byte-cursor reset problem.

Destroying a channel closes existing publishers/subscribers and releases queued
owners. A stale handle cannot destroy a replacement channel of the same name.
Already received owners remain valid. Retaining arbitrarily many received owners
or publishing huge frames still consumes caller-controlled memory; this API does
not implement the session allocator's byte-budget or mapped-process lease protocol.
