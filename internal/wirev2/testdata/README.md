# Bootstrap Vectors

`handshake.json` fixes complete 512-byte HELLO, CHALLENGE, FINISH, READY and
REJECT packets, the 480-byte HELLO digest, the exact HELLO/CHALLENGE transcript,
and all three derived keys. Hex strings include every zero padding byte.

`generate_vectors.py` is an independent reference using Python's standard
`struct`, HMAC and SHA-256 APIs. Its RFC 5869 implementation directly performs
Extract plus the single Expand block required for a 32-byte key. It neither
imports the Go codec nor reads generated Go output. Run it from this directory
to print the deterministic JSON; Go tests read the committed fixture and never
invoke the generator.

The example offers Datagram RS3+2, no repair/mux, fixed/session budgets,
asymmetric hard caps, and configured bootstrap PathID3 of5. Twelve mandatory
TLVs occupy149 bytes, including48 header bytes and101 value bytes. The
remaining223 bytes of the TLV area are zero. These are format/crypto vectors,
not evidence of negotiated runtime support or throughput.

`established.json` and `generate_established_vectors.py` independently fix
the route prefix, 24-byte encoding context, 36-byte context ACK and a mixed
epoch FEC bundle using the bootstrap vector's c2s key. The three records use
64/96/64-byte shards, including partial data-shard tails and an opaque parity
record. Synthetic shard contents test framing and padding, not an RS manifest;
the separate fecv2 codec validates reconstructed logical contents.
