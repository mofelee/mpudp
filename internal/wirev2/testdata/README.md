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
