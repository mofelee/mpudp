#!/usr/bin/env python3
"""Independent established-record framing and SHA-256/HMAC reference."""

import hashlib
import hmac
import json
import struct

key = bytes.fromhex("b2eff53b7fc0c0da8ec4c298606f745a54053ae2e3a35c6a6695c74ba2028fb9")
session_id = bytes(range(1, 17))
route = struct.pack(">IQI", 3, 0x0102030405060708, 9)
context = struct.pack(">IHHBBHHHII", 7, 1, 1, 3, 2, 64, 4, 0, 192, 0)
second_context = struct.pack(">IHHBBHHHII", 8, 1, 1, 2, 1, 96, 4, 0, 192, 0)
context_digest = hashlib.sha256(context).digest()
ack = struct.pack(">I", 7) + context_digest


def packet(kind, typed_body):
    body = route + typed_body
    prefix = struct.pack(">4sBBH16s", b"MPUD", 2, kind, len(body), session_id)
    signed = prefix + body
    return signed + hmac.new(key, signed, hashlib.sha256).digest()


records = [
    (0x0102030405060708, 7, 25, 0, bytes(range(1, 26)) + bytes(39)),
    (0x1122334455667788, 8, 120, 1, bytes(range(128, 152)) + bytes(72)),
    (9, 7, 24, 3, bytes([255]) * 64),
]
bundle = struct.pack(">HH", len(records), 0) + b"".join(
    struct.pack(">QIIBB", group, epoch, logical, index, 0) + payload
    for group, epoch, logical, index, payload in records
)
print(json.dumps({name: value.hex() for name, value in {
    "key": key,
    "route": route,
    "context_body": context,
    "second_context_body": second_context,
    "context_digest": context_digest,
    "context_packet": packet(27, context),
    "ack_packet": packet(28, ack),
    "bundle_packet": packet(32, bundle),
}.items()}, indent=2))
