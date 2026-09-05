#!/usr/bin/env python3
"""Independent RFC 5869 / struct reference; prints vectors without importing Go."""

import hashlib
import hmac
import json
import struct


def hkdf(secret, salt, info):
    prk = hmac.new(salt or bytes(32), secret, hashlib.sha256).digest()
    return hmac.new(prk, info + b"\x01", hashlib.sha256).digest()


def parameters(send_cap, receive_cap):
    values = [
        struct.pack(">B", 1),
        struct.pack(">QQ", 0x103, 0x101),
        struct.pack(">H", 1),
        struct.pack(">BB", 3, 2),
        struct.pack(">H", 0),
        struct.pack(">BB", 0, 0),
        struct.pack(">HHHH", send_cap, receive_cap, 512, 0),
        struct.pack(">IIIHHI", 65536, 65536, 65536, 256, 32, 1024),
        struct.pack(">IHH", 0, 0, 0),
        struct.pack(">IIIIIII", 0, 0, 0, 0, 0, 0, 0),
        struct.pack(">HHI", 2, 2, 5000),
        struct.pack(">HH", 5, 3),
    ]
    return b"".join(
        struct.pack(">HH", 0x8001 + index, len(value)) + value
        for index, value in enumerate(values)
    )


def packet(kind, client, server, transcript, token, tlvs, key):
    fixed = client + server + transcript + token + struct.pack(">HH", len(tlvs), 0)
    body = fixed + tlvs + bytes(456 - len(fixed) - len(tlvs))
    prefix = struct.pack(">4sBBH16s", b"MPUD", 2, kind, len(body), session_id)
    signed = prefix + body
    return signed + hmac.new(key, signed, hashlib.sha256).digest()


psk = bytes(range(32))
session_id = bytes(range(1, 17))
client_nonce = bytes(range(16, 32))
server_nonce = bytes(range(32, 48))
token = bytes(range(48, 64))
handshake_key = hkdf(psk, b"", b"MPUDP/v2/handshake")
hello = packet(1, client_nonce, bytes(16), bytes(32), bytes(16), parameters(1200, 1472), handshake_key)
hello_digest = hashlib.sha256(hello[:-32]).digest()
challenge = packet(2, client_nonce, server_nonce, hello_digest, token, parameters(1472, 1200), handshake_key)
transcript = hashlib.sha256(hello[:-32] + challenge[:-32]).digest()
salt = client_nonce + server_nonce
c2s_key = hkdf(psk, salt, b"MPUDP/v2/c2s" + session_id + transcript)
s2c_key = hkdf(psk, salt, b"MPUDP/v2/s2c" + session_id + transcript)
finish = packet(3, client_nonce, server_nonce, transcript, token, b"", c2s_key)
ready = packet(4, client_nonce, server_nonce, transcript, token, b"", s2c_key)
reject = packet(5, client_nonce, bytes(16), hello_digest, bytes(16), struct.pack(">HHHH", 0x800d, 4, 4, 1000), handshake_key)

print(json.dumps({name: value.hex() for name, value in {
    "psk": psk,
    "handshake_key": handshake_key,
    "hello_digest": hello_digest,
    "transcript": transcript,
    "c2s_key": c2s_key,
    "s2c_key": s2c_key,
    "hello": hello,
    "challenge": challenge,
    "finish": finish,
    "ready": ready,
    "reject": reject,
}.items()}, indent=2))
