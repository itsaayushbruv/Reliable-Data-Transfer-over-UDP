import struct
import zlib

from checksum import checksum as chk, verify as vfy

DATA = 1
ACK = 2
FIN = 3

# B is for 1BYTE and I is for 4BYTE
HEADER = struct.Struct("!BIIII")

def make_packet(ptype, seq=0, ack=0, payload=b""):
    payload = bytes(payload)
    header_without_checksum = HEADER.pack(ptype, seq, ack, len(payload), 0)

    data = header_without_checksum + payload
    checksum = chk(data)

    return HEADER.pack(ptype, seq, ack, len(payload), checksum) + payload

def parse_packet(raw):
    if len(raw) < HEADER.size:
        return None
    ptype, seq, ack, length, checksum = HEADER.unpack(raw[:HEADER.size])
    payload = raw[HEADER.size:]
    if length != len(payload):
        return None

    test_header = HEADER.pack(ptype, seq, ack, length, 0)
    data = test_header + payload
    verify = vfy(data,checksum)

    if not verify:
        return None
        
    return ptype, seq, ack, payload