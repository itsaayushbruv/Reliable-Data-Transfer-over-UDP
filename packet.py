import struct
import zlib

DATA = 1
ACK = 2
FIN = 3

# B is for 1BYTE and I is for 4BYTE
HEADER = struct.Struct("!BIIII")

def make_packet(ptype, seq=0, ack=0, payload=b""):
    payload = bytes(payload)
    header_without_checksum = HEADER.pack(ptype, seq, ack, len(payload), 0)
    # Calculate the checksum of the header and payload 
    return HEADER.pack(ptype, seq, ack, len(payload), checksum) + payload

def parse_packet(raw):
    if len(raw) < HEADER.size:
        return None
    ptype, seq, ack, length, checksum = HEADER.unpack(raw[:HEADER.size])
    payload = raw[HEADER.size:]
    if length != len(payload):
        return None
    test_header = HEADER.pack(ptype, seq, ack, length, 0)
    # Verify checksum and return details of the packet if valid