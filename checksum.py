import zlib

def checksum(data):
    return zlib.crc32(data) & 0xffffffff

def verify(data, recieved_checksum):
    return checksum(data) == recieved_checksum