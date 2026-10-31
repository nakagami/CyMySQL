import socket
import zlib
try:
    from compression import zstd
except ImportError:
    try:
        import pyzstd as zstd
    except ImportError:
        zstd = None
from cymysql.err import OperationalError


def pack_int24(n):
    return bytes([n & 0xFF, (n >> 8) & 0xFF, (n >> 16) & 0xFF])


def unpack_uint24(n):
    return n[0] + (n[1] << 8) + (n[2] << 16)


class SocketWrapper():
    def __init__(self, sock, compress):
        self._sock = sock
        self._compress = compress
        self._decompressed = b''
        self._buf = bytearray()
        self._buf_pos = 0

    def recv(self, size):
        buf_len = len(self._buf) - self._buf_pos
        if buf_len >= size:
            res = bytes(self._buf[self._buf_pos:self._buf_pos + size])
            self._buf_pos += size
            if self._buf_pos == len(self._buf):
                self._buf.clear()
                self._buf_pos = 0
            return res

        chunks = []
        if buf_len > 0:
            chunks.append(bytes(self._buf[self._buf_pos:]))
            self._buf.clear()
            self._buf_pos = 0
            size -= buf_len

        read_size = 65536 if isinstance(self._sock, socket.socket) else size
        while size > 0:
            recv_data = self._sock.recv(max(size, read_size))
            if not recv_data:
                raise OperationalError(2013, "Lost connection to MySQL server during query")
            chunk_len = len(recv_data)
            if chunk_len <= size:
                chunks.append(recv_data)
                size -= chunk_len
            else:
                chunks.append(recv_data[:size])
                self._buf = bytearray(recv_data[size:])
                self._buf_pos = 0
                size = 0

        return b''.join(chunks)

    def recv_uncompress_packet(self):
        return self.recv(unpack_uint24(self.recv(4)))

    def _recv_from_decompressed(self, size):
        if len(self._decompressed) < size:
            compressed_length = unpack_uint24(self.recv(3))
            self.recv(1)  # compressed sequence
            uncompressed_length = unpack_uint24(self.recv(3))
            data = self.recv(compressed_length)
            if uncompressed_length != 0:
                if self._compress == "zlib":
                    data = zlib.decompress(data)
                elif self._compress == "zstd":
                    if zstd is None:
                        raise NotImplementedError("zstd compression requires compression.zstd or pyzstd")
                    data = zstd.decompress(data)
                assert len(data) == uncompressed_length
            self._decompressed += data
        recv_data, self._decompressed = self._decompressed[:size], self._decompressed[size:]
        return recv_data

    def recv_packet(self):
        """Read entire mysql packet."""
        recv_data = b''
        if self._compress:
            ln = unpack_uint24(self._recv_from_decompressed(4))
            recv_data = self._recv_from_decompressed(ln)
        else:
            buf_len = len(self._buf) - self._buf_pos
            if buf_len >= 4:
                pos = self._buf_pos
                ln = self._buf[pos] | (self._buf[pos+1] << 8) | (self._buf[pos+2] << 16)
                if buf_len >= 4 + ln and ln < 0xffffff:
                    recv_data = bytes(memoryview(self._buf)[pos+4:pos+4+ln])
                    self._buf_pos = pos + 4 + ln
                    if self._buf_pos == len(self._buf):
                        self._buf.clear()
                        self._buf_pos = 0
                    return recv_data

            while True:
                recv_data += self.recv(unpack_uint24(self.recv(4)))
                if recv_data[:3] != b'\xff\xff\xff':
                    break
        return recv_data

    def send_uncompress_packet(self, data):
        self._sock.sendall(data)

    def send_packet(self, data):
        if self._compress:
            uncompressed_length = len(data)
            if uncompressed_length < 50:
                compressed = data
                compressed_length = len(compressed)
                uncompressed_length = 0
            else:
                if self._compress == "zlib":
                    compressed = zlib.compress(data)
                elif self._compress == "zstd":
                    if zstd is None:
                        raise NotImplementedError("zstd compression requires compression.zstd or pyzstd")
                    compressed = zstd.compress(data)
                compressed_length = len(compressed)
                if len(data) < compressed_length:
                    compressed = data
                    compressed_length = len(compressed)
                    uncompressed_length = 0
            data = pack_int24(compressed_length) + b'\x00' + pack_int24(uncompressed_length) + compressed
        self._sock.sendall(data)

    def setblocking(self, b):
        self._sock.setblocking(b)

    def close(self):
        self._sock.close()
