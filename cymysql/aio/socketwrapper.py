import socket
import ssl
import zlib
try:
    from compression import zstd
except ImportError:
    try:
        import pyzstd as zstd
    except ImportError:
        zstd = None
from ..socketwrapper import SocketWrapper
from ..err import OperationalError

def pack_int24(n):
    return bytes([n & 0xFF, (n >> 8) & 0xFF, (n >> 16) & 0xFF])


def unpack_uint24(n):
    return n[0] + (n[1] << 8) + (n[2] << 16)


class AsyncSocketWrapper(SocketWrapper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def _sock_recv(self, size, loop):
        if isinstance(self._sock, ssl.SSLSocket):
            while True:
                try:
                    return self._sock.recv(size)
                except (ssl.SSLWantReadError, BlockingIOError):
                    future = loop.create_future()
                    fd = self._sock.fileno()
                    loop.add_reader(fd, lambda: (loop.remove_reader(fd), not future.done() and future.set_result(None)))
                    try:
                        await future
                    finally:
                        loop.remove_reader(fd)
        else:
            return await loop.sock_recv(self._sock, size)

    async def recv(self, size, loop):
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

        read_size = 65536 if isinstance(self._sock, (socket.socket, ssl.SSLSocket)) else size
        while size > 0:
            chunk = await self._sock_recv(max(size, read_size), loop)
            if not chunk:
                raise OperationalError(2013, "Lost connection to MySQL server during query")
            chunk_len = len(chunk)
            if chunk_len <= size:
                chunks.append(chunk)
                size -= chunk_len
            else:
                chunks.append(chunk[:size])
                self._buf = bytearray(chunk[size:])
                self._buf_pos = 0
                size = 0

        return b''.join(chunks)

    async def recv_uncompress_packet(self, loop):
        return await self.recv(unpack_uint24(await self.recv(4, loop)), loop)

    async def _recv_from_decompressed(self, size, loop):
        if len(self._decompressed) < size:
            compressed_length = unpack_uint24(await self.recv(3, loop))
            await self.recv(1, loop)  # compressed sequence
            uncompressed_length = unpack_uint24(await self.recv(3, loop))
            data = await self.recv(compressed_length, loop)
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

    async def recv_packet(self, loop):
        """Read entire mysql packet."""
        recv_data = b''
        if self._compress:
            ln = unpack_uint24(await self._recv_from_decompressed(4, loop))
            recv_data = await self._recv_from_decompressed(ln, loop)
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
                ln = int.from_bytes((await self.recv(4, loop))[:3], "little")
                recv_data += await self.recv(ln, loop)
                if recv_data[:3] != b'\xff\xff\xff':
                    break

        return recv_data

    async def send_uncompress_packet(self, data, loop):
        if isinstance(self._sock, ssl.SSLSocket):
            while data:
                try:
                    sent = self._sock.send(data)
                    data = data[sent:]
                except (ssl.SSLWantWriteError, BlockingIOError):
                    future = loop.create_future()
                    fd = self._sock.fileno()
                    loop.add_writer(fd, lambda: (loop.remove_writer(fd), not future.done() and future.set_result(None)))
                    try:
                        await future
                    finally:
                        loop.remove_writer(fd)
        else:
            await loop.sock_sendall(self._sock, data)

    async def send_packet(self, data, loop):
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
        await self.send_uncompress_packet(data, loop)
