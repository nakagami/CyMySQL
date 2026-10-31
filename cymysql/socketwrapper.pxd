cdef class SocketWrapper:
    cdef public object _sock
    cdef public object _compress
    cdef public object _decompressed
    cdef public bytearray _buf
    cdef public int _buf_pos

    cpdef recv(self, int size)
    cdef bytes _recv_from_decompressed(self, int size)
    cpdef recv_packet(self)
    cpdef send_packet(self, data)
    cpdef send_uncompress_packet(self, data)
