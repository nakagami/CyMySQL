from libc.stdint cimport uint16_t, uint32_t, uint64_t

cdef class MysqlPacket(object):
    cdef object _charset, _encoding
    cdef bytes __data
    cdef const unsigned char* __buf
    cdef int __position

    cpdef reset(self, bytes data)
    cpdef read(self, size)
    cdef bytes _read(self, int size)
    cdef inline unsigned char _read_uint8(self)
    cdef inline uint16_t _read_uint16(self)
    cdef inline uint32_t _read_uint24(self)
    cdef inline uint32_t _read_uint32(self)
    cdef inline uint64_t _read_uint64(self)
    cdef void _skip(self, int size)
    cdef _read_all(self)
    cdef object read_length_coded_binary(self)
    cdef inline int _read_length_coded_string_length(self)
    cdef bytes _read_length_coded_string(self)
    cpdef read_decode_data(self, fields, decoders)
    cpdef is_ok_packet(self)
    cpdef is_eof_packet(self)
    cpdef is_eof_and_status(self)
    cpdef read_ok_packet(self)
    cpdef read_auth_switch_request(self)

cdef class FieldDescriptorPacket(MysqlPacket):
    cdef public object catalog, db, table_name, org_table, name, org_name
    cdef public int charsetnr, length, type_code, flags, scale, is_set, is_binary
    cdef str _field_charset, _field_encoding
