# Python implementation of the MySQL client-server protocol
#   http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol

import sys
import struct
from cymysql.err import raise_mysql_exception, OperationalError
from cymysql.constants import SERVER_STATUS, FLAG
from cymysql.converters import convert_characters, convert_json
from cymysql.charset import charset_by_id, encoding_by_charset
from libc.stdint cimport uint16_t, uint32_t, uint64_t
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.ref cimport Py_INCREF


cdef int FIELD_TYPE_VAR_STRING = 253

cdef int UNSIGNED_CHAR_COLUMN = 251
cdef int UNSIGNED_SHORT_COLUMN = 252
cdef int UNSIGNED_INT24_COLUMN = 253
cdef int UNSIGNED_INT64_COLUMN = 254

cdef int SERVER_MORE_RESULTS_EXISTS = SERVER_STATUS.SERVER_MORE_RESULTS_EXISTS


cdef uint16_t unpack_uint16(bytes s):
    cdef const unsigned char* n = <const unsigned char*>s
    return n[0] | (n[1] << 8)


cdef uint32_t unpack_uint24(bytes s):
    cdef const unsigned char* n = <const unsigned char*>s
    return n[0] | (n[1] << 8) | (n[2] << 16)


cdef uint32_t unpack_uint32(bytes s):
    cdef const unsigned char* n = <const unsigned char*>s
    return n[0] | (n[1] << 8) | (n[2] << 16) | (n[3] << 24)


cdef long long unpack_uint64(bytes n):
    return struct.unpack('<Q', n)[0]


cdef class MysqlPacket(object):
    """Representation of a MySQL response packet.  Reads in the packet
    from the network socket, removes packet header and provides an interface
    for reading/parsing the packet results."""
    cdef object connection, _socket, _charset, _encoding
    cdef bytes __data
    cdef const unsigned char* __buf
    cdef int __position

    def __init__(self, data, charset, encoding):
        self._charset = charset
        self._encoding = encoding
        self.__position = 0
        self.__data = data
        if data:
            self.__buf = <const unsigned char*>self.__data
            if self.__buf[0] == 0xff:
                self.__position += 1  # field_count == error (we already know that)
                self._read_uint16()   # errno
                raise_mysql_exception(self.__data)
        else:
            self.__buf = NULL

    cpdef reset(self, bytes data):
        self.__data = data
        self.__position = 0
        if data:
            self.__buf = <const unsigned char*>self.__data
            if self.__buf[0] == 0xff:
                self.__position += 1
                self._read_uint16()
                raise_mysql_exception(self.__data)
        else:
            self.__buf = NULL

    def get_all_data(self):
        return self.__data

    cpdef read(self, size):
        return self._read(size)

    cdef bytes _read(self, int size):
        """Read the first 'size' bytes in packet and advance cursor past them."""
        cdef int pos = self.__position
        self.__position = pos + size
        return self.__data[pos:self.__position]

    cdef inline unsigned char _read_uint8(self):
        cdef int pos = self.__position
        self.__position = pos + 1
        return self.__buf[pos]

    cdef inline uint16_t _read_uint16(self):
        cdef int pos = self.__position
        self.__position = pos + 2
        return self.__buf[pos] | (self.__buf[pos + 1] << 8)

    cdef inline uint32_t _read_uint24(self):
        cdef int pos = self.__position
        self.__position = pos + 3
        return self.__buf[pos] | (self.__buf[pos + 1] << 8) | (self.__buf[pos + 2] << 16)

    cdef inline uint32_t _read_uint32(self):
        cdef int pos = self.__position
        self.__position = pos + 4
        return (self.__buf[pos] |
                (self.__buf[pos + 1] << 8) |
                (self.__buf[pos + 2] << 16) |
                (self.__buf[pos + 3] << 24))

    cdef inline uint64_t _read_uint64(self):
        cdef int pos = self.__position
        self.__position = pos + 8
        return ((<uint64_t>(self.__buf[pos])) |
                ((<uint64_t>(self.__buf[pos + 1])) << 8) |
                ((<uint64_t>(self.__buf[pos + 2])) << 16) |
                ((<uint64_t>(self.__buf[pos + 3])) << 24) |
                ((<uint64_t>(self.__buf[pos + 4])) << 32) |
                ((<uint64_t>(self.__buf[pos + 5])) << 40) |
                ((<uint64_t>(self.__buf[pos + 6])) << 48) |
                ((<uint64_t>(self.__buf[pos + 7])) << 56))

    cdef void _skip(self, int size):
        """Read the first 'size' bytes in packet and advance cursor past them."""
        self.__position += size

    cdef _read_all(self):
        """Read all remaining data in the packet.

        (Subsequent read() or peek() will return errors.)
        """
        self.__position = -1  # ensure no subsequent read() or peek()
        return self.__data[self.__position:]

    cdef object read_length_coded_binary(self):
        """Read a 'Length Coded Binary' number from the data buffer.

        Length coded numbers can be anywhere from 1 to 9 bytes depending
        on the value of the first byte.
        """
        cdef int pos = self.__position
        cdef unsigned char c = self.__buf[pos]
        self.__position = pos + 1
        if c < UNSIGNED_CHAR_COLUMN:
            return c
        elif c == UNSIGNED_CHAR_COLUMN:
            return None
        elif c == UNSIGNED_SHORT_COLUMN:
            return self._read_uint16()
        elif c == UNSIGNED_INT24_COLUMN:
            return self._read_uint24()
        elif c == UNSIGNED_INT64_COLUMN:
            return self._read_uint64()
        else:
            return c

    cdef inline int _read_length_coded_string_length(self):
        cdef int pos = self.__position
        cdef unsigned char c = self.__buf[pos]
        self.__position = pos + 1
        if c < UNSIGNED_CHAR_COLUMN:
            return c
        elif c == UNSIGNED_CHAR_COLUMN:
            return -1
        elif c == UNSIGNED_SHORT_COLUMN:
            pos = self.__position
            self.__position = pos + 2
            return self.__buf[pos] | (self.__buf[pos+1] << 8)
        elif c == UNSIGNED_INT24_COLUMN:
            pos = self.__position
            self.__position = pos + 3
            return self.__buf[pos] | (self.__buf[pos+1] << 8) | (self.__buf[pos+2] << 16)
        elif c == UNSIGNED_INT64_COLUMN:
            pos = self.__position
            self.__position = pos + 8
            return <int>(
                (<uint64_t>self.__buf[pos]) |
                ((<uint64_t>self.__buf[pos+1]) << 8) |
                ((<uint64_t>self.__buf[pos+2]) << 16) |
                ((<uint64_t>self.__buf[pos+3]) << 24) |
                ((<uint64_t>self.__buf[pos+4]) << 32) |
                ((<uint64_t>self.__buf[pos+5]) << 40) |
                ((<uint64_t>self.__buf[pos+6]) << 48) |
                ((<uint64_t>self.__buf[pos+7]) << 56)
            )
        else:
            return c

    cdef bytes _read_length_coded_string(self):
        """Read a 'Length Coded String' from the data buffer.

        A 'Length Coded String' consists first of a length coded
        (unsigned, positive) integer represented in 1-9 bytes followed by
        that many bytes of binary data.  (For example "cat" would be "3cat".)
        """
        cdef int ln = self._read_length_coded_string_length()
        if ln < 0:
            return None
        cdef int pos = self.__position
        self.__position = pos + ln
        return self.__data[pos:pos+ln]

    cpdef read_decode_data(self, fields, decoders):
        cdef list row_l
        cdef tuple row_t
        cdef object val, d, f, res
        cdef int i, n
        if isinstance(decoders, dict):
            row_l = []
            n = len(fields)
            for i in range(n):
                val = self._read_length_coded_string()
                if val is None:
                    row_l.append(None)
                else:
                    f = fields[i]
                    d = decoders.get(f.type_code)
                    if d in (convert_characters, convert_json):
                        row_l.append(d(val, self._encoding, f))
                    elif d is not None:
                        row_l.append(d(val))
                    else:
                        row_l.append(val)
            return tuple(row_l)
        else:
            n = len(decoders)
            row_t = PyTuple_New(n)
            for i in range(n):
                val = self._read_length_coded_string()
                if val is None:
                    Py_INCREF(None)
                    PyTuple_SET_ITEM(row_t, i, None)
                else:
                    d = decoders[i]
                    if d is not None:
                        res = d(val)
                    else:
                        res = val
                    Py_INCREF(res)
                    PyTuple_SET_ITEM(row_t, i, res)
            return row_t

    cpdef is_ok_packet(self):
        return (<unsigned char>(self.__buf[0])) == 0

    cpdef is_eof_packet(self):
        return (<unsigned char>(self.__buf[0])) == 0xfe

    cpdef is_eof_and_status(self):
        if (<unsigned char>(self.__buf[0])) != 0xfe:
            return False, 0, 0
        return True, self._read_uint16(), self._read_uint16()

    cpdef read_ok_packet(self):
        cdef object affected_rows, insert_id
        cdef int server_status, warning_count
        cdef message
        self._skip(1)  # field_count (always '0')
        affected_rows = self.read_length_coded_binary()
        insert_id = self.read_length_coded_binary()
        server_status = self._read_uint16()
        warning_count = self._read_uint16()
        message = self._read_all()
        return (affected_rows, insert_id, server_status, warning_count, message)

    cpdef read_auth_switch_request(self):
        cdef int i, j
        cdef data, plugin_name
        data = self.get_all_data()
        i = data.find(b'\0', 1)
        plugin_name = data[1:i].decode('utf-8')
        j = data.find(b'\0', i + 1)
        salt = data[i + 1:j]
        return plugin_name, salt


cdef class FieldDescriptorPacket(MysqlPacket):
    """A MysqlPacket that represents a specific column's metadata in the result.

    Parsing is automatically done and the results are exported via public
    attributes on the class such as: db, table_name, name, length, type_code.
    """
    cdef public object catalog, db, table_name, org_table, name, org_name
    cdef public int charsetnr, length, type_code, flags, scale, is_set, is_binary
    cdef public str charset, encoding

    def __init__(self, *args):
        MysqlPacket.__init__(self, *args)
        self.__parse_field_descriptor()

    def __parse_field_descriptor(self):
        """Parse the 'Field Descriptor' (Metadata) packet.
        This is compatible with MySQL 4.1+ (not compatible with MySQL 4.0).
        """
        self.catalog = self._read_length_coded_string()
        self.db = self._read_length_coded_string()
        self.table_name = self._read_length_coded_string()
        self.org_table = self._read_length_coded_string()
        self.name = self._read_length_coded_string().decode(self._encoding)
        self.org_name = self._read_length_coded_string()
        self._skip(1)  # non-null filler
        self.charsetnr = self._read_uint16()
        self.charset = charset_by_id(self.charsetnr).name
        self.encoding = encoding_by_charset(self.charset)
        self.length = self._read_uint32()
        self.type_code = self._read_uint8()
        self.flags = self._read_uint16()
        self.is_set = self.flags & FLAG.SET
        self.is_binary = self.flags & FLAG.BINARY
        self.scale = self._read_uint8()  # "decimals"
        self._skip(2)  # filler (always 0x00)

    def description(self):
        """Provides a 7-item tuple compatible with the Python PEP249 DB Spec."""
        return (
            self.name,
            self.type_code,
            None,
            self.get_column_length(),
            self.get_column_length(),
            self.scale,
            1 if self.flags % 2 == 0 else 0,
        )

    def get_column_length(self):
        cdef int mblen
        if self.type_code == FIELD_TYPE_VAR_STRING:
            mblen = {8: 1, 33: 3, 88: 2, 91: 2}.get(self.charsetnr, 1)
            return self.length // mblen
        return self.length

    def __str__(self):
        return ('%s %s.%s.%s, type=%s' % (
            self.__class__, self.db, self.table_name, self.name, self.type_code)
        )
