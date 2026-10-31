# Python implementation of the MySQL client-server protocol
#   https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html

import struct
from cymysql.err import raise_mysql_exception
from cymysql.constants import SERVER_STATUS, FLAG
from cymysql.converters import convert_characters, convert_json
from cymysql.charset import charset_by_id, encoding_by_charset

FIELD_TYPE_VAR_STRING = 253

UNSIGNED_CHAR_COLUMN = 251
UNSIGNED_SHORT_COLUMN = 252
UNSIGNED_INT24_COLUMN = 253
UNSIGNED_INT64_COLUMN = 254

SERVER_MORE_RESULTS_EXISTS = SERVER_STATUS.SERVER_MORE_RESULTS_EXISTS


def unpack_uint16(n):
    return n[0] + (n[1] << 8)


def unpack_uint24(n):
    return n[0] + (n[1] << 8) + (n[2] << 16)


def unpack_uint32(n):
    return n[0] + (n[1] << 8) + (n[2] << 16) + (n[3] << 24)


def unpack_uint64(n):
    return struct.unpack('<Q', n)[0]


class MysqlPacket(object):
    """Representation of a MySQL response packet.  Reads in the packet
    from the network socket, removes packet header and provides an interface
    for reading/parsing the packet results."""

    def __init__(self, data, charset, encoding):
        self._charset = charset
        self._encoding = encoding
        self.__position = 0
        self.__data = data
        if self.__data[0] == 0xff:
            self.__position += 1  # field_count == error (we already know that)
            self._read_uint16()   # errno
            raise_mysql_exception(self.__data)

    def get_all_data(self):
        return self.__data

    def read(self, size):
        return self._read(size)

    def _read(self, size):
        """Read the first 'size' bytes in packet and advance cursor past them."""
        pos = self.__position
        self.__position = pos + size
        return self.__data[pos:self.__position]

    def _read_uint8(self):
        pos = self.__position
        self.__position = pos + 1
        return self.__data[pos]

    def _read_uint16(self):
        pos = self.__position
        self.__position = pos + 2
        d = self.__data
        return d[pos] | (d[pos + 1] << 8)

    def _read_uint24(self):
        pos = self.__position
        self.__position = pos + 3
        d = self.__data
        return d[pos] | (d[pos + 1] << 8) | (d[pos + 2] << 16)

    def _read_uint32(self):
        pos = self.__position
        self.__position = pos + 4
        d = self.__data
        return d[pos] | (d[pos + 1] << 8) | (d[pos + 2] << 16) | (d[pos + 3] << 24)

    def _read_uint64(self):
        pos = self.__position
        self.__position = pos + 8
        d = self.__data
        return (d[pos] | (d[pos + 1] << 8) | (d[pos + 2] << 16) | (d[pos + 3] << 24) |
                (d[pos + 4] << 32) | (d[pos + 5] << 40) | (d[pos + 6] << 48) | (d[pos + 7] << 56))

    def _skip(self, size):
        """Read the first 'size' bytes in packet and advance cursor past them."""
        self.__position += size

    def _read_all(self):
        """Read all remaining data in the packet.

        (Subsequent read() or peek() will return errors.)
        """
        self.__position = -1  # ensure no subsequent read() or peek()
        return self.__data[self.__position:]

    def read_length_coded_binary(self):
        """Read a 'Length Coded Binary' number from the data buffer.

        Length coded numbers can be anywhere from 1 to 9 bytes depending
        on the value of the first byte.
        """
        pos = self.__position
        c = self.__data[pos]
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

    def _read_length_coded_string(self):
        """Read a 'Length Coded String' from the data buffer.

        A 'Length Coded String' consists first of a length coded
        (unsigned, positive) integer represented in 1-9 bytes followed by
        that many bytes of binary data.  (For example "cat" would be "3cat".)
        """
        length = self.read_length_coded_binary()
        if length is None:
            return None
        pos = self.__position
        self.__position = pos + length
        return self.__data[pos:self.__position]

    def read_decode_data(self, fields, decoders):
        row = []
        if isinstance(decoders, dict):
            encoding = self._encoding
            for f in fields:
                val = self._read_length_coded_string()
                if val is None:
                    row.append(None)
                else:
                    d = decoders.get(f.type_code)
                    if d in (convert_characters, convert_json):
                        row.append(d(val, encoding, f))
                    elif d is not None:
                        row.append(d(val))
                    else:
                        row.append(val)
        else:
            for d in decoders:
                val = self._read_length_coded_string()
                if val is None:
                    row.append(None)
                else:
                    row.append(d(val) if d is not None else val)
        return tuple(row)

    def is_ok_packet(self):
        return self.__data[0] == 0

    def is_eof_packet(self):
        return self.__data[0] == 0xfe

    def is_eof_and_status(self):
        if self.__data[0] != 0xfe:
            return False, 0, 0
        return True, self._read_uint16(), self._read_uint16()

    def read_ok_packet(self):
        self._skip(1)  # field_count (always '0')
        affected_rows = self.read_length_coded_binary()
        insert_id = self.read_length_coded_binary()
        server_status = self._read_uint16()
        warning_count = self._read_uint16()
        message = self._read_all()
        return (affected_rows, insert_id, server_status, warning_count, message)

    def read_auth_switch_request(self):
        data = self.get_all_data()
        i = data.find(b'\0', 1)
        plugin_name = data[1:i].decode('utf-8')
        j = data.find(b'\0', i + 1)
        salt = data[i + 1:j]
        return plugin_name, salt


class FieldDescriptorPacket(MysqlPacket):
    """A MysqlPacket that represents a specific column's metadata in the result.

    Parsing is automatically done and the results are exported via public
    attributes on the class such as: db, table_name, name, length, type_code.
    """

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
        if self.type_code == FIELD_TYPE_VAR_STRING:
            mblen = {8: 1, 33: 3, 88: 2, 91: 2}.get(self.charsetnr, 1)
            return self.length // mblen
        return self.length

    def __str__(self):
        return ('%s %s.%s.%s, type=%s' % (
            self.__class__, self.db, self.table_name, self.name, self.type_code)
        )
