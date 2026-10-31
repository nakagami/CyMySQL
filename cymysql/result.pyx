from cymysql.packet import MysqlPacket, FieldDescriptorPacket
from cymysql.packet cimport MysqlPacket
from cymysql.socketwrapper cimport SocketWrapper
from cymysql.converters import convert_characters, convert_json, convert_set
from cymysql.constants import SERVER_STATUS, FLAG


SERVER_MORE_RESULTS_EXISTS = SERVER_STATUS.SERVER_MORE_RESULTS_EXISTS


cdef class MySQLResult(object):
    cdef public object affected_rows, insert_id, rest_rows, has_result
    cdef public object message, description
    cdef public object connection
    cdef public int has_next
    cdef public object first_packet, fields, _decoders
    cdef public int rest_row_index, server_status, warning_count, field_count

    def __init__(self, connection):
        self.connection = connection
        self.affected_rows = None
        self.insert_id = None
        self.server_status = 0
        self.warning_count = 0
        self.message = None
        self.field_count = 0
        self.description = None
        self.has_next = 0
        self.has_result = False
        self.rest_rows = None
        self.rest_row_index = 0
        self._decoders = None

    def _build_decoders(self):
        conv = self.connection.conv
        encoding = self.connection.encoding
        prepared = []
        for f in self.fields:
            d = conv.get(f.type_code)
            if d is None:
                prepared.append(None)
            elif d is convert_characters:
                if f.is_set:
                    f_enc = f.encoding
                    prepared.append(lambda v, enc=f_enc: convert_set(v.decode(enc)))
                elif f.is_binary and f.charset == 'binary':
                    prepared.append(lambda v: v)
                elif f.encoding in ('utf-8', 'utf8'):
                    prepared.append(bytes.decode)
                else:
                    f_enc = f.encoding
                    prepared.append(lambda v, enc=f_enc: v.decode(enc))
            elif d is convert_json:
                if encoding in ('utf-8', 'utf8'):
                    prepared.append(bytes.decode)
                else:
                    prepared.append(lambda v, enc=encoding: v.decode(enc))
            else:
                prepared.append(d)
        return tuple(prepared)

    def read_result(self):
        self.first_packet = MysqlPacket(
            self.connection.socket.recv_packet(),
            self.connection.charset,
            self.connection.encoding,
        )

        if self.first_packet.is_ok_packet():
            (self.affected_rows, self.insert_id,
                self.server_status, self.warning_count,
                self.message) = self.first_packet.read_ok_packet()
            self.has_result = False
        else:
            self.field_count = ord(self.first_packet.read(1))
            self._get_descriptions()
            self.has_result = True
            self.read_rest_rowdata_packet()

    def read_rest_rowdata_packet(self):
        """Read rest rowdata packets for each data row in the result set."""
        cdef int is_eof, warning_count, server_status
        if (not self.has_result) or (self.rest_rows is not None):
            return
        cdef list rest_rows = []
        cdef object decoder = self._decoders if self._decoders is not None else self.connection.conv
        cdef SocketWrapper sock = <SocketWrapper>self.connection.socket
        cdef object charset = self.connection.charset
        cdef object encoding = self.connection.encoding
        cdef object fields = self.fields
        cdef bytes pkt_data
        cdef const unsigned char* buf
        cdef MysqlPacket packet = MysqlPacket(b'', charset, encoding)

        while True:
            pkt_data = sock.recv_packet()
            buf = <const unsigned char*>pkt_data
            if buf[0] == 0xfe and len(pkt_data) < 9:
                packet.reset(pkt_data)
                is_eof, warning_count, server_status = packet.is_eof_and_status()
                if is_eof:
                    self.warning_count = warning_count
                    self.server_status = server_status
                    self.has_next = (server_status & SERVER_MORE_RESULTS_EXISTS)
                    break
            packet.reset(pkt_data)
            rest_rows.append(packet.read_decode_data(fields, decoder))
        self.rest_rows = rest_rows
        self.rest_row_index = 0

    cdef void _get_descriptions(self):
        """Read a column descriptor packet for each column in the result."""
        cdef int i
        cdef object eof_packet
        self.fields = []
        description = []
        for i in range(self.field_count):
            field = FieldDescriptorPacket(
                self.connection.socket.recv_packet(),
                self.connection.charset,
                self.connection.encoding,
            )
            self.fields.append(field)
            description.append(field.description())

        eof_packet = MysqlPacket(
            self.connection.socket.recv_packet(),
            self.connection.charset,
            self.connection.encoding,
        )
        assert eof_packet.is_eof_packet(), 'Protocol error, expecting EOF'
        self.description = tuple(description)
        self._decoders = self._build_decoders()

    cpdef fetchone(self):
        cdef int is_eof, warning_count, server_status
        cdef object row
        if not self.has_result:
            return None
        if self.rest_rows is not None:
            if self.rest_row_index < len(self.rest_rows):
                row = self.rest_rows[self.rest_row_index]
                self.rest_rows[self.rest_row_index] = None
                self.rest_row_index += 1
                if self.rest_row_index >= len(self.rest_rows):
                    self.rest_rows = []
                    self.rest_row_index = 0
                return row
            self.rest_rows = []
            return None
        decoder = self._decoders if self._decoders is not None else self.connection.conv
        packet = MysqlPacket(
            self.connection.socket.recv_packet(),
            self.connection.charset,
            self.connection.encoding,
        )
        is_eof, warning_count, server_status = packet.is_eof_and_status()
        if is_eof:
            self.warning_count = warning_count
            self.server_status = server_status
            self.has_next = (server_status & SERVER_MORE_RESULTS_EXISTS)
            self.rest_rows = []
            return None
        return packet.read_decode_data(self.fields, decoder)

    cpdef fetchmany(self, object size=None):
        cdef int start, end, s, i
        cdef list rows
        if not self.has_result:
            return []
        if self.rest_rows is None:
            self.read_rest_rowdata_packet()
        if self.rest_rows is not None:
            s = 1 if size is None else <int>size
            start = self.rest_row_index
            end = start + s
            if end > len(self.rest_rows):
                end = len(self.rest_rows)
            rows = self.rest_rows[start:end]
            for i in range(start, end):
                self.rest_rows[i] = None
            self.rest_row_index = end
            if self.rest_row_index >= len(self.rest_rows):
                self.rest_rows = []
                self.rest_row_index = 0
            return rows
        return []

    cpdef fetchall(self):
        cdef int start
        cdef list rows
        if not self.has_result:
            return []
        if self.rest_rows is None:
            self.read_rest_rowdata_packet()
        if self.rest_rows is not None:
            start = self.rest_row_index
            if start == 0:
                rows = self.rest_rows
            else:
                rows = [r for r in self.rest_rows[start:] if r is not None]
            self.rest_rows = []
            self.rest_row_index = 0
            return rows
        return []
