import cython

cdef class Charset(object):
    cdef public int id
    cdef public str name, collation
    cdef public bint is_default

cdef class Charsets(object):
    cdef dict _by_id
    cdef add(self, Charset c)

    @cython.locals(c=Charset)
    cpdef Charset by_name(self, str name)

    cpdef Charset by_id(self, int id)

cdef Charsets _charsets

