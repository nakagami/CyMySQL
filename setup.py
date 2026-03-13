import os

from setuptools import setup, Extension

if os.environ.get('NO_CYTHON'):
    ext_modules = None
else:
    try:
        from Cython.Build import cythonize
        ext_modules = cythonize([
                Extension("cymysql.packet", ["cymysql/packet.pyx"]),
                Extension("cymysql.result", ["cymysql/result.pyx"]),
                Extension("cymysql.socketwrapper", ["cymysql/socketwrapper.pyx"]),
                Extension("cymysql.charset", ["cymysql/charset.py"]),
                Extension("cymysql.converters", ["cymysql/converters.py"]),
                Extension("cymysql.connections", ["cymysql/connections.py"]),
                Extension("cymysql.cursors", ["cymysql/cursors.py"]),
                Extension("cymysql.err", ["cymysql/err.py"]),
                Extension("cymysql.times", ["cymysql/times.py"]),
            ],
            compiler_directives={
                "language_level": "3",
                "boundscheck": False,
                "wraparound": False,
            },
        )
    except ImportError:
        ext_modules = None

setup(
    ext_modules=ext_modules,
)
