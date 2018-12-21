# coding=utf8

from thriftpy.transport import TTransportBase, TTransportException
from thriftpy.protocol.compact import pack, unpack
from thriftpy.transport import readall
from happybase import Connection
from cStringIO import StringIO as BufferIO

# This class should be thought of as an interface.
class CReadableTransport(object):
    """base class for transports that are readable from C"""

    # TODO(dreiss): Think about changing this interface to allow us to use
    #               a (Python, not c) StringIO instead, because it allows
    #               you to write after reading.

    # NOTE: This is a classic class, so properties will NOT work
    #       correctly for setting.
    @property
    def cstringio_buf(self):
        """A cStringIO buffer that contains the current chunk we are reading."""
        pass

    def cstringio_refill(self, partialread, reqlen):
        """Refills cstringio_buf.
        Returns the currently used buffer (which can but need not be the same as
        the old cstringio_buf). partialread is what the C code has read from the
        buffer, and should be inserted into the buffer before any more reads.  The
        return value must be a new, not borrowed reference.  Something along the
        lines of self._buf should be fine.
        If reqlen bytes can't be read, throw EOFError.
        """
        pass


class TSaslClientTransport(TTransportBase, CReadableTransport):
    """
    SASL transport
    """

    START = 1
    OK = 2
    BAD = 3
    ERROR = 4
    COMPLETE = 5

    def __init__(self, transport, host, service, mechanism='GSSAPI',
                 **sasl_kwargs):
        """
        transport: an underlying transport to use, typically just a TSocket
        host: the name of the server, from a SASL perspective
        service: the name of the server's service, from a SASL perspective
        mechanism: the name of the preferred mechanism to use
        All other kwargs will be passed to the puresasl.client.SASLClient
        constructor.
        """

        from puresasl.client import SASLClient

        self.transport = transport
        self.sasl = SASLClient(host, service, mechanism, **sasl_kwargs)

        self.__wbuf = BufferIO()
        self.__rbuf = BufferIO(b'')

    def is_open(self):
        return self.transport.is_open() and bool(self.sasl)

    def open(self):
        if not self.transport.is_open():
            self.transport.open()

        self.send_sasl_msg(self.START, self.sasl.mechanism)
        self.send_sasl_msg(self.OK, self.sasl.process())

        while True:
            status, challenge = self.recv_sasl_msg()
            if status == self.OK:
                self.send_sasl_msg(self.OK, self.sasl.process(challenge))
            elif status == self.COMPLETE:
                if not self.sasl.complete:
                    raise TTransportException(
                        TTransportException.NOT_OPEN,
                        "The server erroneously indicated "
                        "that SASL negotiation was complete")
                else:
                    break
            else:
                raise TTransportException(
                    TTransportException.NOT_OPEN,
                    "Bad SASL negotiation status: %d (%s)"
                    % (status, challenge))

    def send_sasl_msg(self, status, body):
        header = pack(">BI", status, len(body))
        self.transport.write(header + body)
        self.transport.flush()

    def recv_sasl_msg(self):
        header = readall(self.transport.read, 5)
        status, length = unpack(">BI", header)
        if length > 0:
            payload = readall(self.transport.read, length)
        else:
            payload = ""
        return status, payload

    def write(self, data):
        self.__wbuf.write(data)

    def flush(self):
        data = self.__wbuf.getvalue()
        encoded = self.sasl.wrap(data)
        self.transport.write(''.join((pack("!i", len(encoded)), encoded)))
        self.transport.flush()
        self.__wbuf = BufferIO()

    def read(self, sz):
        ret = self.__rbuf.read(sz)
        if len(ret) != 0:
            return ret

        self._read_frame()
        return self.__rbuf.read(sz)

    def _read_frame(self):
        header = readall(self.transport.read, 4)
        length, = unpack('!i', header)
        encoded = readall(self.transport.read, length)
        self.__rbuf = BufferIO(self.sasl.unwrap(encoded))

    def close(self):
        self.sasl.dispose()
        self.transport.close()

    # based on TFramedTransport
    @property
    def cstringio_buf(self):
        return self.__rbuf

    def cstringio_refill(self, prefix, reqlen):
        # self.__rbuf will already be empty here because fastbinary doesn't
        # ask for a refill until the previous buffer is empty.  Therefore,
        # we can start reading new frames immediately.
        while len(prefix) < reqlen:
            self._read_frame()
            prefix += self.__rbuf.getvalue()
        self.__rbuf = BufferIO(prefix)
        return self.__rbuf

import six
from thriftpy.thrift import TClient
from thriftpy.transport import TBufferedTransport, TFramedTransport, TSocket
from thriftpy.protocol import TBinaryProtocol, TCompactProtocol
from happybase.util import ensure_bytes
from Hbase_thrift import Hbase

STRING_OR_BINARY = (six.binary_type, six.text_type)

COMPAT_MODES = ('0.90', '0.92', '0.94', '0.96')

THRIFT_TRANSPORTS = dict(
    buffered=TBufferedTransport,
    framed=TFramedTransport,
)
THRIFT_PROTOCOLS = dict(
    binary=TBinaryProtocol,
    compact=TCompactProtocol,
)

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 9090
DEFAULT_TRANSPORT = 'buffered'
DEFAULT_COMPAT = '0.96'
DEFAULT_PROTOCOL = 'binary'


class KerberosConnection(Connection):
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, timeout=None,
                 autoconnect=True, table_prefix=None,
                 table_prefix_separator=b'_', compat=DEFAULT_COMPAT,
                 transport=DEFAULT_TRANSPORT, protocol=DEFAULT_PROTOCOL,
                 use_kerberos=False, sasl_service_name='hbase'):

        if transport not in THRIFT_TRANSPORTS:
            raise ValueError("'transport' must be one of %s"
                             % ", ".join(THRIFT_TRANSPORTS.keys()))

        if table_prefix is not None:
            if not isinstance(table_prefix, STRING_OR_BINARY):
                raise TypeError("'table_prefix' must be a string")
            table_prefix = ensure_bytes(table_prefix)

        if not isinstance(table_prefix_separator, STRING_OR_BINARY):
            raise TypeError("'table_prefix_separator' must be a string")
        table_prefix_separator = ensure_bytes(table_prefix_separator)

        if compat not in COMPAT_MODES:
            raise ValueError("'compat' must be one of %s"
                             % ", ".join(COMPAT_MODES))

        if protocol not in THRIFT_PROTOCOLS:
            raise ValueError("'protocol' must be one of %s"
                             % ", ".join(THRIFT_PROTOCOLS))

        # Allow host and port to be None, which may be easier for
        # applications wrapping a Connection instance.
        self.host = host or DEFAULT_HOST
        self.port = port or DEFAULT_PORT
        self.timeout = timeout
        self.table_prefix = table_prefix
        self.table_prefix_separator = table_prefix_separator
        self.compat = compat
        self.use_kerberos = use_kerberos
        self.sasl_service_name = sasl_service_name

        self._transport_class = THRIFT_TRANSPORTS[transport]
        self._protocol_class = THRIFT_PROTOCOLS[protocol]
        self._refresh_thrift_client()

        if autoconnect:
            self.open()

        self._initialized = True

    def _refresh_thrift_client(self):
        """Refresh the Thrift socket, transport, and client."""
        socket = TSocket(self.host, self.port)
        if self.timeout is not None:
            socket.set_timeout(self.timeout)

        self.transport = self._transport_class(socket)
        if self.use_kerberos:
            self.transport = TSaslClientTransport(self.transport, self.host, self.sasl_service_name)
        protocol = self._protocol_class(self.transport, decode_response=False)
        self.client = TClient(Hbase, protocol)


import contextlib
import logging
import socket
import threading

from six.moves import queue, range

from thriftpy.thrift import TException

logger = logging.getLogger(__name__)
from happybase import ConnectionPool
class KerberosConnectionPool(ConnectionPool):
    def __init__(self, size, **kwargs):
        if not isinstance(size, int):
            raise TypeError("Pool 'size' arg must be an integer")

        if not size > 0:
            raise ValueError("Pool 'size' arg must be greater than zero")

        logger.debug(
            "Initializing connection pool with %d connections", size)

        self._lock = threading.Lock()
        self._queue = queue.LifoQueue(maxsize=size)
        self._thread_connections = threading.local()

        connection_kwargs = kwargs
        connection_kwargs['autoconnect'] = False

        for i in range(size):
            connection = KerberosConnection(**connection_kwargs)
            self._queue.put(connection)

        # The first connection is made immediately so that trivial
        # mistakes like unresolvable host names are raised immediately.
        # Subsequent connections are connected lazily.
        with self.connection():
            pass