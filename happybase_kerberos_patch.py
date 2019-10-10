# coding=utf8

from __future__ import unicode_literals, print_function

from happybase import ConnectionPool, NoConnectionsAvailable
import six
try:
    from thriftpy2.thrift import TClient, TException
    from thriftpy2.transport import TBufferedTransport, TFramedTransport, TSocket, TTransportBase, TTransportException, readall
    from thriftpy2.protocol import TBinaryProtocol, TCompactProtocol
except:
    from thriftpy.thrift import TClient, TException
    from thriftpy.transport import TBufferedTransport, TFramedTransport, TSocket, TTransportBase, TTransportException, readall
    from thriftpy.protocol import TBinaryProtocol, TCompactProtocol

from puresasl.client import SASLClient
from happybase.util import ensure_bytes
from Hbase_thrift import Hbase
from struct import pack, unpack
from happybase import Connection
# from cStringIO import StringIO as BytesIO
from io import BytesIO
import contextlib
import logging
import socket
import threading

from six.moves import queue, range

from kerberos import KrbError

logger = logging.getLogger(__name__)

class TSaslClientTransport(TTransportBase):
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

        self.transport = transport
        self.sasl = SASLClient(host, service, mechanism, **sasl_kwargs)

        self.__wbuf = BytesIO()
        self.__rbuf = BytesIO()

    def is_open(self):
        return self.transport.is_open() and bool(self.sasl)

    def open(self):
        if not self.transport.is_open():
            self.transport.open()

        self.send_sasl_msg(self.START, bytearray(self.sasl.mechanism, 'utf8'))
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
        self.transport.write(b''.join((pack("!i", len(encoded)), encoded)))
        self.transport.flush()
        self.__wbuf = BytesIO()

    def read(self, sz):
        ret = self.__rbuf.read(sz)
        if len(ret) != 0 or sz == 0:
            return ret

        self._read_frame()
        return self.__rbuf.read(sz)

    def _read_frame(self):
        header = readall(self.transport.read, 4)
        length, = unpack('!i', header)
        encoded = readall(self.transport.read, length)
        self.__rbuf = BytesIO(self.sasl.unwrap(encoded))

    def close(self):
        self.sasl.dispose()
        self.transport.close()

    # # based on TFramedTransport
    # @property
    # def cstringio_buf(self):
    #     return self.__rbuf

    # def cstringio_refill(self, prefix, reqlen):
    #     # self.__rbuf will already be empty here because fastbinary doesn't
    #     # ask for a refill until the previous buffer is empty.  Therefore,
    #     # we can start reading new frames immediately.
    #     while len(prefix) < reqlen:
    #         self._read_frame()
    #         prefix += self.__rbuf.getvalue()
    #     self.__rbuf = BytesIO(prefix)
    #     return self.__rbuf

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


class NoHostsAvailable(RuntimeError):
    """
    Exception raised when no hosts specified in KerberosConnectionPool are available.
    """
    pass

class KerberosConnectionPool(ConnectionPool):
    """
    similar to `happybase.ConnectionPool` with the following extra features
    1. support multiple specify multiple hosts as destination to connection as
        a support to high avaliable
    2. pool will auto connect to the next host if current is unavailable even in
        the outermost with statement
    """
    def __init__(self, size, hosts=None, **kwargs):
        '''
            hosts: 
                A list of hosts or a string of hosts seperated by ","
                This parameter works only if host is not specified
        '''
        if not isinstance(size, int):
            raise TypeError("Pool 'size' arg must be an integer")

        if not size > 0:
            raise ValueError("Pool 'size' arg must be greater than zero")

        logger.debug(
            "Initializing connection pool with %d connections", size)

        self._lock = threading.Lock()
        self._host_queue_map = {}
        self._thread_connections = threading.local()

        connection_kwargs = kwargs
        connection_kwargs['autoconnect'] = False

        if kwargs.get('host'):
            self._hosts = [kwargs.get('host')]
        else:
            if isinstance(hosts, list):
                self._hosts = hosts
            elif isinstance(hosts, six.text_type):
                self._hosts = hosts.split(',')
            else:
                raise Exception('error hosts type')

        for host in self._hosts:
            self._host_queue_map[host] = queue.LifoQueue(maxsize=size)
            connection_kwargs['host'] = host

            for i in range(size):
                connection = KerberosConnection(**connection_kwargs)
                self._host_queue_map[host].put(connection)
                # self._queue.put(connection)

        # The first connection is made immediately so that trivial
        # mistakes like unresolvable host names are raised immediately.
        # Subsequent connections are connected lazily.
        with self.connection():
            pass

    def _acquire_connection(self, host, timeout=None):
        """Acquire a connection from the pool."""
        try:
            return self._host_queue_map[host].get(True, timeout)
        except queue.Empty:
            raise NoConnectionsAvailable(
                "No connection available from pool within specified "
                "timeout")

    def _return_connection(self, host, connection):
        """Return a connection to the pool."""
        self._host_queue_map[host].put(connection)

    @contextlib.contextmanager
    def connection(self, timeout=None):
        for host in self._hosts:
            connection = getattr(self._thread_connections, 'current', None)
            # whether in the outermost `with` context
            is_outermost_with = False
            # whether the exception raised in ``with`` block, not include ``with`` statement
            is_in_with = False
            # whether to retry next host
            is_continue = False
            if connection is None:
                # This is the outermost connection requests for this thread.
                # Obtain a new connection from the pool and keep a reference
                # in a thread local so that nested connection requests from
                # the same thread can return the same connection instance.
                # Note: this code acquires a lock before assigning to the
                #
                # thread local; see
                # http://emptysquare.net/blog/another-thing-about-pythons-
                # threadlocals/
                is_outermost_with = True
                connection = self._acquire_connection(host, timeout)
                with self._lock:
                    self._thread_connections.current = connection

            try:
                # Open connection, because connections are opened lazily.
                # This is a no-op for connections that are already open.
                # import ipdb; ipdb.set_trace()
                connection.open()
                is_in_with = True
                # Return value from the context manager's __enter__()
                yield connection

            except (TException, socket.error, KrbError) as e:
                # Refresh the underlying Thrift client if an exception
                # occurred in the Thrift layer, since we don't know whether
                # the connection is still usable.
                logger.info("Replacing tainted pool connection")
                logger.error("{} error when connect to {}".format(str(e), host))
                # don't try to open the new connection here because even if 
                # the new connection's `connection.open` failed, the 
                # `connection.transport.is_open` still returns `True` which
                # results in success of the next invoking of `connection.open` 
                # in `with pool.connection()` even if the host is still unaccessible.
                connection._refresh_thrift_client()
                if is_outermost_with and not is_in_with:
                    # only retry to connect to next host during the outermost `with` statement
                    is_continue = True
                else:
                    raise
            finally:
                # Remove thread local reference after the outermost 'with'
                # block ends. Afterwards the thread no longer owns the
                # connection.
                if is_outermost_with:
                    del self._thread_connections.current
                    self._return_connection(host, connection)
            if not is_continue:
                break
        else:
            raise NoHostsAvailable(
                "No available host connection available from pool within specified "
                "timeout")
