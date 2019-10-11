# coding=utf8
from __future__ import unicode_literals, print_function
import unittest
try:
    from unittest.mock import Mock, patch, MagicMock
except:
    from mock import Mock, patch, MagicMock
from struct import pack, unpack
import happybase_kerberos_patch
# from happybase_kerberos_patch import KerberosConnection, KerberosConnectionPool, DEFAULT_PORT

def mock_func(self):
    return (1,2)


def mock_read(data):
    from io import BytesIO
    b = BytesIO(data)

    def _(sz):
        return b.read(sz)
    return _

class MockSASLClient(MagicMock):
    def wrap(self, data):
        return data
    
    def unwrap(self, data):
        return data


sasl_client_patch = patch(
    'happybase_kerberos_patch.SASLClient', 
    wrap=lambda self,data: data,
    unwrap=lambda self,data: data)

@patch('happybase_kerberos_patch.TSocket')
class TestNormalConnection(unittest.TestCase):
    def test_connection(self, mock_tsocket):
        '''test no kerberos connection'''
        # from happybase_kerberos_patch import TSaslClientTransport, KerberosConnection, KerberosConnectionPool, DEFAULT_PORT
        # import happybase_kerberos_patch
        _HOST = 'HOST_TO_THRIFT_SERVER'
        connection = happybase_kerberos_patch.KerberosConnection(
            _HOST, protocol='compact')
        mock_tsocket.assert_called_with(
            _HOST, happybase_kerberos_patch.DEFAULT_PORT)
        
        # self.assertEqual('foo'.upper(), 'FOO')

@patch('happybase_kerberos_patch.TSocket')
@patch('happybase_kerberos_patch.SASLClient')
# @sasl_client_patch
class TestKerberosConnection(unittest.TestCase):
    def setUp(self):
        
        self.mock_transport = MagicMock()
        self.transport_patch = patch.dict(
            'happybase_kerberos_patch.THRIFT_TRANSPORTS', 
            buffered=self.mock_transport, 
            framed=self.mock_transport
        )

        # data received when connect to hbase
        self.data_to_receive = b'test'
        self.connect_received_data = (
            pack(">BI", happybase_kerberos_patch.TSaslClientTransport.OK, len(self.data_to_receive)) +
            self.data_to_receive +
            pack(">BI", happybase_kerberos_patch.TSaslClientTransport.COMPLETE, len(self.data_to_receive)) +
            self.data_to_receive
        )
        self.transport_patch.start()
    def tearDown(self):
        self.transport_patch.stop()

    def test_pool_empty(
            self,
            mock_sasl_client,
            mock_tsocket,
            ):
        '''test exception when pool is empty'''
        self.mock_transport.return_value.is_open.return_value = False

        self.mock_transport.return_value.read = mock_read(
            self.connect_received_data)
        mock_sasl_client.return_value.wrap = lambda data: data
        mock_sasl_client.return_value.unwrap = lambda data: data
        mock_sasl_client.return_value.mechanism = 'GSSAPI'
        _HOST = 'HOST_TO_THRIFT_SERVER'
        _HOSTS = [_HOST, 'HOST_TO_THRIFT_SERVER2']

        with self.assertRaises(happybase_kerberos_patch.NoConnectionsAvailable):
            pool = happybase_kerberos_patch.KerberosConnectionPool(
                size=1, hosts=_HOSTS, protocol='compact', use_kerberos=True)
            # consume the only connection first
            pool._host_queue_map[_HOST].get(True, None)
            with pool.connection(timeout=0.1) as connection:
                pass

    def test_in_with_exception(
            self,
            mock_sasl_client,
            mock_tsocket,
            ):
        '''test kerberos exception in with'''
        self.mock_transport.return_value.is_open.return_value = False

        self.mock_transport.return_value.read = mock_read(
            self.connect_received_data)
        mock_sasl_client.return_value.wrap = lambda data: data
        mock_sasl_client.return_value.unwrap = lambda data: data
        mock_sasl_client.return_value.mechanism = 'GSSAPI'
        _HOST = 'HOST_TO_THRIFT_SERVER'
        _HOSTS = [_HOST, 'HOST_TO_THRIFT_SERVER2']
        with self.assertRaises(happybase_kerberos_patch.TException):
            pool = happybase_kerberos_patch.KerberosConnectionPool(
                size=3, hosts=_HOSTS, protocol='compact', use_kerberos=True)
            self.mock_transport.return_value.is_open.return_value = True
            with pool.connection() as connection:
                raise happybase_kerberos_patch.TException

    def test_connect_exception(
            self,
            mock_sasl_client,
            mock_tsocket,
            ):
        '''test kerberos connection pool when connect failed'''
        import socket
        from kerberos import KrbError
        self.mock_transport.return_value.is_open.return_value = False
        mock_sasl_client.return_value.mechanism = 'GSSAPI'

        _HOST = 'HOST_TO_THRIFT_SERVER'
        _HOSTS = [_HOST, 'HOST_TO_THRIFT_SERVER2']

        mock_tsocket.return_value.open = Mock(side_effect=happybase_kerberos_patch.TTransportException)
        with self.assertRaises(happybase_kerberos_patch.NoHostsAvailable):
            pool = happybase_kerberos_patch.KerberosConnectionPool(
                size=3, hosts=_HOSTS, protocol='compact', use_kerberos=True)

        mock_sasl_client.return_value.process = Mock(side_effect=KrbError)
        mock_tsocket.return_value.open = Mock()
        with self.assertRaises(happybase_kerberos_patch.NoHostsAvailable):
            pool = happybase_kerberos_patch.KerberosConnectionPool(
                size=3, hosts=_HOSTS, protocol='compact', use_kerberos=True)

    def test_create_onnection_pool(
            self,
            mock_sasl_client,
            mock_tsocket,
            ):
        '''test kerberos connection pool'''
        self.mock_transport.return_value.is_open.return_value = False
        
        self.mock_transport.return_value.read = mock_read(self.connect_received_data)
        mock_sasl_client.return_value.wrap = lambda data: data
        mock_sasl_client.return_value.unwrap = lambda data: data
        mock_sasl_client.return_value.mechanism = 'GSSAPI'

        _HOST = 'HOST_TO_THRIFT_SERVER'
        _HOSTS = [_HOST, 'HOST_TO_THRIFT_SERVER2']

        pool = happybase_kerberos_patch.KerberosConnectionPool(
            size=3, host=_HOST, protocol='compact', use_kerberos=True)
        # connection.tables()
        mock_tsocket.assert_called_with(
            _HOST, happybase_kerberos_patch.DEFAULT_PORT)
        mock_sasl_client.assert_called_with(
            'HOST_TO_THRIFT_SERVER', u'hbase', u'GSSAPI')
        self.mock_transport.return_value.read = mock_read(
            self.connect_received_data)
        pool = happybase_kerberos_patch.KerberosConnectionPool(
            size=3, hosts=_HOSTS, protocol='compact', use_kerberos=True)
        self.mock_transport.return_value.read = mock_read(
            self.connect_received_data)
        pool = happybase_kerberos_patch.KerberosConnectionPool(
            size=3, hosts=','.join(_HOSTS), protocol='compact', use_kerberos=True)

    def test_create_connection(
            self, 
            mock_sasl_client, 
            mock_tsocket, 
            ):
        '''test kerberos connection'''
        self.data_to_receive = b'test'
        self.mock_transport.return_value.is_open.return_value = False
        self.mock_transport.return_value.read = mock_read(
            pack(">BI", happybase_kerberos_patch.TSaslClientTransport.OK, len(self.data_to_receive)) +
            self.data_to_receive +
            pack(">BI", happybase_kerberos_patch.TSaslClientTransport.COMPLETE, len(self.data_to_receive)) +
            self.data_to_receive
        )
        mock_sasl_client.return_value.wrap = lambda data: data
        mock_sasl_client.return_value.unwrap = lambda data: data
        mock_sasl_client.return_value.mechanism = 'GSSAPI'

        _HOST = 'HOST_TO_THRIFT_SERVER'
        connection = happybase_kerberos_patch.KerberosConnection(
            _HOST, protocol='compact', use_kerberos=True, table_prefix='featurebank:')
        # connection.tables()
        mock_tsocket.assert_called_with(_HOST, happybase_kerberos_patch.DEFAULT_PORT)
        mock_sasl_client.assert_called_with(
            'HOST_TO_THRIFT_SERVER', u'hbase', u'GSSAPI')


if __name__ == '__main__':
    unittest.main()
