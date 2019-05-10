# happybase-kerberos-patch

## Introduction
This is a patch for happybase to support kerberos when connect to hbase thrift server.

# Installment
    pip install -U git+https://github.com/normal-cock/happybase-kerberos-patch.git

## Usage:
    from happybase_kerberos_patch import KerberosConnection, KerberosConnectionPool
    connection = KerberosConnection('HOST_TO_THRIFT_SERVER', protocol='compact', use_kerberos=True)
    test_table = connection.table('test')
    # insert
    test_table.put('row_key_1', {'f1:q1':'v1'})
    # get data
    print test_table.row('row_key_1')

    pool = KerberosConnectionPool(size=3, host='...', protocol='compact', use_kerberos=True)
    with pool.connection() as connection:
        test = connection.table('test')
        print test_table.row('row_key_1')
        
    # multiple thrift servers high avaliable
    pool = KerberosConnectionPool(size=3, hosts=['thrift1', 'thrift2'], protocol='compact', use_kerberos=True)
    with pool.connection() as connection:
        test = connection.table('test')
        print test_table.row('row_key_1')

## Important points to remember:
* Only support "compact" protocol and "buffered" transport
* Only support happybase>=1.0.0

