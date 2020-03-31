from setuptools import setup, find_packages
version = "0.1.4"
setup(
    name="happybase-kerberos-patch",
    version=version,
    py_modules=['happybase_kerberos_patch'],
    test_suite='tests',
    install_requires=[
        'pykerberos',
        'thriftpy2==0.4.8',
        'pure-sasl==0.6.1',
        'happybase==1.2.0',
    ],
)
