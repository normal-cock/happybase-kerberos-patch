from setuptools import setup, find_packages
version = "0.1.3"
setup(
    name="happybase-kerberos-patch",
    version=version,
    py_modules=['happybase_kerberos_patch'],
    test_suite='tests',
    install_requires=[
        'pykerberos',
        'pure-sasl',
        'happybase>=1.2.0',
    ],
)
