from setuptools import setup, find_packages
setup(
    name="happybase-kerberos-patch",
    version="0.1",
    py_modules=['happybase_kerberos_patch'],
    install_requires=[
        'kerberos',
        'pure-sasl',
        'happybase>=1.0.0',
    ],
)