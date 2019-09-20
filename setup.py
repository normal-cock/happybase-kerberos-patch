from setuptools import setup, find_packages
version = "0.1.3"
setup(
    name="happybase-kerberos-patch",
    version=version,
    py_modules=['happybase_kerberos_patch'],
    install_requires=[
        'pykerberos',
        'pure-sasl',
        'happybase==1.1.0',
    ],
)
