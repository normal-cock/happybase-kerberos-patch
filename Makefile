test:
	coverage run setup.py test -v
	coverage report --include happybase_kerberos_patch.py -m