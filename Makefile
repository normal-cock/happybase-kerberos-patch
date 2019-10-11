test:
	coverage run --source happybase_kerberos_patch setup.py test
	coverage report --include happybase_kerberos_patch.py -m