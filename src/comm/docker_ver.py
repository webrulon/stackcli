import os

def docker_ver():
	SECRET_KEY = os.environ.get('AM_I_IN_A_DOCKER_CONTAINER', False)
	return SECRET_KEY