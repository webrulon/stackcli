import sys
sys.path.append( '../../..' )
from src.user.database.database import MongoClient
from src.user.user import User
import random
import string
import hashlib
from datetime import date, timedelta
import json 
from pathlib import Path

class License(object):
	"""docstring for License"""
	def __init__(self, ):
		super(License, self).__init__()
	
	def generateLicense(self, user, days_lic):
		# generates license
		exp_date = date.today()+timedelta(days=days_lic)
		lic = [random.choice(string.ascii_letters) for i in range(15)]
		license = ""
		for ele in lic: 
			license += ele

		dic_cloud = {
			'username' : user.username,
			'exp_day' : exp_date.day,
			'exp_month' : exp_date.month,
			'exp_year' : exp_date.year,
			'license'  : license
		}

		mongo_client = MongoClient()
		lic_db = mongo_client.getDatabase('lic_db')
		lic_col = mongo_client.getCollection('lic_data',lic_db)

		mongo_client.insert2Collection(dic_cloud,lic_col)

		dic_local = {
			'user' : user.username,
			'license'  : hashlib.sha256(license.encode('utf-8')).hexdigest()
		}

		json_object = json.dumps(dic_local)
		with open(str(Path.home())+"/stack_license.lic", "w") as outfile:
			outfile.write(json_object)

		return True

	def checkLicense(self, user):
		mongo_client = MongoClient()
		lic_db = mongo_client.getDatabase('lic_db')
		lic_col = mongo_client.getCollection('lic_data',lic_db)

		# checks user does not exist yet
		user_id = mongo_client.findInCollection(lic_col,{'username' : user.username})

		l = 0
		for x in user_id:
			l = l + 1
			ref = hashlib.sha256(x['license'].encode('utf-8')).hexdigest()
			lic = x

		f = open(str(Path.home())+"/stack_license.lic")
		data = json.load(f)
		f.close()

		# checks if the license is valide
		valid = True

		if date.today().year > lic['exp_year']:
			valid = False
		elif date.today().year == lic['exp_year']:
			if date.today().month > lic['exp_month']:
				valid = False
			elif date.today().month == lic['exp_month']:
					if date.today().day > lic['exp_day']:
						valid = False
		
		if not valid:
			print('license expired...')
			return False

		if ref != data['license']:
			print('invalid license')
			return False

		print('valid stack_license')
		return True

def main():
	usr = User()
	usr.loginUser()

	lic = License()
	lic.generateLicense(usr,100)
	lic.checkLicense(usr)

	usr.deleteUser()

if __name__ == '__main__':
	main()