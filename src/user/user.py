import sys
sys.path.append( '../..' )
from src.user.database.database import MongoClient
import hashlib
import maskpass

class User(object):
	"""docstring for User"""
	def __init__(self):
		super(User, self).__init__()
		self.username = ""
		self.logged_in = ""
		self.email = ""
		self.mongo_client = MongoClient()
		self.user_db = self.mongo_client.getDatabase('user_db')
		self.user_col = self.mongo_client.getCollection('user_data',self.user_db)

	def loginUser(self):
		username = input("Enter username: ")
		password = maskpass.askpass(prompt="Enter password: ", mask="#")

		user_id = self.mongo_client.findInCollection(self.user_col,{'username' : username})

		# should be only once
		l = len(user_id)
		for x in user_id:
			ref = x['password']
			email = x['email']

		if l == 0:
			print('wrong username or password')
			return self.loginUser()

		hashed_password = hashlib.sha256(password.encode('utf-8')).hexdigest()

		if hashed_password == ref:
			self.username = username
			self.email = email
			return True
		else:
			print('wrong username or password')
			return self.loginUser()
		return self.loginUser()

	def createUser(self):
		# reads from prompt
		username = input("Enter username: ")
		password = maskpass.askpass(prompt="Enter password: ", mask="#")
		password2 = maskpass.askpass(prompt="Enter password again: ", mask="#")

		email = input("Enter your e-mail: ")
		email2 = input("Enter your e-mail again: ")

		if not email == email2:
			print('emails do not match...')
			print('Try again')
			return self.createUser()

		if not password == password2:
			print('passwords do not match...')
			print('Try again')
			return self.createUser()

		# checks user does not exist yet
		user_id = self.mongo_client.findInCollection(self.user_col,{'username' : username})

		for x in user_id:
			if x['username'] == username:
				print('Username taken!')
				return self.createUser()

		# encrypts password
		hashed_password = hashlib.sha256(password.encode('utf-8')).hexdigest()

		# stored user data
		self.mongo_client.insert2Collection({'username' : username,'email' : email, 'password' : hashed_password},self.user_col)
		self.username = username
		self.email = email
		return True

	def deleteUser(self):
		# reads from prompt
		username = input("Enter username: ")

		# checks user does not exist yet
		user_id = self.mongo_client.findInCollection(self.user_col,{'username' : username})

		l = 0
		for x in user_id:
			l = l + 1
			ref = x['password']

		if l == 0:
			ref = ''

		password = maskpass.askpass(prompt="Enter password: ", mask="#")
		password2 = maskpass.askpass(prompt="Enter password again: ", mask="#")

		if not password == password2:
			print('passwords do not match...')
			print('Operation aborted')
			return False

		hashed_password = hashlib.sha256(password.encode('utf-8')).hexdigest()

		if hashed_password == ref:
			print("Are you sure you want to proceed? all data will be erased")
			yn = input("[Y/n]: ")

			if yn == 'y' or yn == 'Y':
				# stored user data
				self.mongo_client.deleteFromCollection({'username' : username},self.user_col)

			return True
		else:
			print('wrong username or password')
			print('Operation aborted')
			return False

def main():
	usr = User()
	usr.createUser()
	usr.loginUser()
	usr.deleteUser()

if __name__ == '__main__':
	main()
