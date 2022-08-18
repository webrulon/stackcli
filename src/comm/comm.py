import webbrowser
from nylas import APIClient
import dotenv

dotenv.config()
NYLAS_KEY_1 = process.env.NYLAS_KEY_1
NYLAS_KEY_2 = process.env.NYLAS_KEY_2
NYLAS_KEY_3 = process.env.NYLAS_KEY_3

class Communications(object):
	"""docstring for Communications"""
	def __init__(self, user):
		super(Communications, self).__init__()
		self.email = user.email
		self.nylas = APIClient(
			NYLAS_KEY_1,
			NYLAS_KEY_2,
			NYLAS_KEY_3)

	def requestSupport(self):
		webbrowser.open('https://calendly.com/eigen-team/eigen-team')
		return True

	def sendFeedback(self):
		draft = self.nylas.drafts.create()
		draft.subject = "feedback from user: "+self.email
		message = input('Type your feedback: ')
		draft.body = "Hello, I'm user "+self.email+"\n"+message+"\nBest!"
		draft.to = [{'name': 'B', 'email': 'getstack@mit.edu'}]

		try:
			draft.send()
		except:
			print('failure to send email')
			return False

		return True

	def sendUpdate(self, update):
		draft = self.nylas.drafts.create()
		draft.subject = "Update "+self.update.title
		draft.body = update.body
		draft.to = [{'name': 'B', 'email': self.email}]

		try:
			draft.send()
		except:
			print('failure to send email update')
			return False

		return True

if __name__ == '__main__':
	import sys
	sys.path.append( '../..' )
	from src.user.user import User
	usr = User()
	usr.loginUser()
	comm = Communications(usr)
	comm.sendFeedback()
