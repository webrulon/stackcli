import sys
sys.path.append( '../../../' )
import pymongo
import urllib 
from dotenv import *

config = dotenv_values()

class MongoClient(object):
	"""docstring for Database"""
	def __init__(self):
		super(MongoClient, self).__init__()
		uri = "mongodb+srv://baceituno:"+urllib.parse.quote(config['MONGODB_API_KEY'])+"@cluster0.utuloy2.mongodb.net/?retryWrites=true&w=majority"
		self.client = pymongo.MongoClient(uri)
		self.client.test

	def getDatabase(self, db_name):
		return self.client[db_name]

	def getCollection(self, collection, db):
		return db[collection]

	def insert2Collection(self, item, collection):
		return collection.insert_one(item)

	def deleteFromCollection(self, item, collection):
		return collection.delete_one(item)
		
	def insert_many2Collection(self, items, collection):
		return collection.insert_many(items)
		
	def findInCollection(self, collection, query):
		return collection.find(query)

if __name__ == '__main__':
	client = MongoClient()

	db = client.getDatabase('db0')
	collection = client.getCollection('col0',db)
	res = client.insert2Collection({'a' : 'b'},collection)
	res = client.findInCollection(collection,{'a':'b'})
