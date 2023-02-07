import json
from supabase import create_client, Client
from faker import Faker
import faker_commerce
import hashlib
import pymongo

def add_entries_to_squad_dataset_mongo(dset, dp_count):
	fake = Faker()
	fake.add_provider(faker_commerce.Provider)
	
	schema = []

	for i in range(dp_count):
		dp = {}
		dp['title'] = 'dp'
		dp['context'] = f"{fake.company()} has {fake.random_int(40, 169)} employees, and is based in {fake.country()}"
		dp['question'] = f"Is {fake.company()} real?"
		dp['answers'] = [{'text': 'no', 'start': i}, {'text': 'yos', 'start': i+1}]
		dp['tags'] = ['hello', 'not hello']
		dp['metadata'] = [{'annotator': 'Bernardo'}, {'annotator': 'Bernardo'}]
		dp['slices'] = ['test']
		dp['versions'] = [{'dp': dp.copy(),'type': 'added', 'source': 'N/A', 'comment': '', 'file': 'raw', 'date': 'today'}]
		dp['key'] = hashlib.md5((dp['title']+dp['context']+dp['question']).encode('utf-8')).hexdigest()

		dset.find_one_and_replace({'key': dp['key']}, dp, upsert=True)
	return True


def add_entries_to_squad_dataset(supabase, dp_count):
	fake = Faker()
	fake.add_provider(faker_commerce.Provider)
	
	schema = []

	for i in range(dp_count):
		dp = {}
		dp['filename'] = str(500-i)
		dp['title'] = 'dp'
		dp['context'] = f"{fake.company()} has {fake.random_int(40, 169)} employees, and is based in {fake.country()}"
		dp['question'] = f"Is {fake.company()} real?"
		dp['answers'] = [{'text': 'no', 'start': i}, {'text': 'yos', 'start': i+1}]
		dp['tags'] = ['hello', 'not hello']
		dp['metadata'] = [{'annotator': 'Bernardo'}, {'annotator': 'Bernardo'}]
		dp['slices'] = ['test']
		dp['versions'] = [{'dp': dp.copy(),'type': 'added', 'source': 'N/A', 'comment': '', 'file': 'raw', 'date': 'today'}]
		schema.append(dp)
	data = supabase.table('squad2').insert(schema).execute()
	data_json = json.loads(data.json())

	return data_json

def main_supabase():
	dp_count = 20
	url: str = os.environ.get("SUPABASE_URL")
	key: str = os.environ.get("SUPABASE_KEY")
	supabase: Client = create_client(url, key)
	supabase.table('squad2')
	fk_list = add_entries_to_squad_dataset(supabase, dp_count)
	print(fk_list)


def main_mongo():
	dp_count = 20
	client = pymongo.MongoClient("mongodb://localhost:27017/")
	db = client["paperplane"]
	dset = db["dataset 1"]
	# add_entries_to_squad_dataset_mongo(dset, dp_count)
	print('\n\nDataset\n\n')
	for x in dset.find({'key': '596a27b90daeeeb7f27894dc65d398a4' }):
		print(x)
		x['title'] = 'home'
		dset.find_one_and_replace({'key': x['key']}, x, upsert=True)
		print('\n\n')

	for x in dset.find({'key': '596a27b90daeeeb7f27894dc65d398a4' }):
		print(x)
		print('\n\n')

main_mongo()