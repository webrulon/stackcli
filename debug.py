from src.core.init import Initializer
from src.core.core import *
from src.user.user import User
from src.user.license.license import License
from src.storage.classes.s3 import S3Bucket
from src.storage.classes.local import Local

if __name__ == '__main__':
	# connects to S3 bucket and creates dataset
	# cloud = S3Bucket('stacktest123')
	# cloud.connect_bucket()
	# cloiud.create_dataset('dataset1/')
	cloud = Local()
	cloud.create_dataset('/Users/bernardo/dataset1/')
	print('dataset connected')

	init = Initializer(cloud)
	init.remove_setup()
	print('init created')

	init.setup_dataset()
	print('.stack/ folder added')

	# # does a commit with no changes
	commit(init)
	print_history(init)
	print('commit completed')

	# adds another file
	import time
	start_time = time.time()
	add(init,['/Users/bernardo/Downloads/parquet/'])
	print("upload time: --- %s seconds ---" % (time.time() - start_time))
	commit(init)
	init.storage.reset_buffer()

	print_history(init)
	print('commit completed')