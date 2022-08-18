from src.core.init import Initializer
from src.core.core import *
from src.user.user import User
from src.user.license.license import License
from src.storage.classes.s3 import S3Bucket
from src.storage.classes.local import Local

if __name__ == '__main__':
	# connects to S3 bucket and creates dataset
	# cloud = S3Bucket('stacktest123')
	# cloud.connectBucket()
	# cloiud.createDataset('dataset1/')
	cloud = Local()
	cloud.createDataset('/Users/bernardo/dataset1/')
	print('dataset connected')

	init = Initializer(cloud)
	init.removeSetup()
	print('init created')

	init.setupDataset()
	print('.stack/ folder added')

	# # does a commit with no changes
	commit(init)
	printHistory(init)
	print('commit completed')

	# adds another file
	import time
	start_time = time.time()
	add(init,['/Users/bernardo/Downloads/parquet/'])
	print("upload time: --- %s seconds ---" % (time.time() - start_time))
	commit(init)
	init.storage.resetBuffer()

	printHistory(init)
	print('commit completed')