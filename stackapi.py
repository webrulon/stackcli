from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.core.init import Initializer
from src.core.core import *
from src.user.user import User
from src.user.license.license import License
from src.storage.classes.s3 import S3Bucket
from src.storage.classes.gcs import GCSBucket
from src.storage.classes.local import Local
from pathlib import Path
import pickle

class API(object):
    """docstring for CLI"""
    def __init__(self, reset=False):
        super(API, self).__init__()
        if reset:
            self.Initializer = None
        elif Path(str(Path.home())+'/config.stack').exists():
            file2 = open(str(Path.home())+'/config.stack', 'rb')
            config = pickle.load(file2)
            file2.close()
            self.storage_name = config['storage']
            self.dataset_name = config['dataset']
            if config['type'] == 'local':
                cloud = Local()
                cloud.createDataset(config['dataset'])
                self.Initializer = Initializer(cloud)
            elif config['type'] == 's3':
                cloud = S3Bucket(config['bucket'])
                cloud.connectBucket()
                cloud.createDataset(config['dataset'])
                self.Initializer = Initializer(cloud)
            elif config['type'] == 'gcs':
                cloud = GCSBucket(config['bucket'])
                cloud.connectBucket()
                cloud.createDataset(config['dataset'])
                self.Initializer = Initializer(cloud)
            else:
                self.Initializer = None
        else:
            self.Initializer = None
        
    def init(self, storage = None):
        # builds a config file
        try: 
            config = {}
            print('initializing dataset in ' + storage.lower())
            if storage == None:
                pass
            if 's3' in storage.lower():
                bucket_data = storage.split("/")[1:]
                config['bucket'] = bucket_data[1]
                config['dataset'] = bucket_data[2]+'/'
                config['type'] = 's3'
            elif 'gs' in storage.lower():
                bucket_data = storage.split("/")[1:]
                config['bucket'] = bucket_data[1]
                config['dataset'] = bucket_data[2]+'/'
                config['type'] = 'gcs'
            else:
                config['dataset'] = storage
                config['type'] = 'local'
            config['storage'] = storage
            # stores the config file
            file = open(str(Path.home())+'/config.stack', 'wb')
            pickle.dump(config,file)
            file.close()

            # creates dataset
            return True
        except:
            return False

    def connectDataset(self, dataset):
        # checks if another dataset exists
        # builds a config file
        if Path(str(Path.home())+'/config.stack').exists():
            file2 = open(str(Path.home())+'/config.stack', 'rb')
            config = pickle.load(file2)
            
            print('initializing dataset in ' + dataset)
            config['dataset'] = storage
            # stores the config file
            file = open(str(Path.home())+'/config.stack', 'wb')
            pickle.dump(config,file)
            file.close()

            if config['type'] == 'local':
                cloud = Local()
                cloud.createDataset(config['dataset'])
                self.Initializer = Initializer(cloud)
            elif config['type'] == 's3':
                cloud = S3Bucket(config['bucket'])
                cloud.connectBucket()
                cloud.createDataset(config['dataset'])
                self.Initializer = Initializer(cloud)
            elif config['type'] == 'gcs':
                cloud = GCSBucket(config['bucket'])
                cloud.connectBucket()
                cloud.createDataset(config['dataset'])
                self.Initializer = Initializer(cloud)
            else:
                self.Initializer = None

            # creates dataset
            return True
        else:
            return False

    def getURI(self):
        return {'storage': self.storage_name, 'dataset': self.dataset_name}

    def add(self, path, subpath=''):
        if len(subpath)>1:
            if subpath[-1] != '/':
                subpath = subpath + '/'
        add(self.Initializer,[path],subpath)
        return True

    def pull_all(self,version = 'current'):
        print('downloading files from last commit')
        metapath = self.Initializer.prefix_meta + 'current.json'
        current = json.load(self.Initializer.storage.loadFileGlobal(metapath))
        pull(self.Initializer, current['keys'], version)
        return True

    def pull(self, file, version = 'current'):
        if file == '.' or file == 'all':
            return self.pull_all(version)
        else:
            if not self.Initializer.storage.dataset in file:
                file = self.Initializer.storage.dataset + file
            pull(self.Initializer, [file], version)
        return True

    def pull_file(self, file, version = 'current'):
        if not self.Initializer.storage.dataset in file:
            file = self.Initializer.storage.dataset + file
        
        if version == 'current':
            # saves each file
            for key in files:
                binary = self.Initializer.storage.loadFileGlobal(key)
        else:
            gtfo = False
            # finds the commit of interest
            metapath = self.Initializer.prefix_meta+'history.json'
            history = self.Initializer.load(self.Initializer.storage.loadFileGlobal(metapath))
            for key in files:
                if key[-1] == '/':
                    print('Do not pull directories')
                    return False
                for i in range(len(history),int(version)-1,-1):
                    for commit in history[str(i)]['commits']:
                        # reads each file version
                        cmit = json.load(self.Initializer.storage.loadFileGlobal(commit))
                        if str(cmit['version']) == version and cmit['key'] == key:
                            if cmit['type'] != 'remove':
                                key = self.Initializer.prefix_diffs + key + '/' + str(cmit['version']).zfill(10)
                                binary = self.Initializer.storage.loadFileGlobal(key).read()
                                self.Initializer.storage.resetBuffer()
                            gtfo = True
                        if gtfo:
                            break
                if gtfo:
                    gtfo = False
                    break

        self.Initializer.storage.resetBuffer()
        file_array = {binary: binary, key: file}

        return file_array

    def status(self):
        metapath = self.Initializer.prefix_meta+'current.json'
        return json.load(self.Initializer.storage.loadFileGlobal(metapath))

    def remove(self, key, subpath=''):
        if len(subpath)>1:
            if subpath[-1] != '/':
                subpath = subpath + '/'
        remove(self.Initializer,[key],subpath)
        return True

    def commit(self, comment=''):
        print(comment)
        commit(self.Initializer, comment)
        print('commit done!')
        return True

    def loadCommitMetadata(self, commit_file):
        try:
            return json.load(self.Initializer.storage.loadFileGlobal(commit_file))
        except:
            return {}

    def reset(self):
        self.Initializer.removeSetup()
        self.Initializer.setupDataset()
        print('reset complete')
        return True

    def revert(self, version=0):
        try:
            assert(version != '')
            revertCommit(self.Initializer, int(version))
            return True
        except:
            return False

    def revertFile(self, key, version):
        try: 
            if self.Initializer.storage.dataset in key:
                revertFile(self.Initializer, key, int(version))
            else:
                revertFile(self.Initializer, self.Initializer.storage.dataset+key, int(version))
            self.Initializer.storage.resetBuffer()

            return True
        except:
            return False

    def history(self):
        metapath = self.Initializer.prefix_meta+'history.json'
        return json.load(self.Initializer.storage.loadFileGlobal(metapath))

    def commits_version(self, version = 0):
        metapath = self.Initializer.prefix_meta+'history.json'
        history = json.load(self.Initializer.storage.loadFileGlobal(metapath))
        self.Initializer.storage.resetBuffer()

        response = {}
        idx = 0
        # goes over the commits
        for commit in history[str(version)]['commits']:
            # reads each file version
            cmit = json.load(self.Initializer.storage.loadFileGlobal(commit))
            response[idx] = {'key': cmit['key'], 'source': cmit['source'], 'date': history[str(version)]['date'], 'comment': cmit['comment']}
            idx = idx + 1
        self.Initializer.storage.resetBuffer()
        return response

    def lastNcommits(self, n = 0):
        metapath = self.Initializer.prefix_meta+'history.json'
        history = json.load(self.Initializer.storage.loadFileGlobal(metapath))
        self.Initializer.storage.resetBuffer()

        response = {}
        idx = 0

        # goes over history
        for i in range(len(history),0,-1):
            for commit in history[str(i)]['commits']:
                # reads each file version
                cmit = json.load(self.Initializer.storage.loadFileGlobal(commit))
                response[idx] = {'source': cmit['source'], 'date': history[str(i)]['date'], 'comment': cmit['comment']}

                idx += 1
                if int(idx) >= int(n):
                    break
            if int(idx) >= int(n):
                    break

        return response


    def diff(self, v1, v0, file=''):
        printDiff(self.Initializer, v1, v0, file)
        return True

    def logout(self):
        print('loging you out of ' + self.Initializer.storage.type + '://' + self.Initializer.storage.BUCKET_NAME + '/' + self.Initializer.storage.dataset)
        import os
        os.remove(str(Path.home())+'/config.stack')
        return True

# API Definition
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],  
)

# checks if local files are installed
try:
    api = API()
except:
    import os
    os.remove(str(Path.home())+'/config.stack')
    api = API()
    api.init()

# End-points

@app.get("/init")
async def init(uri):
    return {'success': api.init(uri)}

@app.get("/connect")
async def connect(dataset):
    return {'success': api.connectDataset(dataset)}

@app.get("/uri")
async def uri_api():
    try:
        return api.getURI()
    except:
        return {}

@app.get("/history")
async def history_api():
    try:
        return api.history()
    except:
        return {}

@app.get("/commits_version")
async def commits_version_api(n=0):
    try:
        return api.commits_version(n)
    except:
        return {}

@app.get("/last_n_commits")
async def last_n_commits_api(n=5):
    try:
        return api.lastNcommits(n)
    except:
        return {}

@app.get("/last_commits_from_hist_api")
async def last_commits_from_hist_api(n=1):
    try:
        return api.getHistoryCommits(n)
    except:
        return {}

@app.get("/status")
async def status_api():
    try:
        return api.status()
    except:
        return {}

@app.post("/commit")
async def commit_api(comment=''):
    return {'success': api.commit(comment)}

@app.get("/get_commit_metadata")
async def get_commit_meta_api(commit):
    try:
        return api.loadCommitMetadata(commit)
    except:
        return {}

@app.get("/pull_api")
async def pull_file_api(file):
    try:
        return api.storage.pull_file(file)
    except:
        return {}

@app.get("/pull_metadata")
async def pull_metadata_api(file):
    try:
        return api.storage.loadFileMetadata(file)
    except:
        return {}

@app.get("/diff")
async def diff_api(v2,v1):
    return ''

@app.get("/diff_file")
async def diff_file_api(file,v2,v1):
    return ''

@app.get("/revert")
async def revert_api(version):
    return {'success': api.revert(version)}

@app.get("/revert_file")
async def revert_file_api(file,version):
    return {'success': api.revert(file, version)}