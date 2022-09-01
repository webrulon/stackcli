import sys
sys.path.append( '..' )
from src.core.init import Initializer
from src.core.core import *
from src.storage.classes.s3 import S3Bucket
from src.storage.classes.gcs import GCSBucket
from src.storage.classes.local import Local
from pathlib import Path
import pickle

class API(object):
    """docstring for CLI"""
    def __init__(self, reset=False):
        super(API, self).__init__()
        self.key_bin = None
        if not Path(str(Path.home())+'/datasets.stack').exists():
            file = open(str(Path.home())+'/datasets.stack', 'wb')
            pickle.dump(config,file)
            file.close()
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
                self.dataset_name = self.Initializer.storage.dataset
                self.storage_name = self.Initializer.storage.dataset
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
        
    def init(self, storage = None):
        # builds a config file       
        if Path(str(Path.home())+'/config.stack').exists():
            config = {} 
            if storage == None:
                return False
            if 's3' in storage.lower():
                bucket_data = storage.split("/")[1:]
                config['bucket'] = bucket_data[1]
                config['dataset'] = ''.join(bucket_data[2:])+'/'
                config['type'] = 's3'
            elif 'gs' in storage.lower():
                bucket_data = storage.split("/")[1:]
                config['bucket'] = bucket_data[1]
                config['dataset'] = ''.join(bucket_data[2:])+'/'
                config['type'] = 'gcs'
            else:
                config['dataset'] = storage
                config['type'] = 'local'
            config['storage'] = storage
            # stores the config file

            print('Initializing dataset in ' + storage.lower())

            file = open(str(Path.home())+'/config.stack', 'wb')
            pickle.dump(config,file)
            file.close()
            # creates dataset
            return True
        else:
            print('Creating config file')
            config = {'storage': ''}
            file = open(str(Path.home())+'/config.stack', 'wb')
            pickle.dump(config,file)
            file.close()
            return False

    def set_gs_key(self, file):
        print('adding key file')
        self.key_bin = file
        return True

    def get_datasets(self):
        file1 = open(str(Path.home())+'/datasets.stack', 'rb')
        datasets = pickle.load(file1)
        file1.close()
        return datasets

    def print_datasets(self):
        file1 = open(str(Path.home())+'/datasets.stack', 'rb')
        datasets = pickle.load(file1)
        file1.close()

        if len(datasets.keys()) > 0:
            for s in datasets.keys():
                print('-- '+datasets[s]['name']+' in '+datasets[s]['storage'])
            print('')
            print('run \'stackcli connect [dataset_uri]\' to connect one of these datasets')
        else:
            print('no datasets to show')
        return True
    
    def connect_post_web(self, name='My Dataset', keys={}):
        
        print('connecting to '+name)
        file2 = open(str(Path.home())+'/config.stack', 'rb')
        config = pickle.load(file2)
        file2.close()

        self.storage_name = config['storage']
        self.dataset_name = config['dataset']
        print(config)

        try: 
            file1 = open(str(Path.home())+'/datasets.stack', 'rb')
            datasets = pickle.load(file1)
            file1.close()
        except:
            datasets = {}

        is_not_there = True
        for s in datasets.keys():
            if datasets[s]['storage'] == self.storage_name:
                is_not_there = False

        if is_not_there:
            file1 = open(str(Path.home())+'/datasets.stack', 'wb')
            datasets[self.storage_name] = {'storage': self.storage_name, 'name': name, 'type': config['type']}
            pickle.dump(datasets,file1)
            file1.close()

        if config['type'] == 'local':
            cloud = Local()
            cloud.createDataset(config['dataset'])
            self.Initializer = Initializer(cloud)
            self.dataset_name = self.Initializer.storage.dataset
            self.storage_name = self.Initializer.storage.dataset
        elif config['type'] == 's3':
            cloud = S3Bucket(config['bucket'])
            cloud.connect_bucket_api(keys)
            cloud.createDataset(config['dataset'])
            self.Initializer = Initializer(cloud)
        elif config['type'] == 'gcs':
            cloud = GCSBucket(config['bucket'])
            cloud.connect_bucket_api(self.key_bin)
            cloud.createDataset(config['dataset'])
            self.Initializer = Initializer(cloud)
        else:
            self.Initializer = None
        return True

    def connect_post_cli(self):
        file2 = open(str(Path.home())+'/config.stack', 'rb')
        config = pickle.load(file2)
        file2.close()

        self.storage_name = config['storage']
        self.dataset_name = config['dataset']
        
        try: 
            file1 = open(str(Path.home())+'/datasets.stack', 'rb')
            datasets = pickle.load(file1)
            file1.close()
        except:
            datasets = {}

        is_not_there = True
        for s in datasets.keys():
            if datasets[s]['storage'] == self.storage_name:
                is_not_there = False

        if is_not_there:
            name = input('please give a name to your dataset: ')
            file1 = open(str(Path.home())+'/datasets.stack', 'wb')
            datasets[self.storage_name] = {'storage': self.storage_name, 'name': name, 'type': config['type']}
            pickle.dump(datasets,file1)
            file1.close()

        if config['type'] == 'local':
            cloud = Local()
            cloud.createDataset(config['dataset'])
            self.Initializer = Initializer(cloud)
            self.dataset_name = self.Initializer.storage.dataset
            self.storage_name = self.Initializer.storage.dataset
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
        return True

    def connect_post_api(self, name='My Dataset'):
        file2 = open(str(Path.home())+'/config.stack', 'rb')
        config = pickle.load(file2)
        file2.close()

        self.storage_name = config['storage']
        self.dataset_name = config['dataset']
        
        try: 
            file1 = open(str(Path.home())+'/datasets.stack', 'rb')
            datasets = pickle.load(file1)
            file1.close()
        except:
            datasets = {}

        is_not_there = True
        for s in datasets.keys():
            if datasets[s]['storage'] == self.storage_name:
                is_not_there = False

        if is_not_there:
            file1 = open(str(Path.home())+'/datasets.stack', 'wb')
            datasets[self.storage_name] = {'storage': self.storage_name, 'name': name, 'type': config['type']}
            pickle.dump(datasets,file1)
            file1.close()

        if config['type'] == 'local':
            cloud = Local()
            cloud.createDataset(config['dataset'])
            self.Initializer = Initializer(cloud)
            self.dataset_name = self.Initializer.storage.dataset
            self.storage_name = self.Initializer.storage.dataset
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
        return True

    def disconnectDataset(self, storage=''):
        assert(len(storage) > 1)
        file1 = open(str(Path.home())+'/datasets.stack', 'rb')
        datasets = pickle.load(file1)
        file1.close()

        print('disconnecting from ' + storage)

        for s in datasets.keys():
            if datasets[s]['storage'] == storage:
                del datasets[s]
                file1 = open(str(Path.home())+'/datasets.stack', 'wb')
                pickle.dump(datasets,file1)
                file1.close()
                return True
        return False

    def connectDataset(self, storage=None):
        # checks if another dataset exists
        # builds a config file
        if Path(str(Path.home())+'/config.stack').exists():
            file2 = open(str(Path.home())+'/config.stack', 'rb')
            config = pickle.load(file2)
            
            print('initializing dataset in ' + storage.lower())
            config['dataset'] = storage

            if config['type'] == 'local':
                cloud = Local()
                cloud.createDataset(config['dataset'])
                config['dataset'] = cloud.dataset
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

            # stores the config file
            file = open(str(Path.home())+'/config.stack', 'wb')
            pickle.dump(config,file)
            file.close()

            # creates dataset
            return True
        else:
            return False

    
    def upload_file_binary(self, filename='', binary=''):
        assert(filename != '')
        assert(binary != '')
        add_from_finary(self.Initializer, filename, binary)
        return True

    def start_check(self):
        try:
            return self.Initializer.start_check()
        except:
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
            print('downloading all contents')
            return self.pull_all(version)
        else:
            if not self.Initializer.storage.dataset in file:
                file = self.Initializer.storage.dataset + file
                print('downloading ' + file)
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
                    gtfo = Falsfe
                    break

        self.Initializer.storage.resetBuffer()
        file_array = {binary: binary, key: file}

        return file_array

    def commits_version(self, version = 2, l = 5, page = 0):
        assert(int(l) > 0)
        assert(int(version) >= 0)
        assert(int(page) >= 0)

        metapath = self.Initializer.prefix_meta+'history.json'
        history = json.load(self.Initializer.storage.loadFileGlobal(metapath))
        self.Initializer.storage.resetBuffer()

        response = {}
        idx = 0

        i_p = int(page)*int(l)
        i_f = min((int(page)+1)*int(l),len(history[str(int(version))]['commits']))

        # goes over the commits
        for i in range(i_p, i_f):
            # reads each file version
            commit = history[str(int(version))]['commits'][i]
            cmit = json.load(self.Initializer.storage.loadFileGlobal(commit))
            self.Initializer.storage.resetBuffer()
            response[idx] = {'key': cmit['key'], 'source': cmit['source'], 'date': history[str(int(version))]['date'], 'comment': cmit['comment']}
            idx = idx + 1

        return {'commits': response, 'len': len(history[str(int(version))]['commits'])}

    def key_versions(self, key = '', l = 5, page = 0):
        assert(int(l) > 0)
        assert(int(page) >= 0)

        key_hist = get_key_history(self.Initializer, self.Initializer.storage.dataset + key)

        response = {}
        idx = 0

        i_p = int(page)*int(l)
        i_f = min((int(page)+1)*int(l),len(key_hist))

        # goes over the commits
        for i in range(i_p, i_f):
            # reads each file version
            response[idx] = key_hist[i]
            idx = idx + 1

        return {'commits': response, 'len': len(key_hist)}

    def status(self):
        metapath = self.Initializer.prefix_meta+'current.json'
        return json.load(self.Initializer.storage.loadFileGlobal(metapath))

    def remove(self, key, subpath=''):
        if len(subpath)>1:
            if subpath[-1] != '/':
                subpath = subpath + '/'
        remove(self.Initializer,[key],subpath)
        return True

    def remove_commit(self, version = '-1'):
        assert(int(version) >= 0)
        
        metapath = self.Initializer.prefix_meta+'history.json'
        history = json.load(self.Initializer.storage.loadFileGlobal(metapath))
        self.Initializer.storage.resetBuffer()

        for i in range(len(history[str(int(version))]['commits'])):
            commit = history[str(int(version))]['commits'][i]
            cmit = json.load(self.Initializer.storage.loadFileGlobal(commit))
            self.Initializer.storage.resetBuffer()
            removeGlobal(self.Initializer, [cmit['diff']])

        return True

    def remove_key_diff(self, key, version):
        remove_diff(self.Initializer,self.Initializer.storage.dataset+key,int(version))
        return True

    def remove_key_full(self, key, version):
        remove_full(self.Initializer,self.Initializer.storage.dataset+key)
        return True

    def commit(self, comment=''):
        commit(self.Initializer, comment)
        print('commit done!')
        return True

    def loadCommitMetadata(self, commit_file):
        try:
            return json.load(self.Initializer.storage.loadFileGlobal(commit_file))
        except:
            return {}

    def check_if_setup(self):
        self.Initializer.setupDataset()
        print('setup complete!')
        return True

    def load_file_binary(self, file):
        print('loading '+file+'...')
        return self.Initializer.storage.loadFile(file)

    def load_file_binary_bytes(self, file, bi, bf):
        print('loading '+file+'...')
        return self.Initializer.storage.loadFileBytes(file,bi,bf)

    def reset(self):
        self.Initializer.removeSetup()
        self.Initializer.setupDataset()
        print('setup complete!')
        return True

    def revert(self, version=0):
        assert(version != '')
        revertCommit(self.Initializer, int(version))
        commit(self.Initializer, 'reverted to version' + str(version))

    def revert_file(self, key, version):
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