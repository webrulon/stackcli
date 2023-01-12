from operator import le
import sys
sys.path.append( '..' )
from src.core.init import Initializer
from src.core.core import *
from src.training.projects import projects

from src.storage.classes.s3 import S3Bucket
from src.storage.classes.gcs import GCSBucket
from src.storage.classes.local import Local

from src.dataset.object_detection.yolo import yolo_schema
from src.dataset.object_detection.labelbox import labelbox_schema

from src.dataset.named_entity_recognition.spacy import spacy_ner_schema

from pathlib import Path
import pickle
import os
from io import StringIO
from src.comm.docker_ver import *
path_home = os.getenv('LCP_DKR')+'/' if docker_ver() else str(Path.home())
import pandas as pd

class API(object):
    """docstring for CLI"""
    def __init__(self, reset=False, cli=False):
        super(API, self).__init__()
        self.key_bin = None
        self.Initializer = None
        self.config = None
        self.schema_class = None
        self.current = None
        
        self.in_version = False
        self.selected_version = None
        self.version_keys = None

        self.filtered = False
        self.projects = None
        self.filters = {}
        if not Path(path_home+'/datasets.stack').exists():
            self.set_datasets({})
        if reset:
            self.Initializer = None
        
        if not Path(path_home+'/.config_stack').exists():
            self.config = {}
            self.set_config()
        else:
            self.config = self.get_config()
            try:
                self.storage_name = self.config['storage']
                self.dataset_name = self.config['dataset']
                ctype = self.config['type']
                schema = self.config['schema']
            except:
                self.config = {'storage': '', 'schema': 'files'}
                self.set_config()
                ctype = ''
            try:
                if ctype == 'local':
                    cloud = Local()
                    cloud.create_dataset(self.config['dataset'])
                    self.Initializer = Initializer(cloud)
                    self.dataset_name = self.config['storage']
                    self.storage_name = self.config['dataset']
                    self.Initializer.schema = schema
                elif ctype == 's3':
                    cloud = S3Bucket(self.config['bucket'])
                    if cli:
                        cloud.connect_bucket()
                    else:
                        cloud.reconnect_bucket_api()
                    cloud.create_dataset(self.config['dataset'])
                    self.Initializer = Initializer(cloud)
                    self.Initializer.schema = schema
                elif ctype == 'gcs':
                    cloud = GCSBucket(self.config['bucket'])
                    if cli:
                        cloud.connect_bucket()
                    else:
                        cloud.reconnect_bucket_api()
                    cloud.create_dataset(self.config['dataset'])
                    self.Initializer = Initializer(cloud)
                    self.Initializer.schema = schema
                else:
                    self.Initializer = None
            except:
                self.Initializer = None
        
    def init(self, storage = None):
        # builds a config file       
        if Path(path_home+'/.config_stack').exists():
            config = {} 
            if storage == None:
                return False
            if 's3:/' in storage.lower():
                bucket_data = storage.split("/")[1:]
                config['bucket'] = bucket_data[1]
                config['dataset'] = ''.join(bucket_data[2:])+'/'
                config['type'] = 's3'
            elif 'gs:/' in storage.lower():
                bucket_data = storage.split("/")[1:]
                config['bucket'] = bucket_data[1]
                config['dataset'] = ''.join(bucket_data[2:])+'/'
                config['type'] = 'gcs'
            else:
                if path_home in storage:
                    storage.replace(path_home,'')
                if storage[0] == '/':
                    storage = storage[1:]
                if storage[-1] == '/':
                    storage = storage[:-1]
                config['dataset'] = storage
                config['type'] = 'local'
            config['storage'] = storage
            
            # stores the config file
            self.config = config
            self.filtered = False
            # creates dataset
            return True
        else:
            config = {'storage': ''}
            self.config = config
            return False

    def init_config(self):
        # builds a config file       
        if Path(path_home+'/.config_stack').exists():
            return True
        else:
            config = {'storage': '', 'schema': 'files'}
            self.config = config
            self.set_config()
            return False

    def set_gs_key(self, file):
        self.key_bin = file
        return True

    def get_config(self):
        file2 = open(path_home+'/.config_stack', 'rb')
        config = pickle.load(file2)
        file2.close()
        self.config = config
        return config

    def set_config(self):
        file = open(path_home+'/.config_stack', 'wb')
        pickle.dump(self.config,file)
        file.close()

    def get_datasets(self):
        file1 = open(path_home+'/datasets.stack', 'rb')
        datasets = pickle.load(file1)
        file1.close()
        return datasets

    def get_labels(self, filename, version='current'):
        
        if(self.config['schema'] != 'files'):
            return self.schema_class.get_labels(filename, version)

        return []

    def set_bounding_boxes(self, val = 'false'):
        if self.config['schema'] == 'yolo' or self.config['schema'] == 'labelbox':
            if val == 'true':
                self.schema_class.bounding_box_thumbs = False
            else:
                self.schema_class.bounding_box_thumbs = True
        return True

    def load_thumbnail(self, file):
        if self.config['schema'] == 'yolo' or self.config['schema'] == 'labelbox':
            try: 
                return self.schema_class.get_thumbnail(file)
            except:
                return self.load_file_binary(file, 'current')    
        else:
            return self.load_file_binary(file, 'current')

    def set_labels(self, data):
        if self.config['schema'] == 'yolo' or self.config['schema'] == 'labelbox':
            if self.in_version:
                filename =  os.path.dirname(data['keyId'].replace(self.Initializer.prefix_diffs,''))
                in_version = True
                version = self.selected_version
                self.reset_version()
            else:
                in_version = False
                filename = data['keyId']
            self.schema_class.set_labels(filename, data)

            if in_version:
                self.select_version(version)
        elif self.config['schema'] == 'spacy_ner':
            if self.in_version:
                self.reset_version()
            else:
                filename = data['keyId']
            self.schema_class.set_labels(filename, data)

            if in_version:
                self.select_version(version)
        return {'success': True}

    def set_datasets(self, datasets):
        file1 = open(path_home+'/datasets.stack', 'wb')
        pickle.dump(datasets,file1)
        file1.close()

    def print_datasets(self):
        datasets = self.get_datasets()

        if len(datasets.keys()) > 0:
            print('List of remote datasets:\n')
            for s in datasets.keys():
                print('-- '+datasets[s]['name']+' in '+datasets[s]['storage'])
            print('')
            print('run \'stack connect [dataset_uri]\' to connect one of these datasets')
        else:
            print('no datasets to show')
        return True
    
    def get_dataset_name(self,uri):
        if uri == '':
            return ''
        datasets = self.get_datasets()
        if uri[-1] == '/':
            uri = uri[:-1]
        if uri[0] == '/':
            uri = uri[1:]
        for s in datasets.keys():
            if uri == datasets[s]['storage']:
                return datasets[s]['name']
        return ''

    def get_dataset_URI(self,name):
        datasets = self.get_datasets()
        for s in datasets.keys():
            if name == datasets[s]['name']:
                return datasets[s]['storage']
        return ''
    
    def set_schema(self):

        if self.config['schema'] == 'yolo':
            self.schema_class = yolo_schema(self.Initializer)
            try:
                metapath = self.schema_class.schema_path
                self.schema_class.schema = json.load(self.Initializer.storage.load_file_global(metapath))
            except:
                print('creating schema file')
                self.schema_class.create_schema_file()
                print('computing metadata')
                self.schema_class.compute_meta_data()
        elif self.config['schema'] == 'labelbox':
            self.schema_class = labelbox_schema(self.Initializer)
            try:
                metapath = self.schema_class.meta_path
                json.load(self.Initializer.storage.load_file_global(metapath))
            except:
                self.schema_class.create_schema_file()
                self.schema_class.compute_meta_data()
        elif self.config['schema'] == 'spacy_ner':
            self.schema_class = spacy_ner_schema(self.Initializer)
            try:
                metapath = self.schema_class.meta_path
                json.load(self.Initializer.storage.load_file_global(metapath))
            except:
                self.schema_class.create_schema_file()
                self.schema_class.compute_meta_data()

    def reset_schema(self):
        self.reset_version()
        if self.config['schema'] != 'files':
            self.schema_class.create_schema_file()
            self.schema_class.compute_meta_data()

    def merge(self, father='', child=''):
        self.reset_version()
        if father[-1] != '/':
            father += '/'
        if child[-1] != '/':
            child += '/'

        if 's3://' in father:
            uri = 's3://'+self.Initializer.storage.BUCKET_NAME+'/'
            if uri in child:
                uri = ''
        elif 'gs://' in father:
            uri = 'gs://'+self.Initializer.storage.BUCKET_NAME+'/'
            if uri in child:
                uri = ''
        else:
            uri = ''

        if path_home in child:
            child = child.replace(path_home,'')

        if path_home in father:
            father = father.replace(path_home,'')

        self.init(uri+child)
        self.connect_post_api()
        self.set_schema()

        if self.config['schema'] != 'files':
            self.schema_class.merge(goal=father)
        else:
            dataset = self.Initializer.storage.dataset
            if self.Initializer.storage.type == 'local':
                father = path_home + '/' + father + '/'
            status = self.status()
            for f in status['keys']:
                self.Initializer.storage.copy_file_global(f,father+f.replace(dataset,''))

        self.init(father)
        self.connect_post_api()
        self.set_schema()
        return True
    
    def branch(self, branch_name='new_branch123/', branch_type='copy', branch_title=''):
        if not self.in_version:
            if branch_name[-1] != '/':
                branch_name += '/'
            if self.config['schema'] != 'files':
                trial = self.schema_class.branch(branch_name=branch_name,type_=branch_type)
                if trial:
                    if self.config['type'] == 's3':
                        uri = 's3://'+self.Initializer.storage.BUCKET_NAME+'/'
                    elif self.config['type'] == 'gcs':
                        uri = 'gs://'+self.Initializer.storage.BUCKET_NAME+'/'
                    else:
                        uri = ''

                    if uri in branch_name:
                        branch_name = branch_name.replace(uri,'')

                    schme = self.config['schema']

                    if branch_name[-1] != '/':
                        branch_name += '/'

                    self.init(uri+branch_name)

                    if branch_title == '':
                        branch_title = branch_name

                    self.reconnect_post_web(branch_title, schme)
                    self.set_schema()
                    self.start_check()
                    self.commit('created branch ' + branch_name)
                    return True
                else:
                    return False
            else:
                type_ = branch_type
                dataset = self.Initializer.storage.dataset
                if self.Initializer.storage.type == 'local':
                    branch_name = path_home + '/' + branch_name + '/'

                status = self.status()
                versions = []
                if len(versions) == 0:
                    for f in status['keys']:
                        versions.append('current')
                idx = 0
                if type_ == 'copy':
                    for f in status['keys']:
                        if versions[idx] == 'current':
                            self.Initializer.storage.copy_file_global(f,branch_name+f.replace(dataset,''))
                        else:
                            self.Initializer.storage.copy_file_global(self.Initializer.prefix_diffs+f+'/'+str(int(versions[idx])).zfill(10),branch_name+f)
                        idx += 1
                else:
                    for f in self.status['keys']:
                        if versions[idx] == 'current':
                            self.Initializer.storage.remove_file_global(dataset+f)
                        else:
                            self.Initializer.storage.copy_file_global(self.Initializer.prefix_diffs+f+'/'+str(int(versions[idx])).zfill(10),branch_name+f)
                        idx += 1

                if self.config['type'] == 's3':
                    uri = 's3://'+self.Initializer.storage.BUCKET_NAME+'/'
                elif self.config['type'] == 'gcs':
                    uri = 'gs://'+self.Initializer.storage.BUCKET_NAME+'/'
                else:
                    uri = ''

                schme = self.config['schema']

                if branch_name[-1] != '/':
                    branch_name += '/'

                self.init(uri+branch_name)

                self.reconnect_post_web(branch_name, schme)
                self.set_schema()
                self.start_check()
                self.commit('created branch ' + branch_name)
        else:
            dataset = self.Initializer.storage.dataset
            if self.Initializer.storage.type == 'local':
                branch_name = path_home + '/' + branch_name + '/'

            status = self.status()
            idx = 0
            for f in status['keys']:
                f_base = os.path.dirname(f.replace(self.Initializer.prefix_diffs,''))
                self.Initializer.storage.copy_file_global(f,branch_name+f_base.replace(dataset,''))
                idx += 1

            if self.config['type'] == 's3':
                uri = 's3://'+self.Initializer.storage.BUCKET_NAME+'/'
            elif self.config['type'] == 'gcs':
                uri = 'gs://'+self.Initializer.storage.BUCKET_NAME+'/'
            else:
                uri = ''

            schme = self.config['schema']

            if branch_name[-1] != '/':
                branch_name += '/'

            self.init(uri+branch_name)

            self.reconnect_post_web(branch_name, schme)
            self.set_schema()
            self.start_check()
            self.commit('created branch ' + branch_name)

            return True

    def schema_metadata(self):
        if self.config['schema'] != 'files':
            return self.schema_class.get_metadata()
        else:
            return {}

    def apply_filter(self):
        if self.config['schema'] != 'files':
            metapath = self.schema_class.meta_path
            return json.load(self.Initializer.storage.load_file_global(metapath))
        else:
            return {}

    def connect_post_web(self, name='My Dataset', keys={}, schema='files'):
        
        print('connecting to '+name)
        self.config['schema'] = schema
        config = self.config

        self.storage_name = config['storage']
        self.dataset_name = config['dataset']
        
        try: 
            datasets = self.get_datasets()
        except:
            datasets = {}

        if config['type'] == 'local':
            cloud = Local()
            cloud.create_dataset(config['dataset'],verbose=True)
            self.Initializer = Initializer(cloud)
        elif config['type'] == 's3':
            cloud = S3Bucket(config['bucket'])
            cloud.connect_bucket_api(keys)
            cloud.create_dataset(config['dataset'])
            self.Initializer = Initializer(cloud)
        elif config['type'] == 'gcs':
            cloud = GCSBucket(config['bucket'])
            cloud.connect_bucket_api(self.key_bin)
            cloud.create_dataset(config['dataset'])
            self.Initializer = Initializer(cloud)
        else:
            self.Initializer = None

        datasets[self.storage_name] = {'storage': self.storage_name, 'name': name, 'type': config['type'], 'schema': config['schema']}
        self.set_datasets(datasets)
        
        self.set_config()
        return True

    def reconnect_post_web(self, name='My Dataset', schema='files'):
        
        print('connecting to '+name)
        self.config['schema'] = schema
        config = self.config

        self.storage_name = config['storage']
        self.dataset_name = config['dataset']
        
        try: 
            datasets = self.get_datasets()
        except:
            datasets = {}

        if config['type'] == 'local':
            cloud = Local()
            cloud.create_dataset(config['dataset'],verbose=True)
            self.Initializer = Initializer(cloud)
        elif config['type'] == 's3':
            cloud = S3Bucket(config['bucket'])
            cloud.reconnect_bucket_api()
            cloud.create_dataset(config['dataset'])
            self.Initializer = Initializer(cloud)
        elif config['type'] == 'gcs':
            cloud = GCSBucket(config['bucket'])
            cloud.reconnect_bucket_api()
            cloud.create_dataset(config['dataset'])
            self.Initializer = Initializer(cloud)
        else:
            self.Initializer = None

        is_not_there = True
        for s in datasets.keys():
            if datasets[s]['storage'] == self.storage_name:
                is_not_there = False

        datasets[self.storage_name] = {'storage': self.storage_name, 'name': name, 'type': config['type'], 'schema': config['schema']}
        self.set_datasets(datasets)
        self.set_config()
        return True

    def connect_post_cli(self):
        config = self.config

        self.storage_name = config['storage']
        self.dataset_name = config['dataset']

        datasets = self.get_datasets()
        try: 
            datasets = self.get_datasets()
        except:
            datasets = {}
        
        try:
            self.config['schema'] = datasets[self.storage_name]['schema']
        except:
            self.config['schema'] = 'files'

        if config['type'] == 'local':
            cloud = Local()
            cloud.create_dataset(config['dataset'], verbose=True)
            self.Initializer = Initializer(cloud)
        elif config['type'] == 's3':
            cloud = S3Bucket(config['bucket'])
            cloud.connect_bucket(verbose=True)
            cloud.create_dataset(config['dataset'])
            self.Initializer = Initializer(cloud)
        elif config['type'] == 'gcs':
            cloud = GCSBucket(config['bucket'])
            cloud.connect_bucket(verbose=True)
            cloud.create_dataset(config['dataset'])
            self.Initializer = Initializer(cloud)
        else:
            self.Initializer = None

        self.set_config()
        return True

    def connect_post_api(self, cli=False):
        config = self.config

        self.storage_name = config['storage']
        self.dataset_name = config['dataset']

        datasets = self.get_datasets()
        try:
            self.config['schema'] = datasets[self.storage_name]['schema']
        except:
            self.config['schema'] = 'files'

        if config['type'] == 'local':
            cloud = Local()
            cloud.create_dataset(config['dataset'], verbose=True)
            self.Initializer = Initializer(cloud)
            self.dataset_name = config['dataset']
            self.storage_name = config['storage']
        elif config['type'] == 's3':
            cloud = S3Bucket(config['bucket'])
            if cli:
                cloud.connect_bucket()
            else:
                cloud.reconnect_bucket_api()
            cloud.create_dataset(config['dataset'])
            self.Initializer = Initializer(cloud)
        elif config['type'] == 'gcs':
            cloud = GCSBucket(config['bucket'])
            if cli:
                cloud.connect_bucket(verbose=True)
            else:
                cloud.reconnect_bucket_api()
            cloud.create_dataset(config['dataset'])
            self.Initializer = Initializer(cloud)
        else:
            self.Initializer = None
        self.set_config()
        return True

    def disconnect_dataset(self, storage=''):
        assert(len(storage) > 0)
        datasets = self.get_datasets()
        print('disconnecting from ' + storage)

        for s in datasets.keys():
            if datasets[s]['storage'] == storage:
                del datasets[s]
                self.set_datasets(datasets)
                return True
        print('run \'stack datasets\' to see other available datasets')
        return False

    def upload_file_binary(self, filename='', binary=''):
        assert(filename != '')
        assert(binary != '')
        add_from_binary(self.Initializer, filename, binary)
        return True

    def upload_file_local_path(self, filename='', target_path=''):
        assert(filename != '')
        add(self.Initializer,[filename],target_path)
        return True

    def start_check(self):
        try:
            return self.Initializer.start_check()
        except:
            return False

    def get_uri(self):
        datasets = self.get_datasets()
        dataset_meta = datasets[self.storage_name]
        return {'storage': self.storage_name, 'schema': dataset_meta['schema'], 'dataset': dataset_meta['name'], 'storage_dataset': self.Initializer.storage.dataset}

    def add(self, path, subpath=''):
        self.reset_version()
        if len(subpath)>1:
            if subpath[-1] != '/':
                subpath = subpath + '/'
        add(self.Initializer,[path],subpath)
        return True

    def pull_all(self,version = 'current'):
        print('downloading files from last commit')
        metapath = self.Initializer.prefix_meta + 'current.json'
        current = json.load(self.Initializer.storage.load_file_global(metapath))
        pull(self.Initializer, current['keys'], version)
        return True

    def pull(self, file, version = 'current'):
        if file == '.' or file == 'all':
            print('downloading all contents')
            return self.pull_all(version)
        else:
            pull(self.Initializer, [file], version)
        return True

    def pull_file(self, file, version = 'current'):
         
        if version == 'current':
            # saves each file
            for key in file:
                binary = self.Initializer.storage.load_file_global(key)
        else:
            gtfo = False
            # finds the commit of interest
            metapath = self.Initializer.prefix_meta+'history.json'
            history = self.Initializer.load(self.Initializer.storage.load_file_global(metapath))
            for key in file:
                if key[-1] == '/':
                    print('Do not pull directories')
                    return False
                for i in range(len(history),int(version)-1,-1):
                    for commit in history[str(i)]['commits']:
                        # reads each file version
                        if self.Initializer.storage.type == 'local':
                            cmit = json.load(self.Initializer.storage.load_file_global(commit))
                        else:
                            cmit = json.load(self.Initializer.storage.load_file_global(commit))
                        if str(cmit['version']) == version and cmit['key'] == key:
                            if cmit['type'] != 'remove':
                                key = self.Initializer.prefix_diffs + key + '/' + str(cmit['version']).zfill(10)
                                binary = self.Initializer.storage.load_file_global(key).read()
                                self.Initializer.storage.reset_buffer()
                            gtfo = True
                        if gtfo:
                            break
                if gtfo:
                    gtfo = False
                    break

        self.Initializer.storage.reset_buffer()
        file_array = {binary: binary, key: file}

        return file_array

    def commits_version(self, version = 2, l = 5, page = 0):
        assert(int(l) > 0)
        assert(int(version) >= 0)
        assert(int(page) >= 0)

        metapath = self.Initializer.prefix_meta+'history.json'
        history = json.load(self.Initializer.storage.load_file_global(metapath))
        self.Initializer.storage.reset_buffer()

        response = {}
        idx = 0

        i_p = int(page)*int(l)
        i_f = min((int(page)+1)*int(l),len(history[str(int(version))]['commits']))

        # goes over the commits
        for i in range(i_p, i_f):
            # reads each file version
            commit = history[str(int(version))]['commits'][i]
            if self.Initializer.storage.type == 'local':
                cmit = json.load(self.Initializer.storage.load_file_global(commit))
            else:
                cmit = json.load(self.Initializer.storage.load_file_global(commit))
            self.Initializer.storage.reset_buffer()
            response[idx] = {'key': cmit['key'], 'source': cmit['source'], 'date': history[str(int(version))]['date'], 'comment': cmit['comment']}
            idx = idx + 1

        return {'commits': response, 'len': len(history[str(int(version))]['commits'])}

    def key_versions(self, key = '', l = 5, page = 0):
        if self.config['schema'] == 'spacy_ner':
            schema = self.schema_class.get_schema()
            key = schema[key]['filename']
        
        if self.Initializer.prefix_diffs in key:
            key = os.path.dirname(key.replace(self.Initializer.prefix_diffs,''))

        assert(int(l) > 0)
        assert(int(page) >= 0)

        key_hist = get_key_history(self.Initializer, key)
        response = {}
        i_p = len(key_hist) - int(page)*int(l)
        i_f = max(len(key_hist) - int(l)*(int(page)+1),0)

        idx = 0

        # goes over the commits
        for i in range(i_p, i_f, -1):
            # reads each file version
            response[idx] = key_hist[str(i)]
            response[idx]['file'] = 'raw'
            idx = idx+1

        return {'commits': response, 'len': len(key_hist)}

    def get_tags(self, key):
        if self.config['schema'] != 'files':
            return self.schema_class.get_tags(key)
        else:
            return []

    def add_tag(self, key, tag):
        if self.config['schema'] != 'files':
            self.schema_class.add_tag(key, tag)
        return True
    
    def add_slice(self, slice_name):
        if self.config['schema'] != 'files':
            self.schema_class.add_slice(slice_name)
        return True

    def remove_slice(self, slice_name):
        if self.config['schema'] != 'files':
            self.schema_class.remove_slice(slice_name)
            self.schema_class.apply_filters()
        return True

    def reset_slices(self):
        if self.config['schema'] != 'files':
            self.schema_class.sliced = False
            self.schema_class.selected_slices = []
            self.schema_class.apply_filters()
        return True
    
    def get_slices(self):
        if self.config['schema'] != 'files':
            return self.schema_class.get_slices()
        return {}
    
    def select_slices(self, slices = []):
        self.reset_version()
        if self.config['schema'] != 'files':
            self.schema_class.select_slice(slices=slices)
        return True

    def selection_add_tag(self, keys, tag):
        if self.config['schema'] != 'files':
            self.schema_class.add_many_tag(keys, tag)
        return True

    def get_readme(self):
        ls, _ = self.Initializer.storage.load_dataset_list()
        matches = [match for match in ls if 'readme' in match.lower()]
        if len(matches) == 0:
            return ''
        else:
            return self.Initializer.storage.load_file_global(matches[0]).read()
    
    def selection_remove_all_tags(self, keys):
        if self.config['schema'] != 'files':
            self.schema_class.many_remove_all_tags(keys)
        return True

    def remove_tag(self, key, tag):
        if self.config['schema'] != 'files':
            self.schema_class.remove_tag(key, tag)
        return True

    def remove_all_tags(self, key):
        if self.config['schema'] != 'files':
            self.schema_class.remove_all_tags(key)
        return True

    def label_versions(self, key, l = 5, page = 0):
        if self.config['schema'] == 'yolo':
            if not self.in_version and self.Initializer.prefix_diffs in key:
                key = os.path.dirname(key.replace(self.Initializer.prefix_diffs,''))
            labels_key = self.schema_class.get_labels_filename(key)
            if self.Initializer.prefix_diffs in labels_key:
                labels_key = os.path.dirname(labels_key.replace(self.Initializer.prefix_diffs,''))
            labels_hist = get_key_history(self.Initializer, labels_key)
            response = {}
            idx = 0
            i_p = len(labels_hist) - int(page)*int(int(l))
            i_f = max(len(labels_hist) - int(int(l))*(int(page)+1),0)

            # goes over the commits
            for i in range(i_p, i_f, -1):
                # reads each file version
                response[idx] = labels_hist[str(i)]
                response[idx]['file'] = 'label'
                idx = idx+1

        elif self.config['schema'] == 'labelbox':
            labels_key = self.schema_class.get_labels_filename()
            if self.in_version:
                labels_key = os.path.dirname(labels_key.replace(self.Initializer.prefix_diffs,''))
            labels_hist = get_key_history(self.Initializer, labels_key)
            response = {}
            idx = 0
            i_p = len(labels_hist) - int(page)*int(int(l))
            i_f = max(len(labels_hist) - int(int(l))*(int(page)+1),0)

            # goes over the commits
            for i in range(i_p, i_f, -1):
                # reads each file version
                response[idx] = labels_hist[str(i)]
                response[idx]['file'] = 'label'
                idx = idx+1
        elif self.config['schema'] == 'spacy_ner':
            schema = self.schema_class.get_schema()
            labels_key = schema[key]['filename']
            labels_hist = get_key_history(self.Initializer, labels_key)
            response = {}
            idx = 0
            i_p = len(labels_hist) - int(page)*int(int(l))
            i_f = max(len(labels_hist) - int(int(l))*(int(page)+1),0)

            # goes over the commits
            for i in range(i_p, i_f, -1):
                # reads each file version
                response[idx] = labels_hist[str(i)]
                response[idx]['file'] = 'label'
                idx = idx+1
        else:
            labels_key = key
            if self.in_version:
                labels_key = os.path.dirname(labels_key.replace(self.Initializer.prefix_diffs,''))
            labels_hist = get_key_history(self.Initializer, key)
            response = {}
            i_p = len(labels_hist) - int(page)*int(l)
            i_f = max(len(labels_hist) - int(l)*(int(page)+1),0)

            idx = 0

            # goes over the commits
            for i in range(i_p, i_f, -1):
                # reads each file version
                response[idx] = labels_hist[str(i)]
                response[idx]['file'] = 'raw'
                idx = idx+1
        return {'commits': response, 'len': len(labels_hist), 'keyId': labels_key}

    def status(self):
        if self.config['schema'] != 'files':
            if self.schema_class.filtered:
                return self.schema_class.status
            else:
                return self.schema_class.read_all_files()
        else:
            if self.filtered:
                if self.in_version:
                    if self.current != None:
                        schema = self.current
                    else:
                        schema = self.Initializer.load_current_version(self.selected_version)
                        self.current = schema
                else:
                    if self.current != None:
                        schema = self.current
                    else:
                        schema = self.Initializer.load_current()
                        self.current = schema
                status = {'keys': [], 'lm': []}

                idx = 0
                for key in schema['keys']:
                    for f in self.filters:
                        for filt in self.filters[f]:
                            if filt == 'name':
                                if self.filters[f]['name'] in key:
                                    status['keys'].append(schema['keys'][idx])
                                    status['lm'].append(schema['lm'][idx])
                    idx += 1
                return status
            else:
                return self.Initializer.load_current()

    def get_next_key(self,key):
        status = self.status()
        idx = status['keys'].index(key)
        if idx == len(status['keys']) - 1:
            return status['keys'][0]
        else:
            return status['keys'][idx + 1]

    def get_prev_key(self,key):
        status = self.status()
        idx = status['keys'].index(key)
        if idx == 0:
            return status['keys'][-1]
        else:
            return status['keys'][idx - 1]

    def remove(self, key, subpath=''):
        if len(subpath)>1:
            if subpath[-1] != '/':
                subpath = subpath + '/'
        key = key.replace(self.Initializer.storage.dataset,'')
        remove(self.Initializer,[key],subpath)
        return True

    def remove_commit(self, version = '-1'):
        self.reset_version()
        assert(int(version) >= 0)
        
        metapath = self.Initializer.prefix_meta+'history.json'
        history = json.load(self.Initializer.storage.load_file_global(metapath))
        self.Initializer.storage.reset_buffer()

        for i in range(len(history[str(int(version))]['commits'])):
            commit = history[str(int(version))]['commits'][i]
            if self.Initializer.storage.type == 'local':
                cmit = json.load(self.Initializer.storage.load_file_global(commit))
            else:
                cmit = json.load(self.Initializer.storage.load_file_global(commit))
            self.Initializer.storage.reset_buffer()
            remove_global(self.Initializer, [cmit['diff']])

        return True

    def remove_key_diff(self, key, version):
        if not self.in_version:
            remove_diff(self.Initializer,key,int(version))
        return True

    
    def set_filters(self, filters):
        if self.config['schema'] != 'files':
            self.schema_class.apply_filters(filters)
        else:
            self.filters = filters
            self.filtered = True
        return True

    def reset_filters(self):
        if self.config['schema'] != 'files':
            self.schema_class.filtered = False
            self.schema_class.apply_filters()
        else:
            self.filtered = False
        return True

    def get_versions(self):
        versions = self.Initializer.load_versions()
        if self.in_version:
            versions['current_v'] = self.selected_version
        else:
            versions['current_v'] = -1
        return versions

    def add_version(self, label = ''):
        add_version(self.Initializer, label)
        if self.config['schema'] != 'files':
            self.schema_class.copy_schema_to_latest_version_checkpoint()
        return True

    def select_version(self, version):
        versions = self.Initializer.load_versions()
        if version in versions.keys():
            self.in_version = True
            self.selected_version = version
            self.version_keys = json.load(self.Initializer.storage.load_file_global(versions[version]['path']))
            if self.config['schema'] != 'files':
                self.schema_class.schema = None
                self.schema_class.select_version(version, versions[version]['schema_path'], self.version_keys)
                self.schema_class.get_schema()
                self.schema_class.read_all_files()
            self.current = self.Initializer.load_current_version(version)
            return True
        else:
            self.reset_version()
            return False

    def reset_version(self):
        if self.in_version:
            self.in_version = False
            self.version_keys = None
            self.current = self.Initializer.load_current()
            self.filtered = False
            if self.config['schema'] != 'files':
                self.schema_class.schema = None
                self.schema_class.in_version = False
                self.schema_class.version_keys = None
                self.schema_class.reset_to_current_version()
                self.schema_class.read_all_files()
                self.filtered = False
        return True

    def get_hierarchy(self):
        metapath = self.Initializer.prefix_meta + 'hierarchy.json'
        try:
            return json.load(self.Initializer.storage.load_file_global(metapath))
        except:
            self.Initializer.storage.add_file_from_binary_global(metapath,io.BytesIO(json.dumps({'parent': '', 'children': []}).encode('ascii')))
            return {'parent': '', 'children': []}

    def set_current_hierarchy(self, hierarchy):
        metapath = self.Initializer.prefix_meta + 'hierarchy.json'
        self.Initializer.storage.add_file_from_binary_global(metapath,io.BytesIO(json.dumps(hierarchy).encode('ascii')))
        self.Initializer.storage.reset_buffer()
        return True
    
    def set_hierarchy(self, parent='', child=''):
        metapath = self.Initializer.prefix_meta + 'hierarchy.json'
        try:
            hierarchy = json.load(self.Initializer.storage.load_file_global(metapath))
        except:
            hierarchy = {'parent': '', 'children': []}

        if self.config['type'] == 's3':
            uri = 's3://'+self.Initializer.storage.BUCKET_NAME+'/'
        elif self.config['type'] == 'gcs':
            uri = 'gs://'+self.Initializer.storage.BUCKET_NAME+'/'
        else:
            uri = ''

        parent = parent.replace(path_home,'')
        child = child.replace(path_home,'')

        if parent != '':
            if not uri in parent:
                parent = uri + parent
            hierarchy['parent'] = parent
        if child != '':
            if not uri in child:
                child = uri + child
            hierarchy['children'].append(child)
        self.Initializer.storage.add_file_from_binary_global(metapath,io.BytesIO(json.dumps(hierarchy).encode('ascii')))
        self.Initializer.storage.reset_buffer()
        return True

    def remove_child(self, child=''):
        metapath = self.Initializer.prefix_meta + 'hierarchy.json'
        try:
            hierarchy = json.load(self.Initializer.storage.load_file_global(metapath))
        except:
            hierarchy = {'parent': '', 'children': []}
        if self.config['type'] == 's3':
            uri = 's3://'+self.Initializer.storage.BUCKET_NAME+'/'
        elif self.config['type'] == 'gcs':
            uri = 'gs://'+self.Initializer.storage.BUCKET_NAME+'/'
        else:
            uri = ''

        if child != '':
            if not uri in child:
                child = uri + child
            if child in hierarchy['children']:
                hierarchy['children'].remove(child)
        self.Initializer.storage.add_file_from_binary_global(metapath,io.BytesIO(json.dumps(hierarchy).encode('ascii')))
        self.Initializer.storage.reset_buffer()
        return True

    def remove_parent(self):
        metapath = self.Initializer.prefix_meta + 'hierarchy.json'
        try:
            hierarchy = json.load(self.Initializer.storage.load_file_global(metapath))
        except:
            hierarchy = {'parent': '', 'children': []}
        hierarchy['parent'] = ''
        self.Initializer.storage.add_file_from_binary_global(metapath,io.BytesIO(json.dumps(hierarchy).encode('ascii')))
        self.Initializer.storage.reset_buffer()
        return True

    def add_child_to_current(self, child=''):
        metapath = self.Initializer.prefix_meta + 'hierarchy.json'
        try:
            hierarchy = json.load(self.Initializer.storage.load_file_global(metapath))
            self.Initializer.storage.reset_buffer()
        except:
            hierarchy = {'parent': '', 'children': []}
        if self.config['type'] == 's3':
            uri = 's3://'+self.Initializer.storage.BUCKET_NAME+'/'
        elif self.config['type'] == 'gcs':
            uri = 'gs://'+self.Initializer.storage.BUCKET_NAME+'/'
        else:
            uri = ''

        if child != '':
            if not uri in child:
                child = uri + child
            if not child in hierarchy['children']:
                hierarchy['children'].append(child)
        self.Initializer.storage.add_file_from_binary_global(metapath,io.BytesIO(json.dumps(hierarchy).encode('ascii')))
        self.Initializer.storage.reset_buffer()
        return True

    def add_parent_to_current(self, parent=''):
        metapath = self.Initializer.prefix_meta + 'hierarchy.json'
        try:
            hierarchy = json.load(self.Initializer.storage.load_file_global(metapath))
        except:
            hierarchy = {'parent': '', 'children': []}
        if self.config['type'] == 's3':
            uri = 's3://'+self.Initializer.storage.BUCKET_NAME+'/'
        elif self.config['type'] == 'gcs':
            uri = 'gs://'+self.Initializer.storage.BUCKET_NAME+'/'
        else:
            uri = ''

        if parent != '':
            if not uri in parent:
                parent = uri + parent
            hierarchy['parent'] = parent
        self.Initializer.storage.add_file_from_binary_global(metapath,io.BytesIO(json.dumps(hierarchy).encode('ascii')))
        self.Initializer.storage.reset_buffer()
        return True

    def remove_key_full(self, key):
        remove_full(self.Initializer,key)
        return True

    def commit(self, comment='',verbose=True):
        self.reset_version()
        res, added, modified, deleted = commit(self.Initializer, comment)
        if verbose:
            if res: 
                print('sync done!')
            else:
                print('already up-to-date')
        if self.config['schema'] != 'files':
            self.schema_class.update_schema_file(added, modified, deleted)

        return True

    def load_commit_metadata(self, commit_file):
        try:
            return json.load(self.Initializer.storage.load_file_global(commit_file))
        except:
            return {}

    def check_if_setup(self):
        self.Initializer.setup_dataset()
        print('setup complete!')
        return True

    def load_file_binary(self, file, version='current'):
        if version=='current':
            print('loading ' + file + '...')
            return self.Initializer.storage.load_file_global(file)
        elif int(version) >= 1:
            print('loading ' + file + ' version '+ version +'...')
            path = self.Initializer.prefix_diffs + file + '/' + str(int(version)).zfill(10)
            return self.Initializer.storage.load_file_global(path)
        else:
            assert(False)

    def load_csv_binary(self, file, row_p, col_p, version='current'):
        assert(int(row_p)>=0)
        assert(int(col_p)>=0)

        N_rows = 10
        N_cols = 10

        if version=='current':
            print('loading ' + file + '...')
            data  = self.Initializer.storage.load_file_global(file)
        elif int(version) >= 1:
            print('loading ' + file + ' version '+ version +'...')
            data  =  self.Initializer.storage.load_file_global(self.Initializer.prefix_diffs+ file + '/' + str(int(version)).zfill(10))
        else:
            assert(False)

        df1 = pd.read_csv(data, encoding='unicode_escape', encoding_errors='backslashreplace', lineterminator='\n')

        total_cols = len(df1.axes[1])
        bot_col = min(N_cols*int(col_p),total_cols)
        top_col = min(N_cols*(int(col_p)+1),total_cols)
        
        total_rows = len(df1.axes[0])
        bot_row = min(N_rows*int(row_p), total_rows)
        top_row = min(N_rows*(int(row_p)+1),total_rows)
        
        df1 = df1.iloc[bot_row:top_row,bot_col:top_col]
        return StringIO(df1.to_csv(index=False, header=False)), total_rows/N_rows, total_cols/N_cols

    def load_csv_diff_metadata(self, file, v1, v2):
        if v1 == 'current':
            d1 = self.Initializer.storage.load_file_global(file)
        else:
            d1 = self.Initializer.storage.load_file_global(self.Initializer.prefix_diffs + file + '/' + str(int(v1)).zfill(10))

        if v2 == 'current':
            d2 = self.Initializer.storage.load_file_global(file)
        else:
            d2 = self.Initializer.storage.load_file_global(self.Initializer.prefix_diffs + file + '/' + str(int(v2)).zfill(10))

        from csv_diff import load_csv, compare
        import codecs

        csv1 = load_csv(codecs.getreader("utf-8")(d1))
        csv2 = load_csv(codecs.getreader("utf-8")(d2))
        diff = compare(csv2, csv1)

        # converts diff to json
        diff_meta = {}
        diff_meta['additions'] = len(diff['added'])
        diff_meta['deletions'] = len(diff['removed'])
        diff_meta['modifications'] = len(diff['changed'])
        diff_meta['new_cols'] = len(diff['columns_added'])

        return diff_meta

    def load_csv_diff(self, file, v1, v2):
        if v1 == 'current':
            d1 = self.Initializer.storage.load_file_global(file)
        else:
            d1 = self.Initializer.storage.load_file_global(self.Initializer.prefix_diffs + file + '/' + str(int(v1)).zfill(10))

        if v2 == 'current':
            d2 = self.Initializer.storage.load_file_global(file)
        else:
            d2 = self.Initializer.storage.load_file_global(self.Initializer.prefix_diffs + file + '/' + str(int(v2)).zfill(10))

        from csv_diff import load_csv, compare
        import codecs

        csv1 = load_csv(codecs.getreader("utf-8")(d1))
        csv2 = load_csv(codecs.getreader("utf-8")(d2))
        diff = compare(csv2, csv1)

        return diff

    def load_file_binary_bytes(self, file, bi, bf):
        print('loading '+file+'...')
        return self.Initializer.storage.load_file_bytes(file,bi,bf)

    def reset(self):
        self.Initializer.remove_setup()
        self.Initializer.setup_dataset()
        print('setup complete!')
        return True

    def revert(self, version=0):
        self.reset_version()
        assert(version != '')
        revert_commit(self.Initializer, int(version))
        self.commit('reverted to version ' + str(version))

    def revert_file(self, key, version):
        try: 
            revert_file(self.Initializer, key, int(version))
            self.Initializer.storage.reset_buffer()
            return True
        except:
            return False

    def history(self):
        metapath = self.Initializer.prefix_meta+'history.json'
        return json.load(self.Initializer.storage.load_file_global(metapath))

    def last_n_commits(self, n = 0):
        metapath = self.Initializer.prefix_meta+'history.json'
        history = json.load(self.Initializer.storage.load_file_global(metapath))
        self.Initializer.storage.reset_buffer()

        response = {}
        idx = 0

        # goes over history
        for i in range(len(history),0,-1):
            for commit in history[str(i)]['commits']:
                # reads each file version
                if self.Initializer.storage.type == 'local':
                    cmit = json.load(self.Initializer.storage.load_file_global(commit))
                else:
                    cmit = json.load(self.Initializer.storage.load_file_global(commit))

                response[idx] = {'source': cmit['source'], 'date': history[str(i)]['date'], 'comment': cmit['comment']}

                idx += 1
                if int(idx) >= int(n):
                    break
            if int(idx) >= int(n):
                    break

        return response

    def diff(self, v1, v0, file=''):
        print_diff(self.Initializer, v1, v0, file)
        return True

    def server_init_experiment(self, uri, project):
        if path_home in uri:
            uri = uri.replace(path_home,'')
        if uri != self.config['storage']:
            self.init(uri)
            self.connect_post_api()
            self.set_schema()

        self.projects = projects(init=self.Initializer, project=project)
        self.projects.init_project()

    def server_add_log(self, data):
        assert(not self.projects is None)
        self.projects.add_log(data)

    def server_add_prediction(self, data):
        assert(not self.projects is None)
        self.projects.add_prediction(data)

    def server_get_models(self):
        assert(not self.projects is None)
        return self.projects.models()

    def server_upload_model(self, model, label):
        assert(not self.projects is None)
        return self.projects.add_model(model, label)

    def server_get_model(self, label):
        assert(not self.projects is None)
        return self.projects.get_model(label)

    def server_logout_experiment(self):
        assert(not self.projects is None)
        self.projects = None

    def get_projects(self):
        prefix_projects = self.Initializer.prefix_meta + 'projects/' + 'projects.json'
        return json.load(self.Initializer.storage.load_file_global(prefix_projects))         

    def get_logs(self, project):
        prefix_projects = self.Initializer.prefix_meta + 'projects/' + project + '/experiments/experiments.json'
        return json.load(self.Initializer.storage.load_file_global(prefix_projects))         
    
    def get_logs_experiment(self, log):
        return json.load(self.Initializer.storage.load_file_global(log))         
    
    def get_predictions_list(self, project):
        prefix_projects = self.Initializer.prefix_meta + 'projects/'
        path = prefix_projects + project + '/predictions.json'
        return json.load(self.Initializer.storage.load_file_global(path))

    def get_prediction(self, prediction):
        return json.load(self.Initializer.storage.load_file_global(prediction))         
        
        
