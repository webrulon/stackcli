import sys
sys.path.append( '../../' )
import socket
import string
import json
import io
from datetime import datetime
import time
from pathlib import Path
import os
from src.comm.docker_ver import *
path_home = os.getenv('LCP_DKR')+'/' if docker_ver() else str(Path.home())

class projects:
    def __init__(self, project, init):
        self.init = init
        self.project = project
        self.projects = {}
        self.predictions = {}
        self.prefix_projects = init.prefix_meta + 'projects/' 
        self.prefix_experiments = self.prefix_projects + project + '/experiments/' 
        self.prefix_models = self.prefix_projects + project + '/models/'
        self.prefix_predictions = self.prefix_projects + project + '/predictions/'

        self.experiments = None
        self.models = None
        self.in_log = None
        self.logs = None
        self.current_run: int = None
        
    def verify_setup(self):
        if self.init.storage.check_if_empty(self.prefix_projects):
            return False
        if self.init.storage.check_if_empty(self.prefix_experiments):
            return False
        return True

    def init_project(self):
        if not self.verify_setup():
            self.setup_experiments()
            self.setup_models()
        self.experiments = json.load(self.init.storage.load_file_global(self.prefix_experiments + 'experiments.json'))
        self.models = json.load(self.init.storage.load_file_global(self.prefix_models + 'models.json'))
        try: 
            self.projects = json.load(self.init.storage.load_file_global(self.prefix_projects + 'projects.json'))
        except:
            self.projects = {}
        if not self.project in self.projects.keys():
            self.projects[self.project] = {'date created': datetime.now().strftime("%m/%d/%Y, %H:%M:%S"), 'runs': [], 'models': []}
            self.init.storage.add_file_from_binary_global(self.prefix_projects + 'projects.json',io.BytesIO(json.dumps(self.projects).encode('ascii')))
        self.logs = None
        return True

    def setup_experiments(self):
        # creates json for experiments in project
        # List<dict>:
        #   dates: strings (dates of each run)
        #   logs: strings (URIS of the training logs of each run)
        #   models: strings (URIS of the models of each run)
        #   dataset_versions: strings (URIS of the dataset versions file for each run)
        experiments = []
        self.experiments = experiments
        json_path = self.prefix_experiments + 'experiments.json'

        # stores json in .stack folder
        self.init.storage.add_file_from_binary_global(json_path,io.BytesIO(json.dumps(experiments).encode('ascii')))
        self.init.storage.reset_buffer()
        return True

    def setup_models(self):
        # creates json for models in project
        models = {'date': [], 'uri': []}
        self.models = models
        json_path = self.prefix_models + 'models.json'
        
        self.init.storage.add_file_from_binary_global(json_path,io.BytesIO(json.dumps(models).encode('ascii')))
        self.init.storage.reset_buffer()
        return True

    def init_run(self):
        # checks run number
        if len(self.experiments) == 0:
            self.current_run = 0
            print('NNNNNN')
            print(self.current_run)
        else:
            self.current_run = self.get_latest_run_number()
            print('ADDDDD')
            print(self.current_run)
        
        self.logs = []

        # setups up empty run
        run = {'date': datetime.now().strftime("%m/%d/%Y, %H:%M:%S"), 'logs': self.prefix_experiments + 'logs/' + str(self.current_run).zfill(10), 'models': [], 'dataset_version': ''}
        self.experiments.append(run)
        self.projects[self.project]['runs'].append(run)
        self.init.storage.add_file_from_binary_global(self.prefix_projects + 'projects.json',io.BytesIO(json.dumps(self.projects).encode('ascii')))
        return True

    def add_prediction(self, data, label = '', version = None, model = None):
        if label is None:
            label = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        pred_path = self.prefix_predictions+'/predictions/'+str(self.get_latest_prediction_number() + 1).zfill(10)
        self.predictions[datetime.now().strftime("%m/%d/%Y, %H:%M:%S")] = {'label': label, 'version': version, 'model': model, 'path': pred_path}
        
        self.init.storage.add_file_from_binary_global(self.prefix_predictions + 'predictions.json',io.BytesIO(json.dumps(self.predictions).encode('ascii')))
        self.init.storage.reset_buffer()
        
        self.init.storage.add_file_from_binary_global(pred_path,io.BytesIO(json.dumps(data).encode('ascii')))
        self.init.storage.reset_buffer()

    def add_log(self, data):
        if self.logs is None:
            self.logs = []
            self.init_run()

        self.logs.append(data)
        self.init.storage.add_file_from_binary_global(self.experiments[-1]['logs'],io.BytesIO(json.dumps(self.logs).encode('ascii')))
        self.init.storage.reset_buffer()
        
        json_path = self.prefix_experiments + 'experiments.json'
        self.init.storage.add_file_from_binary_global(json_path,io.BytesIO(json.dumps(self.experiments).encode('ascii')))
        self.init.storage.reset_buffer()

    def add_model(self, model, label = None):
        if label is None:
            label = 'model'
        if self.current_run is None:
            self.current_run = self.get_latest_run_number()

        model_path = self.prefix_models + '/' + str(self.current_run).zfill(10) + '/' + label
        
        self.experiments[-1]['models'].append(model_path)
        self.models['uri'].append(model_path)
        self.models['date'].append(datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))

        self.projects[self.project]['models'].append(model_path)
        self.init.storage.add_file_from_binary_global(self.prefix_projects + 'projects.json',io.BytesIO(json.dumps(self.projects).encode('ascii')))

        self.init.storage.add_file_from_binary_global(model_path,model)
        self.init.storage.reset_buffer()
        
        json_path = self.prefix_models + 'models.json'
        self.init.storage.add_file_from_binary_global(json_path,io.BytesIO(json.dumps(self.models).encode('ascii')))
        self.init.storage.reset_buffer()
        
        json_path = self.prefix_experiments + 'experiments.json'
        self.init.storage.add_file_from_binary_global(json_path,io.BytesIO(json.dumps(self.experiments).encode('ascii')))
        self.init.storage.reset_buffer()

    def get_model(self, label):
        model_path = ''
        for model_path in self.models['uri']:
            if label in model_path:
                return self.init.storage.load_file_global(model_path)
        return None
    
    def get_latest_run_number(self):
		# checks all the runs
        run_path, _ = self.init.storage.load_list_in_path(self.prefix_experiments + 'logs/')

		# gets the list in number
        run_path = [int(x.replace(self.prefix_experiments+'logs/','')) for x in run_path]

        if len(run_path):
            return max(run_path) + 1
        else:
            return 0

    def get_latest_prediction_number(self):
		# checks all the prediction
        try:
            self.predictions = json.load(self.init.storage.load_file_global(self.prefix_predictions + 'predictions.json'))
            return len(self.predictions.keys())
        except:
            self.init.storage.add_file_from_binary_global(self.prefix_predictions + 'predictions.json',io.BytesIO(json.dumps({}).encode('ascii')))
            return 0