from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq
from google.oauth2 import service_account
from multiprocessing import Process
import os
import json
import time
import importlib.util
from datetime import datetime as dt


class ProcessManager:
    def __init__(self):
        self.command = 'command'
        self.processes = {}
        self.modules = {}
        self.scedule = {}
        self.processes_amount = 0
        self.config_dir ='./config'
        self.modules_dir = './modules'
        self.active = False
        self.active_processes = []
        self.processes_to_exetute= []

    

    def check_config_file(self, process_id):
        # Этот метод проверят - была ли изменена конфигурация процесса (модуль либо кинфгурационный файл)
        last_process_update = dt.strptime(self.processes[process_id]['last_update'],'%Y-%m-%d %H-%M-%S').timestamp()
        last_conf_update = os.path.getmtime(self.config_dir + '/' + self.processes[process_id]['name']+'.json')
        last_module_update = os.path.getmtime((self.modules_dir + '/' + self.processes[process_id]['name'] +'.py'))

        if (last_process_update < last_conf_update) | (last_process_update < last_module_update):
            self.update_config_by_id(process_id)

    def import_module(self, name):
        # Этот метод импортирует модуль по имени из папки modules
        exec(f"import modules.{name} as {name} ", globals())

        

    def execute_scedule(self) -> list:
        # Этот метод проверяет какие процессы должны запуститься по расписанию и записывает в поле active_processes
        pass

    def run_processes(self):
        # Этот метод запускает процессы которые лежат в self.active_processes
        for process_id in self.processes_to_exetute:
            self.check_config_file(process_id)
            task = Process(target=self.processes[process_id]['module'],args=(), daemon=True)
            task.start()

    def update_scedule(self):
        # Этот метод обновляет распиcание в соответсвие с self.processes
        for process_id, process in self.processes.items():
            self.scedule[process_id] = process['scedule']

    def update_config_by_id(self, process_id):
        # Этот метод обновляет конфигурацию процесса по его id
        #file_path = os.path.join(self.config_dir[process_id], self.processes[process_id]['name'])
        file_path = self.config_dir + '/' + self.processes[process_id]['name'] +'.json'
        with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                data['last_update'] = dt.strftime(dt.now(), '%Y-%m-%d %H-%M-%S')
                self.processes[data['process_id']] = data
                self.import_module(data['name'])
                with open(file_path, 'w', encoding='utf-8') as file:
                    json.dump(data, file)

                exec(f"self.processes[{process_id}]['module'] = {self.processes[process_id]['name']}.main()")


    def update_config_list(self):
        # Этот метод обновляет список процессов в соотвествии с папкой ./config
        folder_path = self.config_dir
        for filename in os.listdir(folder_path):
            if filename.endswith('.json'):
                file_path = os.path.join(folder_path, filename)
                with open(file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    process_id = data['process_id']
                    data['last_update'] = dt.strftime(dt.now(), '%Y-%m-%d %H-%M-%S')
                    self.processes[process_id] = data
                    self.import_module(data['name'])
                with open(file_path, 'w', encoding='utf-8') as file:
                    json.dump(data, file)

                exec(f"self.processes[{process_id}]['module'] = {self.processes[process_id]['name']}.main()")
                    
        
    def check_processes(self):
        # Устаревший процесс на удаление
        dir_amount = len([f for f in os.listdir(self.processes_dir)])
        if self.processes_amount != dir_amount:
            self.update_processes_list()

    def run(self):
        # Этот метод каждую секунду проверяет условия и запускает процессы по расписанию
        self.update_config_list()
        self.update_scedule()
        self.processes_to_exetute = [1, 2, 3]
        while True:
            #self.execute_scedule()
            self.run_processes()
            time.sleep(30)

p = ProcessManager()
p.update_config_list()
p.update_scedule()
p.run()
