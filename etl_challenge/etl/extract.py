import os
import requests
import json

class Extract_To_DataLake:
    def __init__(self, url_list) -> None:        
        self.url_dict = {}
        for url in url_list:
            self.url_dict[os.path.split(url)[-1]] = url
        self.response_dict = {}
        self.lake_path = os.path.join(os.getcwd(), 'data_lake')
    
    def filter_pii(self, key) -> None:
        self.response_dict[key] = self.response_dict[key].json()
        for elem in self.response_dict[key]:
            elem.pop('message', None)
            elem.pop('firstName', None)
            elem.pop('lastName', None)
            elem.pop('address', None)
            if 'email' in elem:
                elem['email'] = elem['email'].split('@')[1]
    
    def save_in_data_lake(self, key):
        with open(os.path.join(self.lake_path, key+".json"), 'w') as file:
            json.dump(self.response_dict[key], file, indent=4)
    
    def request_data(self) -> None:
        for file_key in self.url_dict:
            self.response_dict[file_key] = requests.get(url = self.url_dict[file_key])
            self.filter_pii(file_key)
            self.save_in_data_lake(file_key)