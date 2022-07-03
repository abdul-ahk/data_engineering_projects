import os
import json
import pandas as pd
from datetime import datetime, date

class Transform_Data:
    def __init__(self, source_path) -> None:
        self.source_path = source_path
        self.file_identifiers = []
        self.source_data = {}
        self.df = {}
        self.df['Date'] = pd.DataFrame(columns= ['dateDay', 'dateMonth', 'dateYear'])
        tmp = os.listdir(self.source_path)
        for i in tmp:
            self.file_identifiers.append(i.split('.')[0])

    def get_data(self):
        for file_id in self.file_identifiers:
            with open(os.path.join(self.source_path, file_id+".json"), 'r') as file:
                self.source_data[file_id] = json.load(file)
    
    def to_int(self, table_id, col_name):
        self.df[table_id][col_name] = self.df[table_id][col_name].astype(int)
    
    def to_float(self, table_id, col_name):
        self.df[table_id][col_name] = self.df[table_id][col_name].astype(float)
    
    def to_date(self, table_id, col_name):
        self.df[table_id][col_name] = pd.to_datetime(self.df[table_id][col_name]).dt.date

    def move_date_to_date_dim(self, table_id, col_name):
        dateDay = self.df[table_id][col_name]
        dateDay = pd.to_datetime(dateDay, format='%Y-%m-%d')
        dateMonth = dateDay.dt.strftime('%Y-%m')
        dateYear = dateDay.dt.year
        temp_date_dic = {'dateDay': dateDay, 'dateMonth': dateMonth, 'dateYear': dateYear}
        temp_date_df = pd.DataFrame(temp_date_dic)
        self.df['Date'] = self.df['Date'].append(temp_date_df, ignore_index=True)
        
    def  transform_msg_data(self, table_id):
        self.df[table_id] = pd.DataFrame(self.source_data[table_id])
        self.to_date(table_id, 'createdAt')
        self.df[table_id].rename({'id': 'msgId'}, axis=1, inplace=True)
        self.move_date_to_date_dim(table_id, 'createdAt')
        self.to_int(table_id, 'msgId')
        self.to_int(table_id, 'senderId')
        self.to_int(table_id, 'receiverId')

    def transform_user_json_to_dataframes(self, table_id):
        subscription_list = []
        for elem in self.source_data[table_id]:
            profile_dict = elem.pop('profile', None)
            elem.update(profile_dict)
            sub_dict = elem.pop('subscription', None)
            for sub_elem in sub_dict:
                sub_elem['userId'] = elem['id']
                subscription_list.append(sub_elem)
        self.df[table_id] = pd.DataFrame(self.source_data[table_id])
        self.df[table_id].rename({'id': 'userId'}, axis=1, inplace=True)
        self.df['Subscription'] = pd.DataFrame(subscription_list)
    
    def transform_user_df_dates(self, table_id):
        self.to_date(table_id, 'createdAt')
        self.to_date(table_id, 'updatedAt')
        self.df[table_id]['birthDate'] = pd.to_datetime(self.df[table_id]['birthDate'])
        today = date.today()
        age_mask = (today.month < self.df[table_id]['birthDate'].dt.month) & (today.day < self.df[table_id]['birthDate'].dt.day)
        age_mask = age_mask.astype(int)
        self.df[table_id]['age'] = (today.year - self.df[table_id]['birthDate'].dt.year) - age_mask
    
    def filter_user_df_cols(self, table_id):
        del self.df[table_id]['zipCode']
        del self.df[table_id]['birthDate']
        del self.df[table_id]['profession']

    def transform_user_data(self, table_id):
        self.transform_user_json_to_dataframes(table_id)
        self.transform_user_df_dates(table_id)
        self.filter_user_df_cols(table_id)
        self.move_date_to_date_dim(table_id, 'createdAt')
        self.move_date_to_date_dim(table_id, 'updatedAt')
        self.to_int(table_id, 'userId')
        self.to_int(table_id, 'age')
        self.to_float(table_id, 'income')
    
    def transform_subscription_data(self, table_id):
        self.to_date(table_id, 'createdAt')
        self.to_date(table_id, 'startDate')
        self.to_date(table_id, 'endDate')
        self.df[table_id] = self.df[table_id].reset_index()
        self.df[table_id].rename({'index': 'subId'}, axis=1, inplace=True)
        self.df[table_id]['subId'] = self.df[table_id]['subId'] + 1
        self.move_date_to_date_dim(table_id, 'createdAt')
        self.move_date_to_date_dim(table_id, 'startDate')
        self.move_date_to_date_dim(table_id, 'endDate')
        self.to_int(table_id, 'subId')
        self.to_int(table_id, 'userId')
        self.to_float(table_id, 'amount')
    
    def transform_date_data(self, table_id):
        self.df[table_id] = self.df[table_id].drop_duplicates(subset=['dateDay'])
        self.df[table_id] = self.df[table_id].sort_values(by=['dateDay'])
        self.df[table_id] = self.df[table_id].reset_index()
        del self.df[table_id]['index']
        self.df[table_id] = self.df[table_id].reset_index()
        self.df[table_id].rename({'index': 'dateId'}, axis=1, inplace=True)
        self.df[table_id]['dateId'] = self.df[table_id]['dateId'] + 1
        self.to_int(table_id, 'dateYear')
        self.to_int(table_id, 'dateId')
    
    def build_relationship(self, table_id, col_name,date_id='Date'):
        for index, row in self.df[date_id].iterrows():
            self.df[table_id].loc[self.df[table_id][col_name] == row['dateDay'], col_name] = row['dateId']
    
    def build_dimensions_date_relationship(self):
        for table_id in self.df:
            if table_id == 'messages':
                self.build_relationship(table_id, 'createdAt')
                self.to_int(table_id, 'createdAt')
            elif table_id == 'users':
                self.build_relationship(table_id, 'createdAt')
                self.build_relationship(table_id, 'updatedAt')
                self.to_int(table_id, 'createdAt')
                self.to_int(table_id, 'updatedAt')
            elif table_id == 'Subscription':
                self.build_relationship(table_id, 'createdAt')
                self.build_relationship(table_id, 'startDate')
                self.build_relationship(table_id, 'endDate')
                self.to_int(table_id, 'createdAt')
                self.to_int(table_id, 'startDate')
                self.to_int(table_id, 'endDate')

    def transform_data(self):
        self.get_data()    
        self.transform_msg_data('messages')
        self.transform_user_data('users')
        self.transform_subscription_data('Subscription')
        self.transform_date_data('Date')
        self.build_dimensions_date_relationship()
        return self.df