from datetime import date
import pandas as pd
import numpy as np
import cx_Oracle
import os

class Database:
    def __init__(self, usr, pswrd, dn) -> None:
        self.connection = cx_Oracle.connect(user=usr, password= pswrd, dsn=dn)
        self.cursor = self.connection.cursor()
        self.data = None

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
    
    def create_msg_table(self):
        self.cursor.execute("""
            CREATE TABLE message (
                msgId NUMBER NOT NULL,
                createdAt NUMBER,
                senderId NUMBER,
                receiverId NUMBER,
                PRIMARY KEY (msgId),
                FOREIGN KEY (senderId) REFERENCES appuser(userId),
                FOREIGN KEY (receiverId) REFERENCES appuser(userId),
                FOREIGN KEY (createdAt) REFERENCES datedim(dateId))""")
    
    def create_subscription_table(self):
        self.cursor.execute("""
            CREATE TABLE subscription (
                subId NUMBER NOT NULL,
                createdAt NUMBER,
                startDate NUMBER,
                endDate NUMBER,
                status VARCHAR(10),
                amount NUMBER(*,2),
                userId NUMBER,
                PRIMARY KEY (subId),
                FOREIGN KEY (userId) REFERENCES appuser(userId),
                FOREIGN KEY (createdAt) REFERENCES datedim(dateId),
                FOREIGN KEY (startDate) REFERENCES datedim(dateId),
                FOREIGN KEY (endDate) REFERENCES datedim(dateId))""")
    
    def create_user_table(self):
        self.cursor.execute("""
            CREATE TABLE appuser (
                userId NUMBER NOT NULL,
                createdAt NUMBER,
                updatedAt NUMBER,
                city VARCHAR(20),
                country VARCHAR(20),
                email VARCHAR(20),
                gender VARCHAR(10),
                age NUMBER,
                isSmoking VARCHAR(10),
                income NUMBER,
                PRIMARY KEY (userId),
                FOREIGN KEY (createdAt) REFERENCES datedim(dateId),
                FOREIGN KEY (updatedAt) REFERENCES datedim(dateId))""")
    
    def create_date_table(self):
        self.cursor.execute("""
            CREATE TABLE datedim (
                dateId NUMBER NOT NULL,
                dateDay DATE,
                dateMonth VARCHAR(10),
                dateYear NUMBER,
                PRIMARY KEY (dateId))""")

    def define_schema(self):
        self.create_date_table()
        self.create_user_table()
        self.create_msg_table()
        self.create_subscription_table()
    
    def map_df_to_tuple_list(self, table_id):
        self.data[table_id] = self.data[table_id].values.tolist()
        self.data[table_id] = list(map(tuple, self.data[table_id]))

    def insert_date_data(self):
        self.map_df_to_tuple_list('Date')
        sql = ('INSERT INTO datedim (dateId, dateDay, dateMonth, dateYear) '
                'values(:dateId, :dateDay, :dateMonth, :dateYear)')
        self.cursor.executemany(sql, self.data['Date'])
        self.connection.commit()
    
    def map_userdata_to_table_structure(self, table_name):
        self.data[table_name] = self.data[table_name][['userId', 'createdAt', 'updatedAt', 'city', 'country',\
                                                        'email', 'gender', 'age', 'isSmoking', 'income']]
        self.data[table_name] = self.data[table_name].replace({np.nan: None})

    def insert_user_data(self):
        self.map_userdata_to_table_structure('users')
        self.map_df_to_tuple_list('users')
        sql = ('INSERT INTO appuser (userId, createdAt, updatedAt, city, country, email, gender, age, '
                'isSmoking, income) values(:userId, :createdAt, :updatedAt, :city, :country, :email, '
                ':gender, :age, :isSmoking, :income)')
        self.cursor.executemany(sql, self.data['users'])
        self.connection.commit()

    def insert_subscription_data(self):
        self.map_df_to_tuple_list('Subscription')
        sql = ('INSERT INTO subscription (subId, createdAt, startDate, endDate, status, amount, userId) '
                'values(:subId, :createdAt, :startDate, :endDate, :status, :amount, :userId)')
        self.cursor.executemany(sql, self.data['Subscription'])
        self.connection.commit()
    
    def map_msgdata_to_table_structure(self, table_id):
        self.data[table_id] = self.data[table_id][['msgId', 'createdAt', 'senderId', 'receiverId']]

    def insert_msg_data(self):
        self.map_msgdata_to_table_structure('messages')
        self.map_df_to_tuple_list('messages')
        sql = ('INSERT INTO message (msgId, createdAt, senderId, receiverId) values(:msgId, :createdAt, '
                ':senderId, :receiverId)')
        self.cursor.executemany(sql, self.data['messages'])
        self.connection.commit()

    def insert_data(self):
        self.insert_date_data()
        self.insert_user_data()
        self.insert_subscription_data()
        self.insert_msg_data()
    
    def perform_analytics(self):
        queries = open(os.path.join(os.path.join(os.getcwd(), 'sql'),'sql_test.sql'))
        queries = queries.read()
        sql_query = queries.split(';')
        for query in sql_query:
            if query != '':
                print(f"\nQUERY: {query}")
                self.cursor.execute(query)
                rows = self.cursor.fetchall()
                if rows:
                    print("RESULT:")
                    for row in rows:
                        print(row)

    def move_data_to_data_warehouse(self, df):
        self.data = df
        self.define_schema()
        self.insert_data()