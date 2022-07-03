import argparse
import os
from extract import Extract_To_DataLake
from transform import Transform_Data
from load import Database

class ETL_Driver:
    def __init__(self, url_list, user, password, dsn) -> None:
        self.extractDataObj = Extract_To_DataLake(url_list)
        self.transformDataObj = Transform_Data(self.extractDataObj.lake_path)
        self.db_conn = Database(user, password, dsn)
    
    def initiate_etl_job(self):
        self.extractDataObj.request_data()
        data = self.transformDataObj.transform_data()
        self.db_conn.move_data_to_data_warehouse(data)
        self.db_conn.perform_analytics()
        self.db_conn.close_connection()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--url_list', nargs='+', help='Provide HTTPS endpoints with spaces')
    parser.add_argument('--user', help='Oracle DB User-Name')
    parser.add_argument('--password', help='Oracle DB User-Password')
    parser.add_argument('--dsn', help='Oracle DSN e.g. localhost:1521/xe/SPARK-DB')
    args = parser.parse_args()
    url_list = args.url_list
    user = args.user
    password = args.password
    dsn = args.dsn
    driverObj = ETL_Driver(url_list, user, password, dsn)
    driverObj.initiate_etl_job()

#CMD command to execute the job
# python etl\main.py --url_list \
# https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users \
# https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages \
# --user USERNAME --password PASSWORD --dsn localhost/:1521/xe/SPARK-DB