from database_utils import DataConnector
import pandas as pd
import sys
import tabula
import re
import boto3
import json
import requests
sys.path.append('../')



class DataExtractor:
    def __init__(self) -> None:
        self.api_key = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
                        
        
     
    def  read_rds_tables(self, table_name, engine):
        """intially tried to use this line of code but ran into exceptions 'pd.read_sql_table('table', engine)'
    For pandas read_sql_query, there are two things that are easy to get wrong. 
    to avoid this error you need to pass a connection (not the engine) and you need to use 
    the text function to convert the query.

        Args:
            table_name (_type_): name of proposed table
            engine (engine to connect to): this is the variable from 
            databaseutils thats returned from init_db

        Returns:
            _type_: dataframe
        """
        self.reader = DataConnector()
        self.conn = engine.connect()
        df = pd.read_sql_table(table_name, self.conn)
        # df.to_csv('Output.csv', index = False)
        return df 
    
    def retrieve_pdf_data(self,link):
        return pd.concat(tabula.read_pdf(link, pages='all'))
    
    def list_number_of_stores(self):
        endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        response = requests.get(endpoint, headers=self.api_key)
        return response.json()['number_stores']

    def retrieve_stores_data(self):
        store_number   = self.list_number_of_stores()
        frames = []
        for i in range(store_number):
            endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}'
            response = requests.get(endpoint, headers=self.api_key)
            frames.append( pd.json_normalize(response.json()))
        return pd.concat(frames)
    
    def extract_from_s3(self):
        s3_client = boto3.client("s3")
        # Specify the bucket and object key
        bucket_name = 'data-handling-public'
        object_key = 'products.csv'

        # Retrieve the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        status   = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        # Access the data from the response
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            
            df = pd.read_csv(response.get("Body"))
            return df
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")
    def extract_json_from_s3(self):

        # Provide the S3 link to the JSON file
        s3_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        # Download the JSON file
        response = requests.get(s3_link)
        data = response.json()

        # Convert the JSON data to a DataFrame
        df = pd.DataFrame(data)
        return df
        

            
    
    

if __name__ == '__main__':
    extract = DataExtractor()
    # link = upload_dim_card_details
    # extract.retrieve_pdf_data(link)
    
    