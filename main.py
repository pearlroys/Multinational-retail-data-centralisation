import pandas as pd
from database_utils import DataConnector 
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def upload_dim_users():
    
    extract = DataExtractor()
    cleaner = DataCleaning()
    reader = DataConnector()

    # connect to base and get list of frames
    data = reader.read_db_creds("db_creds.yaml") 
    engine = reader.init_db_engine(data)
    engine.connect()
    tables_list = reader.list_db_tables()

    # get clean chosen frame
    df_name = tables_list[1]
    df = cleaner.clean_user_data(extract.read_rds_tables(df_name, engine))
    print(df.head())

    # upload to the local db
    local_db = reader.read_db_creds("local_dc.yaml") 
    engine = reader.init_db_engine(local_db)
    engine.connect()
    reader.upload_to_db(df,'dim_users',engine)

def upload_dim_card_details():
    extract = DataExtractor()
    cleaner = DataCleaning()
    reader = DataConnector()
  
    # get data from pdf
    df = extract.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    print(df.head())
    print(df.info())
    # clean data
    df = cleaner.clean_card_data(df)
    print(df.info())
    print(df.head())

    # upload to the local db
    local_db = reader.read_db_creds("local_dc.yaml") 
    engine = reader.init_db_engine(local_db)
    engine.connect()
    reader.upload_to_db(df,'dim_card_details',engine)

def upload_dim_store_details():
    extract = DataExtractor()
    cleaner = DataCleaning()
    reader = DataConnector()
    # get data
    df = extract.retrieve_stores_data()
    # df.to_csv('dim_store_details.csv')
    # print(df.head)


upload_dim_store_details()