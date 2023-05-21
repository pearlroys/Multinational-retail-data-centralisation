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
    df = tables_list[1]
    df = cleaner.clean_user_data(extract.read_rds_tables(df, engine))
    # upload to the local db
    connect_and_upload('dim_users', df)

def upload_dim_card_details():
    extract = DataExtractor()
    cleaner = DataCleaning()
    # get data from pdf
    df = extract.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf') 
    # clean data
    df = cleaner.clean_card_data(df) 
    # upload to the local db
    connect_and_upload('dim_card_details', df)

def upload_dim_store_details():
    extract = DataExtractor()
    cleaner = DataCleaning()
    # get data
    df = extract.retrieve_stores_data()
    # clean data 
    df = pd.read_csv('frames.csv', index_col=0)
    df = cleaner.clean_store_data(df)

    # upload to db 
    connect_and_upload('dim_store_details', df)
    


def upload_dim_products():
    extract = DataExtractor()
    cleaner = DataCleaning()
    # get data from s3
    df =  extract.extract_from_s3()
    # clean data
    df =  cleaner.clean_products_data(df)
    df =  cleaner.convert_product_weights(df)
    # upload to db 
    connect_and_upload('dim_products', df)
    

def upload_orders():
    extract = DataExtractor()
    cleaner = DataCleaning()
    reader = DataConnector() 
     # connect to base and get list of frames
    data = reader.read_db_creds("db_creds.yaml") 
    engine = reader.init_db_engine(data)
    engine.connect()
    tables_list = reader.list_db_tables()
    # get clean chosen frame
    df_name = tables_list[2]
    df = extract.read_rds_tables(df_name, engine)
    # clean data 
    df = cleaner.clean_order_data(extract.read_rds_tables(df_name, engine))  
    # upload to db 
    connect_and_upload('orders_table', df)

def dim_date_times():
    extract = DataExtractor()
    cleaner = DataCleaning()
    df = extract.extract_json_from_s3()
    df = cleaner.clean_date_time(df)
    connect_and_upload('dim_date_times', df)
    

def connect_and_upload(table_name, df):
    reader = DataConnector()
    local_db = reader.read_db_creds("local_dc.yaml")  
    engine = reader.init_db_engine(local_db)
    engine.connect()
    reader.upload_to_db(df, table_name, engine)

upload_dim_card_details()
