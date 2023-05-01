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
    tables_list = reader.list_db_tables(engine)

    # get clean chosen frame
    df_name = tables_list[1]
    df = cleaner.clean_user_data(extract.read_rds_table(df_name))
    print(df.head())

    # upload to the local db
    local_db = reader.read_db_creds("local_dc.yaml") 
    engine = reader.init_db_engine(local_db)
    engine.connect()
    reader.upload_to_db(df,'dim_users',engine)