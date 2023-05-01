import yaml
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

class DataConnector:
    def __init__(self) -> None:
        self.data = self.read_db_creds()
        

    def read_db_creds(self):
        """ This function parse and converts a YAML object to a Python dictionary (dict object). 
        This process is known as Deserializing YAML into a Python."""
        # Open the file and load the file
        with open('db_creds.yaml') as f:
            self.data = yaml.safe_load(f)
            return self.data

    def init_db_engine(self):
        # db_url = 'postgresql+{RDS_DBAPI}//{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}'.format(**self.data)
        db_url = '{RDS_DATABASEsql}+{RDS_DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}'.format(**self.data)
        self.engine = create_engine(db_url)
        return self.engine
    
    def list_db_tables(self):
        """This function returns an Inspector object, which is a wrapper around the database, 
        and it allows us to retrieve information about the tables and columns inside the database.

        Returns:
            list of tables in the database
        """
        
        self.engine.connect()
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df, new_table_name, engine):
        df.to_sql(new_table_name, engine, if_exists='replace')

       


    

if __name__ == '__main__':
    reader = DataConnector()
    engine = reader.init_db_engine()
    engine.connect()
    print("Hi") 
    print(engine)
    
    
    
  
    
    