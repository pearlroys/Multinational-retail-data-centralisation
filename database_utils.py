import yaml
from yaml.loader import SafeLoader
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
class DataConnector:
    def __init__(self) -> None:
        pass
    def read_db_creds(self):
        """ This function parse and converts a YAML object to a Python dictionary (dict object). 
        This process is known as Deserializing YAML into a Python."""
        # Open the file and load the file
        with open('db_creds.yaml') as f:
            self.data = yaml.safe_load(f)
            return self.data

    def init_db_engine(self):
        db_url = 'postgresql+{RDS_DBAPI}//{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}'.format(**self.data)
        self.engine = create_engine(db_url)
        return self.engine

        
        






if __name__ == '__main__':
    reader = DataConnector()
    reader.read_db_creds()