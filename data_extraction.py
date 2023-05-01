from database_utils import DataConnector
import pandas as pd
import sys
sys.path.append('../')



class DataExtractor:
    def __init__(self) -> None:
        self.reader = DataConnector()
        self.engine = self.reader.init_db_engine()
       


    def  read_rds_tables(self, table_name):
        
        """ intially tried to use this line of code but ran into exceptions 'pd.read_sql_table('table', engine)'
    For pandas read_sql_query, there are two things that are easy to get wrong. 
    to avoid this error you need to pass a connection (not the engine) and you need to use 
    the text function to convert the query.
        """
        self.conn = self.engine.connect()
        df = pd.read_sql_table(table_name, self.conn)
        # df.to_csv('Output.csv', index = False)
        return df 
    



if __name__ == '__main__':
    extract = DataExtractor()
    extract.read_rds_tables('legacy_users')
