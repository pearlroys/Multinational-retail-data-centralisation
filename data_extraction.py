from database_utils import DataConnector
import pandas as pd
import sys
import tabula
import re
sys.path.append('../')



class DataExtractor:
    def __init__(self) -> None:
        pass
        
     
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
    
    

if __name__ == '__main__':
    extract = DataExtractor()
    # extract.read_rds_tables('legacy_users')
    
    pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    df = extract.retrieve_pdf_data(pdf_path)
   