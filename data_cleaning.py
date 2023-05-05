from datetime import datetime
import re
import pandas as pd
import sys
from database_utils import DataConnector
sys.path.append('../')


class DataCleaning:
    def __init__(self) -> None:
        # self.reader = DataConnector()
        pass
        

    def clean_user_data(self, df):
        """ cleans dataframe, by modifying date columns, remove nans,cleaning
          and modifying phone number and country code

        Args:
            df (dataframe): dataframe to be cleaned

        Returns:
            _type_: cleaned date frame
        """
        df = self.format_date(df, 'date_of_birth')
        df = self.format_date(df, 'join_date')        
        df = self.modify_phone_num(df, 'phone_number')
        df = self.modify_country_code(df, 'country_code')
        return df
    
    def format_date(self, df, column):
        """ cleans date by changing from object type to datetime format

        Args:
            df (pd.df): dataframe
            column (object): column name 

        Returns:
            datatime: datetime dataetype returned
        """
        df[column] = pd.to_datetime(df[column], format='%Y-%m-%d', errors='ignore')
        df[column] = pd.to_datetime(df[column], format='%Y %B %d', errors='ignore')
        df[column] = pd.to_datetime(df[column], format='%B %Y %d', errors='ignore')
        df[column] = pd.to_datetime(df[column], errors='coerce')
        #     invalid_dates = df[df[column].isnull()]
        #     if not invalid_dates.empty:

          #         print(invalid_dates)
        # 36 NaT values were found and dropped.
        df = df.dropna(subset=[column],how='any')
        return df
        
    def modify_phone_num(self, df, column):
        """ cleans phone number by removing irrelevant characters like (., /, +)
        formatting the phone numbers to international format with code in brackets

        Args:
            df (pd.df): 
            column (float): 

        Returns:
            int: cleaned phone numbers
        """
        df[column] = df[column].replace('\.', '', regex=True)

        # Remove non-digit characters from phone column
        df[column] = df[column].str.replace(r'\D+', '')
        # df[column].astype(int)

        # Format phone numbers as (XXX) XXX-XXXX
        df[column] = df[column].apply(lambda x: '{} {}-{}'.format(x[0:3], x[3:6], x[6:]))

        # Remove the second and third digits if they are '00'
        df[column] = df[column].apply(lambda x: x[0] + x[3:] if x[1:3] == '00' else x)

        # Remove newline characters from the 'Column' column
        df[column] = df[column].replace(to_replace='\\n', value='\n', regex=True)
        return df
    
    def modify_country_code(self, df, column):
        df[column] = df[column].str.replace(r'GGB', 'GB')
        return df 
    
    def clean_card_data(self, df):
        
        # replace ? at the begininning of card number 
        df['card_number'] = df['card_number'].replace(r'\?', '', regex=True)
        # print(df) where nan
        print(df['card_number'].isna().sum())
        # change data type to str
        df['card_number'] = df['card_number'].astype(str)

        # confirm ? is not in df[card_number]
        new_df = df.loc[df['card_number'].str.startswith('?')].copy()
        # # print the result
        print(new_df['card_number'])
        
        df = self.format_date(df,'date_payment_confirmed') 
        print(df.dtypes) 
        df.dropna(how='any')
        return df
    