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
    
    def remove_char_from_string(self, value):
        return re.sub(r'\D', '',value)
    
    def clean_store_data(self, df):
        # change datetime datatype
        df = self.format_date(df,'opening_date')

        # df.drop(columns='lat',inplace=True)
        df.drop(columns='lat',inplace=True)

        # drop rown with nan's
        df = df.dropna(subset=['address'], how='any')

        # Remove newline characters from the 'address' column
        df['address'] = df['address'].str.replace('\n', '')

        # drop cryptic values
        values = ['QP74AHEQT0',
       'O0QJIRC943', '50IB01SFAZ', '0RSNUU3DF5', 'B4KVQB3P5Y',
       'X0FE7E2EOG', 'NN04B3F6UQ']
        df = df.drop(df[df['store_type'].isin(values)].index)

         # change datatype for longitude and lat
        df['latitude'] = df['latitude'].astype(float)

        # Replace multiple values in a specific column using a dictionary where eeruope and eeamerica is present
        replace_dict = {'eeEurope': 'Europe', 'eeAmerica': 'America'}
        df['continent'] = df['continent'].replace(replace_dict)
        # change datatype of staff_numbers
        df['staff_numbers'] =  pd.to_numeric( df['staff_numbers'].apply(self.remove_char_from_string),errors='coerce', downcast="integer")
        df['longitude'] =  pd.to_numeric( df['longitude'].apply(self.remove_char_from_string),errors='coerce', downcast="float")
        return df
    