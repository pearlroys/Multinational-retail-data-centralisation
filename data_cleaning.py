from datetime import datetime
import re
import pandas as pd
import sys
from database_utils import DataConnector
sys.path.append('../')


class DataCleaning:
   
        

    def clean_user_data(self, df):
        """ cleans dataframe, by modifying date columns, remove nans,cleaning
          and modifying phone number and country code

        Args:
            df (dataframe): dataframe to be cleaned

        Returns:
            _type_: cleaned date frame
        """
        df = self.format_date(df, 'date_of_birth')
        df.dropna(subset=['date_of_birth'],how='all')
        pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'

        # Filter the DataFrame to get rows where 'uuid' contains valid UUID codes
        df = df[df['user_uuid'].str.match(pattern, na=False)]
        df.reset_index(drop=True)
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
        return df
    
    def remove_char_from_string(self, value):
        return re.sub(r'\D', '',value)
    
    def clean_store_data(self, df):
        # change datetime datatype
        print(df.shape)
        df = self.format_date(df, 'opening_date')

        # df.drop(columns='lat',inplace=True)
        df.drop(columns= 'lat', inplace=True)

        # drop rown with nan's
        df = df.dropna(subset=['store_code'], how='any')

        # Remove newline characters from the 'address' column
        df['address'] = df['address'].str.replace('\n', '')

        # drop cryptic values
        values = ['QP74AHEQT0',
       'O0QJIRC943', '50IB01SFAZ', '0RSNUU3DF5', 'B4KVQB3P5Y',
       'X0FE7E2EOG', 'NN04B3F6UQ']
        df = df[~df['store_type'].isin(values)]

        # Replace multiple values in a specific column using a dictionary where eeruope and eeamerica is present
        replace_dict = {'eeEurope': 'Europe', 'eeAmerica': 'America'}
        df['continent'] = df['continent'].replace(replace_dict)
    
        return df
    

    def clean_products_data(self, df):
        # change datetime datatype
        df =  self.format_date(df,'date_added')
        df = df.dropna(subset=['product_name'],how='all')
        df.drop(columns='Unnamed: 0', inplace = True)
        df.reset_index(drop=True, inplace=True)
        df.dropna(how='all',inplace= True)       
        return df
    
    def convert_product_weights(self, df):

        # Convert the numbers column to string
        df['weight'] = df['weight'].astype(str)

        # Extract the letters from the numbers column
        df['unit'] = df['weight'].str.extract(r'([a-zA-Z]+)')
        df['unit'] = df['unit'].astype(str)

        # Print the resulting DataFrame with the extracted letters
        df['unit'].unique()
        
        # get all the numbers without the units and convert to float
        df["numbers"] = df["weight"].str.extract("(\d*\.?\d+)", expand=True)
        df['numbers'] = df['numbers'].astype(float)
       
        # convert weights all to KG
        df['weight (KG)'] = df.loc[df['unit'] == 'x', 'weight'].apply(lambda x: self.extract_numeric_value(x))
        df['weight (KG)'] = df['weight (KG)'].mask(df['unit'] == 'ml', df['numbers']/ 1000)
        df['weight (KG)'] = df['weight (KG)'].mask(df['unit'] == 'kg', df['numbers'])
        df['weight (KG)'] = df['weight (KG)'].mask(df['unit'] == 'g', df['numbers']/ 1000)
        df['weight (KG)'] = df['weight (KG)'].mask(df['unit'] == 'oz', df['numbers']/ 35.27)
        
        # remove rows with cryptic and unidentifiable units
        df = df[df['unit'] != 'GO']
        df = df[df['unit'] != 'MX']
        df = df[df['unit'] != 'Z']
        return df
            
    def extract_numeric_value(self, weight):
        # we can remove last value 'g'
        values = weight[:-1]
        # Split the string by 'x' and extract the first part and multiply by second part
        parts = values.split('x')
        first_value = int(parts[0])
        other_value = int(parts[1])
        return (first_value * other_value) /1000
    
    def clean_order_data(self, df):
        df.drop(columns='1',inplace=True)
        df.drop(columns='first_name',inplace=True)
        df.drop(columns='last_name',inplace=True)
        del df['index']
        df.reset_index(drop=True)
        df.drop(columns='level_0',inplace=True)
        df.dropna(how='any')
        return df
    
    def clean_date_time(self, df):
        df['month']         =  pd.to_numeric( df['month'],errors='coerce', downcast="integer")
        df['year']          =  pd.to_numeric( df['year'], errors='coerce', downcast="integer")
        df['day']           =  pd.to_numeric( df['day'], errors='coerce', downcast="integer")
        df['timestamp']     =  pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors='coerce')
        df.dropna(how='any',inplace= True)
        df.reset_index(drop=True)       
        return df