from datetime import datetime
import re
import pandas as pd


class DataCleaning:
    def __init__(self) -> None:
        self.df = pd.read_csv('output.csv', index_col=0)

    def clean_user_data(self, df):
        df = self.format_date(self, self.df, 'date_of_birth')
        df = self.format_date(self, self.df, 'join_date')        
        df = self.modify_phone_num(self.df, 'phone_number')
        df = self.modify_country_code(self.df, 'country_code')
        return df
    
    def format_date(self, df, column):
        df[column] = pd.to_datetime(df[column], format='%Y-%m-%d', errors='ignore')
        df[column] = pd.to_datetime(df[column], format='%Y %B %d', errors='ignore')
        df[column] = pd.to_datetime(df[column], format='%B %Y %d', errors='ignore')
        df[column] = pd.to_datetime(df[column], errors='coerce')
        invalid_dates = df[df[column].isnull()]
        if not invalid_dates.empty:

            print(invalid_dates)
        # 36 NaT values were found and dropped.
        df = df.dropna(subset=[column])
        return df
    
    def modify_phone_num(df, column):
        df[column] = df[column].replace('\.', '', regex=True)

        # Remove non-digit characters from phone column
        df[column] = df[column].str.replace(r'\D+', '')
        df[column].astype(int)

        # Format phone numbers as (XXX) XXX-XXXX
        df[column] = df[column].apply(lambda x: '({}) {}-{}'.format(x[0:3], x[3:6], x[6:]))

        # Remove the second and third digits if they are '00'
        df[column] = df[column].apply(lambda x: x[0] + x[3:] if x[1:3] == '00' else x)

        # Remove newline characters from the 'Column' column
        df[column] = df[column].replace(to_replace='\\n', value='\n', regex=True)
        return df
    
    def modify_country_code(df, column):
        df[column] = df[column].str.replace(r'GGB', 'GB')
        return df 