import pandas as pd
import re
import pycountry

class DataCleaning:
    def clean_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df.dropna(subset=['first_name', 'last_name', 'email_address', 'join_date'], inplace=True)
        df['date_of_birth'].fillna('1970-01-01', inplace=True)
        df['company'].fillna('Unknown', inplace=True)
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce').fillna(pd.to_datetime('1970-01-01'))
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce').fillna(pd.to_datetime('1970-01-01'))
        df['phone_number'] = df['phone_number'].astype(str)
        df['email_address'] = df['email_address'].apply(self.validate_email)
        valid_country_codes = [country.alpha_2 for country in pycountry.countries]
        df = df[df['country_code'].isin(valid_country_codes)]
        return df

    def validate_email(self, email: str) -> str:
        email_regex = re.compile(r"[^@]+@[^@]+\.[^@]+")
        if email_regex.match(email):
            return email
        else:
            return 'invalid@example.com'
    
    def clean_card_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df.dropna(inplace=True)
        df['card_number'] = df['card_number'].astype(str)
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df = df[df['date_payment_confirmed'] <= df['expiry_date']]
        
        def validate_card_number(row):
            match = re.search(r'(\d+)\s?digit', row['card_provider'])
            if match:
                num_digits = int(match.group(1))
                return len(row['card_number']) == num_digits
            else:
                return len(row['card_number']) == 16
        
        df = df[df.apply(validate_card_number, axis=1)]
        
        df.dropna(inplace=True)
        
        return df
    
    def clean_store_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce').fillna(0)
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce').fillna(0)
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce').fillna(0).astype(int)
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
        df.rename(columns={
            'index': 'store_id',
            'address': 'store_address',
            'longitude': 'store_longitude',
            'latitude': 'store_latitude',
            'locality': 'store_locality',
            'store_code': 'store_code',
            'staff_numbers': 'number_of_staff',
            'opening_date': 'date_opened',
            'store_type': 'type_of_store',
            'country_code': 'country',
            'continent': 'continent'
        }, inplace=True)

        return df
    
    def convert_product_weights(self, df):
        def convert_weight(weight):
            if not isinstance(weight, str):
                weight = str(weight)
            
            weight = weight.lower().strip()

            try:
                if 'kg' in weight:
                    unit = "kg"
                    divisor = 1
                elif 'g' in weight:
                    unit = "g"
                    divisor = 1000
                elif 'ml' in weight:
                    unit = "ml"
                    divisor = 1000
                else:
                    return None  
            except ValueError:
                return None  
            
            weight = weight.split(unit)[0]

            if 'x' in weight:
                parts = weight.split('x')
                try:
                    weight = float(parts[0].strip()) * float(parts[1].strip())
                except ValueError:
                    return None  
            try: 
                return float(weight)/divisor
            except ValueError:
                return None

        df['weight_kg'] = df['weight'].apply(convert_weight)
        return df

    def clean_products_data(self, df):
        df.dropna(subset=['weight_kg'], inplace=True)
        df.drop_duplicates(inplace=True)
        return df
    
    def clean_orders_data(self,df):
        df = df.drop(columns=['first_name', 'last_name', '1'], errors='ignore')
       
        columns_to_drop = ['level_0', 'index', 'first_name', 'last_name', '1']
        df=df.drop(columns=columns_to_drop, errors='ignore')

        
        df.dropna(subset=['user_uuid', 'store_code', 'product_code', 'product_quantity'])
        return df

    def clean_date_times_data(self, df):
        df_cleaned = df.dropna(how='all')
        df_cleaned = df_cleaned[pd.to_numeric(df_cleaned['month'], errors='coerce').notnull()]
        df_cleaned = df_cleaned[pd.to_numeric(df_cleaned['year'], errors='coerce').notnull()]
        df_cleaned = df_cleaned[pd.to_numeric(df_cleaned['day'], errors='coerce').notnull()]
        time_pattern = r'^\d{2}:\d{2}:\d{2}$'
        df_cleaned = df_cleaned[df_cleaned['timestamp'].str.match(time_pattern)]
        return df_cleaned
