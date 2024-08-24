import pandas as pd
import requests
import boto3
import io
import tabula

class DataExtractor:
    def __init__(self, api_key=None):
        self.headers = {"x-api-key": api_key}

    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine("db_creds.yaml")
        return pd.read_sql_table(table_name, engine)

    def retrieve_pdf_data(self, pdf_link):
        df_list = tabula.read_pdf(pdf_link, pages="all")
        return pd.concat(df_list)

    def list_number_stores(self, endpoint):
        response = requests.get(endpoint, headers=self.headers)
        response.raise_for_status()
        return int(response.json().get("number_stores"))

    def retrieve_stores_data(self, endpoint_template, number_of_stores):
        store_data = []
        for store_number in range(1, number_of_stores + 1):
            endpoint = endpoint_template.format(store_number=store_number)
            response = requests.get(endpoint, headers=self.headers)
            if response.status_code == 200:
                store_data.append(response.json())
        return pd.DataFrame(store_data)

    def extract_from_s3(self, s3_address, file_format='csv'):
        session = boto3.Session()
        s3 = session.client('s3')
        path_components = s3_address.replace('s3://', '').split('/', 1)
        bucket_name, file_key = path_components[0], path_components[1]
        file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        body = file_obj['Body']
        if file_format == 'csv':
            return pd.read_csv(io.BytesIO(body.read()))
        elif file_format == 'json':
            return pd.read_json(io.BytesIO(body.read()))
        else:
            raise ValueError(f"Unsupported file format: {file_format}. Supported formats are 'csv' and 'json'.")