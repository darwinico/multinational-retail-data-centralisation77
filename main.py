import pandas as pd
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

def process_and_upload_card_data(pdf_link, local_db_connector):
    data_extractor = DataExtractor()
    data_cleaner = DataCleaning()
    card_data_df = data_extractor.retrieve_pdf_data(pdf_link)
    print("Extracted Card Data:")
    print(card_data_df.head())
    cleaned_card_data_df = data_cleaner.clean_card_data(card_data_df)
    print("Cleaned Card Data:")
    print(cleaned_card_data_df.head())
    local_db_connector.upload_to_db(cleaned_card_data_df, "dim_card_details")
    print("Card data uploaded to the local database.")

def extract_and_clean_user_data(api_key, rds_db_connector):
    data_extractor = DataExtractor(api_key=api_key)
    df = data_extractor.read_rds_table(rds_db_connector, 'legacy_users')
    print("Extracted User Data:")
    print(df.head())
    cleaner = DataCleaning()
    cleaned_df = cleaner.clean_user_data(df)
    print("Cleaned User Data:")
    print(cleaned_df.head())
    return cleaned_df

def extract_and_clean_store_data(api_key):
    data_extractor = DataExtractor(api_key=api_key)
    num_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    retrieve_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    number_of_stores = data_extractor.list_number_stores(num_stores_endpoint)
    stores_df = data_extractor.retrieve_stores_data(retrieve_store_endpoint, number_of_stores)
    print("Extracted Store Data:")
    print(stores_df.head())
    cleaner = DataCleaning()
    cleaned_stores_df = cleaner.clean_store_data(stores_df)
    print("Cleaned Store Data:")
    print(cleaned_stores_df.head())
    return cleaned_stores_df

def extract_clean_upload_products(api_key, local_db_connector):
    extractor = DataExtractor(api_key)
    df = extractor.extract_from_s3('s3://data-handling-public/products.csv')
    print("Extracted Product Data:")
    print(df.head())
    cleaner = DataCleaning()
    df = cleaner.convert_product_weights(df)
    df = cleaner.clean_products_data(df)
    print("Cleaned Product Data:")
    print(df.head())
    local_db_connector.upload_to_db(df, "dim_products")
    print("Product data uploaded to the local database.")

def extract_clean_orders(rds_db_connector, local_db_connector):
    orders_table_name = 'orders_table'
    data_extractor = DataExtractor()
    orders_df = data_extractor.read_rds_table(rds_db_connector, orders_table_name)
    print("Extracted Orders Data:")
    print(orders_df.head())
    data_cleaner = DataCleaning()
    cleaned_orders_df = data_cleaner.clean_orders_data(orders_df)
    print("Cleaned Orders Data:")
    print(cleaned_orders_df.head())
    local_db_connector.upload_to_db(cleaned_orders_df, "dim_orders")
    print("Orders data uploaded to the local database.")

def extract_clean_upload_date_times(s3_link, local_db_connector):
    data_extractor = DataExtractor()
    date_times_df = data_extractor.extract_from_s3(s3_link, file_format='json')
    print("Extracted Date Times Data:")
    print(date_times_df.head(10))
    data_cleaner = DataCleaning()
    cleaned_date_times_df = data_cleaner.clean_date_times_data(date_times_df)
    print("Cleaned Date Times Data:")
    print(cleaned_date_times_df.head())
    local_db_connector.upload_to_db(cleaned_date_times_df, "dim_date_times")
    print("Date times data uploaded to the local database.")

def main():
    api_key = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    local_db_connector = DatabaseConnector()
    local_db_connector.init_db_engine("local_db_creds.yaml")
    rds_db_connector = DatabaseConnector()
    rds_db_connector.init_db_engine("db_creds.yaml")

    pdf_link="https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    process_and_upload_card_data(pdf_link, local_db_connector)
    cleaned_user_data_df = extract_and_clean_user_data(api_key, rds_db_connector)
    local_db_connector.upload_to_db(cleaned_user_data_df, "dim_users")
    cleaned_stores_df = extract_and_clean_store_data(api_key)
    local_db_connector.upload_to_db(cleaned_stores_df, "dim_store_details")
    extract_clean_upload_products(api_key, local_db_connector)
    extract_clean_orders(rds_db_connector, local_db_connector)
    s3_link = 's3://data-handling-public/date_details.json'
    extract_clean_upload_date_times(s3_link, local_db_connector)

if __name__ == '__main__':
    main()


