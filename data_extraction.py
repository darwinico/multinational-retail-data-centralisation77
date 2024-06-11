import pandas as pd
from database_utils import DatabaseConnector

class DataExtractor():
    def read_rds_table(self, db_connector, table_name):
        # Read the table and convert to pandas dataframe
        engine = db_connector.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df
data_extractor = DataExtractor()
db_connector = DatabaseConnector()
df = data_extractor.read_rds_table(db_connector, 'legacy_users')
print(df)
#export to excel
df.to_excel('legacy_users.xlsx')