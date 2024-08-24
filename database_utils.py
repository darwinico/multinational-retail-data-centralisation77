import pandas as pd
import yaml
from sqlalchemy import create_engine, MetaData, text

class DatabaseConnector():
    def read_db_creds(self, yaml_file):
        with open(yaml_file, 'r') as f:
            creds = yaml.safe_load(f)
        return creds
    
    def init_db_engine(self, yaml_file):
        creds = self.read_db_creds(yaml_file)
        db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        self.engine = create_engine(db_url)
        return self.engine
    
    def list_db_tables(self):
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        return metadata.tables.keys()
    
    def upload_to_db(self, df: pd.DataFrame, table_name: str):
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)
        print("uploading", table_name, self.engine)

    def drop_table(self, table_name: str):
        with self.engine.connect() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

if __name__ == "__main__": 
    db_connector = DatabaseConnector()
    db_connector.init_db_engine('db_creds.yaml')
    tables = db_connector.list_db_tables()
    print(tables)