import yaml
from sqlalchemy import create_engine
from sqlalchemy import MetaData

class DatabaseConnector():
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as f:
            creds = yaml.safe_load(f)
        return creds
    
    def init_db_engine(self):
        creds = self.read_db_creds()
        db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine
    
    def list_db_tables(self, engine):
        metadata = MetaData()
        metadata.reflect(bind=engine)
        return metadata.tables.keys()

if __name__ == "__main__": 
    db_connector = DatabaseConnector()
    engine = db_connector.init_db_engine()
    tables = db_connector.list_db_tables(engine)
    print(tables)