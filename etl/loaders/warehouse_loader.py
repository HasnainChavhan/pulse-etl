from sqlalchemy import create_engine
import pandas as pd
import os

def load_data(df, table_name, if_exists='append'):
    db_user = os.getenv('POSTGRES_USER', 'pulse_admin')
    db_password = os.getenv('POSTGRES_PASSWORD', 'pulse_secret')
    db_host = os.getenv('POSTGRES_HOST', 'localhost')
    db_name = os.getenv('POSTGRES_DB', 'pulse_dwh')
    
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}')
    
    with engine.connect() as conn:
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
        return len(df)
