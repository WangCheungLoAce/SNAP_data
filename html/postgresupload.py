import sys
import json
from sqlalchemy import create_engine
import pandas as pd

def upload_to_postgres(username, password, host, port, database_name, dataframes):
    # PostgreSQL connection string
    CONNECTION_STRING_POSTGRESQL = f"postgresql://{username}:{password}@{host}:{port}/{database_name}"
    
    # Create SQLAlchemy engine
    engine = create_engine(CONNECTION_STRING_POSTGRESQL)
    
    try:
        for table_name, dataframe in dataframes.items():
            dataframe.to_sql(table_name, con=engine, if_exists='append', index=False)
            print(f"{table_name} data inserted successfully!")
    
    except Exception as e:
        print(f"Error inserting data: {e}")
    
    finally:
        engine.dispose()

if __name__ == '__main__':
    if len(sys.argv) < 7:
        print("Usage: python postgresupload.py <username> <password> <host> <port> <database_name> <table_name1> <file_path1> [<table_name2> <file_path2> ...]")
        sys.exit(1)
    
    username = sys.argv[1]
    password = sys.argv[2]
    host = sys.argv[3]
    port = sys.argv[4]
    database_name = sys.argv[5]
    
    dataframes = {}
    for i in range(6, len(sys.argv), 2):
        table_name = sys.argv[i]
        file_path = sys.argv[i + 1]
        dataframe = pd.read_csv(file_path)
        dataframes[table_name] = dataframe
    
    # Call the function to upload dataframes to PostgreSQL
    upload_to_postgres(username, password, host, port, database_name, dataframes)
