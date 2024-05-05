import pandas as pd
import zipfile
import os
import shutil
import io
import requests
import json
import time
import glob
from io import StringIO
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from sqlalchemy import create_engine
from geopy.geocoders import Nominatim
from tqdm import tqdm

# URL of the zip file
zip_url = "https://www.fns.usda.gov/sites/default/files/resource-files/historical-snap-retailer-locator-data-2023.12.31.zip"
config_file_path = 'config.json'

def download_and_extract_zip(url):
    try:
        # Download the zip file
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Raise HTTPError for bad responses

        # Read the zip file contents
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            dfs = []  # List to store DataFrames from individual CSV files
            for filename in zf.namelist():
                if filename.lower().endswith('.csv'):
                    # Read the CSV file into a Pandas DataFrame
                    with zf.open(filename) as f:
                        df = pd.read_csv(f)
                        dfs.append(df)

            # Combine all DataFrames into a single DataFrame
            raw_df = pd.concat(dfs, ignore_index=True)
            return raw_df

    except Exception as e:
        print(f"Error downloading and extracting zip file: {e}")
        return None

def update_lat_long(row):
    if row['Latitude'] == 0 or row['Longitude'] == 0:
        address = (
            f"{row['Street Number']} {row['Street Name']}, "
            f"{row['City']}, {row['State']} {row['Zip Code']}"
        )
        try:
            geolocator = Nominatim(user_agent="my_geocoder")
            location = geolocator.geocode(address, timeout=10)
            if location:
                row['Latitude'] = location.latitude
                row['Longitude'] = location.longitude
        except Exception as e:
            print(f"Error geocoding address '{address}': {e}")
    return row

def upload_to_azure_storage(dataframe, connection_string, container_name, blob_name):
    try:
        csv_data = dataframe.to_csv(index=False)
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        upload_azure = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        upload_azure.upload_blob(csv_data, overwrite=True)
        print(f"Uploaded {blob_name} to Azure Blob Storage successfully.")
    except Exception as e:
        print(f"Error uploading {blob_name} to Azure Blob Storage: {e}")

def insert_into_postgresql(dataframe, table_name, engine):
    try:
        dataframe.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"Inserted {table_name} data into PostgreSQL successfully.")
    except Exception as e:
        print(f"Error inserting {table_name} data into PostgreSQL: {e}")

def main():
    try:
        # Download and extract data from the zip file
        raw_df = download_and_extract_zip(zip_url)
        if raw_df is None:
            return
        
        # Prompt user to update latitude and longitude
        proceed = input("Do you want to update latitude and longitude where necessary? (yes/no): ").lower()
        if proceed == "yes":
            tqdm.pandas()
            raw_df = raw_df.progress_apply(update_lat_long, axis=1)
            print("Latitude and Longitude values updated where necessary.")

        # Clean and prepare DataFrame
        raw_df = raw_df[(raw_df['Latitude'] != 0) & (raw_df['Longitude'] != 0)]
        raw_df[raw_df.columns] = raw_df[raw_df.columns].astype(str).apply(lambda x: x.str.strip())
        raw_df['Street Number'] = pd.to_numeric(raw_df['Street Number'], errors='coerce')
        raw_df = raw_df.dropna(subset=['Street Number', 'Street Name', 'City', 'State', 'Zip Code'], how="any")
        raw_df["Authorization Date"] = pd.to_datetime(raw_df["Authorization Date"], errors="coerce")
        raw_df["End Date"] = pd.to_datetime(raw_df["End Date"], errors="coerce")
        raw_df["Authorization Year"] = raw_df["Authorization Date"].dt.year

        # Load configuration from JSON file
        with open(config_file_path, 'r') as config_file:
            config = json.load(config_file)
        
        # Upload to Azure Blob Storage
        azure_connection_string = config["azure_connection_string"]
        container_name_azure = config["container_name"]
        blob_name_azure = 'snap_retailer_data.csv'
        upload_to_azure_storage(raw_df, azure_connection_string, container_name_azure, blob_name_azure)

        # Insert into PostgreSQL database
        postgresql_connection_string = config["postgresql_connection_string"]
        engine = create_engine(postgresql_connection_string)
        insert_into_postgresql(raw_df, 'fact_snap', engine)
        insert_into_postgresql(raw_df[['Record ID', 'Store Name', 'Authorization Year', 'Authorization Date', 'End Date']], 'dim_store', engine)
        insert_into_postgresql(raw_df[['Store Type']].drop_duplicates(), 'dim_storetype', engine)
        insert_into_postgresql(raw_df[['Record ID', 'Street Number', 'Street Name', 'Additional Address', 'City', 'State', 'Zip Code', 'Zip4', 'County']].drop_duplicates(), 'dim_address', engine)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()