import sys
import pandas as pd
from io import StringIO
from azure.storage.blob import BlobServiceClient

def upload_to_azure(account_name, account_key, container_name, file_name, uploaded_file):
    # Azure connection to container
    CONNECTION_STRING_AZURE_STORAGE = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
    CONTAINER_AZURE = container_name

    # Read uploaded CSV file into a DataFrame
    uploaded_df = pd.read_csv(uploaded_file)
    
    # Convert DataFrame to CSV data (string)
    csv_data = uploaded_df.to_csv(index=False)
    
    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING_AZURE_STORAGE)
    container_client = blob_service_client.get_container_client(CONTAINER_AZURE)
    
    # Upload CSV data directly to Azure Blob Storage with specified file name
    blob_client = container_client.get_blob_client(blob=file_name)
    blob_client.upload_blob(csv_data, overwrite=True)
    
    # List all blobs in the specified container (for verification)
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        if blob.name == file_name:
            blob_client = container_client.get_blob_client(blob=blob.name)
            blob_data = blob_client.download_blob()
            blob_content = blob_data.readall().decode('utf-8')
            df = pd.read_csv(StringIO(blob_content))
            return df.info()

if __name__ == '__main__':
    if len(sys.argv) < 5:
        print("Usage: python azureupload.py <account_name> <account_key> <container_name> <file_name>")
        sys.exit(1)
    
    account_name = sys.argv[1]
    account_key = sys.argv[2]
    container_name = sys.argv[3]
    file_name = sys.argv[4]
    
    # Call the function to upload DataFrame to Azure Blob Storage
    upload_to_azure(account_name, account_key, container_name, file_name, sys.stdin)
