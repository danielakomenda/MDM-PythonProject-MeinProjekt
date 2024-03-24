# cd model
# python save.py -c '***AZURE_STORAGE_CONNECTION_STRING***'

import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import argparse
import dotenv


try:
    dotenv.load_dotenv()
    azure_storage_connection_string= os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if azure_storage_connection_string is None:
        import argparse
        parser = argparse.ArgumentParser(description='Upload Model')
        parser.add_argument('-c', '--connection', required=True, help="azure storage connection string")
        args = parser.parse_args()
        azure_storage_connection_string = args.connection

    blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(args.connection)

    exists = False
    containers = blob_service_client.list_containers(include_metadata=True)
    suffix = 0
    for container in containers:
        existingContainerName = container['name']
        print(existingContainerName, container['metadata'])
        if existingContainerName.startswith("hikeplanner-model"):
            parts = existingContainerName.split("-")
            print(parts)
            if (len(parts) == 3):
                newSuffix = int(parts[-1])
                if (newSuffix > suffix):
                    suffix = newSuffix

    suffix += 1
    container_name = str("hikeplanner-model-" + str(suffix))
    print("new container name: ")
    print(container_name)

    for container in containers:            
        print("\t" + container['name'])
        if container_name in container['name']:
            print("EXISTIERTT BEREITS!")
            exists = True

    if not exists:
        # Create the container
        container_client = blob_service_client.create_container(container_name)

    local_file_name = "GradientBoostingRegressor.pkl"
    upload_file_path = os.path.join(".", local_file_name)

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
    print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

    # Upload the created file
    with open(file=upload_file_path, mode="rb") as data:
        blob_client.upload_blob(data)

except Exception as ex:
    print('Exception:')
    print(ex)
    exit(1)
