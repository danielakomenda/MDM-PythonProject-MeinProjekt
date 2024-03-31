import os
from azure.storage.blob import BlobServiceClient
from pathlib import Path
import dotenv


model_directory = Path("../models").resolve()

try:
    dotenv.load_dotenv()
    azure_storage_connection_string= os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)

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


    models = dict(
        nuclear_model = model_directory/"nuclear.pickle",
        solar_model = model_directory/"solar.pickle",
        water_pump_model = model_directory/"water_pump.pickle",
        water_reservoir_model = model_directory/"water_reservoir.pickle",
        water_river_model = model_directory/"water_river.pickle",
        wind_model = model_directory/"wind.pickle",
    )
    
    
    for model, file_path in models:
        upload_file_path = os.path.join(".", file_path)

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path)
        print("\nUploading to Azure Storage as blob:\n\t" + file_path)

        # Upload the created file
        with open(file=upload_file_path, mode="rb") as data:
            blob_client.upload_blob(data)

except Exception as ex:
    print('Exception:')
    print(ex)
    exit(1)
