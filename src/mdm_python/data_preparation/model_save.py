import os
import pickle
from pathlib import Path

import dotenv
from azure.storage.blob import BlobServiceClient


model_directory = Path("./data/models").resolve()


dotenv.load_dotenv()
azure_storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
blob_service_client = BlobServiceClient.from_connection_string(
    azure_storage_connection_string
)

exists = False
containers = blob_service_client.list_containers(include_metadata=True)
suffix = 0
for container in containers:
    existingContainerName = container["name"]
    if existingContainerName.startswith("energy-model"):
        parts = existingContainerName.split("-")
        newSuffix = int(parts[-1])
        if newSuffix > suffix:
            suffix = newSuffix

suffix += 1
container_name = f"energy-model-{suffix}"
print("new container name: ")
print(container_name)

for container in containers:
    print("\t" + container["name"])
    if container_name in container["name"]:
        print("EXISTIERTT BEREITS!")
        exists = True

if not exists:
    # Create the container
    container_client = blob_service_client.create_container(container_name)


models = dict(
    nuclear_model=model_directory / "model_nuclear.pickle",
    solar_model=model_directory / "model_solar.pickle",
    water_pump_model=model_directory / "model_water_pump.pickle",
    water_reservoir_model=model_directory / "model_water_reservoir.pickle",
    water_river_model=model_directory / "model_water_river.pickle",
    wind_model=model_directory / "model_wind.pickle",
)

for model, file_path in models.items():
    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=file_path
    )
    print(f"\nUploading to Azure Storage as blob:\n\t{file_path}")

    # Upload the created file
    with open(file=file_path, mode="rb") as data:
        blob_client.upload_blob(data)
