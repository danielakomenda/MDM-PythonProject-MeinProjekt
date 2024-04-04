from pathlib import Path
from types import SimpleNamespace
import pickle
import os
import dotenv

from azure.storage.blob import BlobServiceClient
import pandas as pd
import numpy as np
import statsmodels
import statsmodels.api

import mdm_python.data_preparation.db_entsoe as db_entsoe

model_directory = Path("./data/models").resolve()


def load_metaparams_local():
    metaparams = dict()
    energy_types = ["solar", "nuclear", "wind", "water_river", "water_pump", "water_reservoir"]
    for type in energy_types:
        with open(model_directory/f'metaparams_{type}.pickle', 'rb') as file:
            metaparam = pickle.load(file)
            metaparams[type]=metaparam
    return metaparams


def prepare_raw_data(data: pd.DataFrame) -> dict:
    """
    Transform the data to log-scale with an offset to handle the high number of zeros
    The offset will change the skew-of the histogram close to 0
    """
    data = data.drop(columns="total")
    
    offset = dict(
        wind = 1.4,
        solar = 6,
        water_reservoir = 900,
        water_river = 150,
        water_pump = 700
    )
    data_transformed = data.apply(lambda col: np.log10(col+offset[col.name]) if col.name in offset else col)
    data_transformed = data_transformed.resample("W").mean()
    
    """
    Create new DataFrame with the missing days in the index and NaN-Values
    Fill NaN-Values with the average between the previous and the next value
    """
    index_date = pd.date_range(start=data_transformed.index[0], end=data_transformed.index[-1], freq='W')
    data_fixed = data_transformed.reindex(index_date)
    data_fixed = data_fixed.interpolate(method='linear')
    data_fixed = data_fixed.dropna()
    assert data_fixed.index.freq is not None, "Data must still be fixed-frequency"

    dict_of_transformed_data = dict()

    for col_name in data_fixed.columns:
        values = SimpleNamespace(
            name = col_name,
            transformed_values = data_fixed[col_name],
            offset = offset.get(col_name),
        )
        dict_of_transformed_data[col_name] = values
    
    return dict_of_transformed_data


def create_production_model(dataset):
    """
    Load the stored Meta-Parameters
    Calculate the model with the actual data
    Return production model
    """
    metaparams = load_metaparams_local()
    
    for name, values in dataset.items():
        series = values.transformed_values
        params = metaparams[name]
        values.production_model = statsmodels.api.tsa.statespace.SARIMAX(
            series,
            trend=params["trend"],
            order=(params["p"], params["d"], params["q"]),
            seasonal_order=(1,1,0,52),
        ).fit(
            disp=False,
            cov_type='none',
            full_output=False,
            low_memory=True
        )
        
        print(f"{name} is done")
    return dataset


def store_locally(dataset):
    for name, values in dataset.items():
        model_directory.mkdir(parents=True, exist_ok=True)
        with open(model_directory/f"{name}.pickle", "wb") as fh:
            pickle.dump(values, fh)
            print(f'Model for {name} is stored')


def store_to_azure():

    dotenv.load_dotenv()
    azure_storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(
        azure_storage_connection_string
    )

    """
    Create new Container with latest Suffix
    """
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
        container_client = blob_service_client.create_container(container_name)

    
    models = dict(
        nuclear_model=model_directory / "nuclear.pickle",
        solar_model=model_directory / "solar.pickle",
        water_pump_model=model_directory / "water_pump.pickle",
        water_reservoir_model=model_directory / "water_reservoir.pickle",
        water_river_model=model_directory / "water_river.pickle",
        wind_model=model_directory / "wind.pickle",
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


if __name__ == "__main__":
    energy_data = db_entsoe.extract_daily_energy()
    dataset = prepare_raw_data(energy_data)
    dataset = create_production_model(dataset)
    store_locally(dataset)
    store_to_azure()
