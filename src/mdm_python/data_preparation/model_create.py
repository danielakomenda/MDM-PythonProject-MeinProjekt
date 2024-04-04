import pickle
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sklearn
import sklearn.metrics
import sklearn.cross_decomposition
import sklearn.linear_model
import statsmodels
import statsmodels.api
import warnings
from statsmodels.tools.sm_exceptions import ValueWarning, ConvergenceWarning

import mdm_python.data_preparation.db_entsoe as db_entsoe

model_directory = Path("../../../data/models").resolve()


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


def load_metaparams_local():
    metaparams = dict()
    energy_types = ["solar", "nuclear", "wind", "water_river", "water_pump", "water_reservoir"]
    for type in energy_types:
        with open(model_directory/f'metaparams_{type}.pickle', 'rb') as file:
            metaparam = pickle.load(file)
            metaparams[type] = metaparam
    return metaparams


def create_production_model(raw_data):
    """
    Create model with the stored Meta-Params and the full Dataset
    Store model locally
    """
    dataset = prepare_raw_data(raw_data)
    metaparams = load_metaparams_local()
    
    for name, values in dataset.items():
        series = values.transformed_values
        params = metaparams[name]
        
        model = statsmodels.api.tsa.statespace.SARIMAX(
            series,
            trend=params["trend"],
            order=(params["p"], params["d"], params["q"]),
            seasonal_order=(1,1,0,52),
        )
        values.production_model =  model.fit(disp=False)
        print(f'Model for {name} is caluclated')
    
        model_directory.mkdir(parents=True, exist_ok=True)
        with open(model_directory/f"model_{name}.pickle", "wb") as fh:
            pickle.dump(values, fh)
        print(f'Model for {name} is stored')
      
    return dataset


if __name__ == "__main__":
    energy_data = db_entsoe.extract_daily_energy()
    create_production_model(energy_data)
