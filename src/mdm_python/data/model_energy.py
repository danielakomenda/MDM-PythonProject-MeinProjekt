from pathlib import Path
from types import SimpleNamespace
import pickle

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sklearn.metrics
import sklearn.cross_decomposition
import sklearn.linear_model
import statsmodels
import statsmodels.api
import warnings


import sys
sys.path.append("/Users/missd/Desktop/6. Semester/6. Model Deployment Maintenance/MDM - Python Projekt/src/")
import mdm_python.data.db_entsoe as db_entsoe

model_directory = Path("../src/mdm_python/models").resolve()

energy_data = db_entsoe.extract_daily_energy()




def prepare_raw_data (data: pd.DataFrame) -> pd.DataFrame:
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
    
    """Create new DataFrame with the missing days in the index and NaN-Values
    Fill NaN-Values with the average between the previous and the next value"""
    index_date = pd.date_range(start=data_transformed.index[0], end=data_transformed.index[-1], freq='D')
    data_fixed = data_transformed.reindex(index_date)
    data_fixed = data_fixed.interpolate(method='linear')
    data_fixed = data_fixed.dropna()
    assert data_fixed.index.freq is not None, "Data must still be fixed-frequency"
    return data_fixed


def detrend_and_deseasonalize_data(prepared_data: pd.DataFrame) -> dict:
    """
    Detrend and deseasonlize the prepared data.
    Create a dictionnary with Name, detrended values and the baseline
    """
    
    warnings.filterwarnings("ignore", message="Series.fillna with 'method' is deprecated.*")

    dict_of_detrended_data = dict()

    # breakpoints for structural break
    breakpoints = dict(
        solar = pd.Timestamp("2020-01-01",tz=prepared_data.index.tz),
    )
    
    for col_name in prepared_data.columns:
        
        modelling_basis = prepared_data[col_name]
     
        if breakpoints.get(col_name) is not None:
            modelling_basis = modelling_basis[modelling_basis.index >= breakpoints.get(col_name)]    
        
        weekly_seasonality = statsmodels.api.tsa.seasonal_decompose(modelling_basis, period=7, model="additive")
        week_removed = modelling_basis - weekly_seasonality.seasonal
        yearly_seasonality = statsmodels.api.tsa.seasonal_decompose(week_removed, period=365, model="additive")
        trend = yearly_seasonality.trend.fillna(method="ffill").fillna(method="bfill")
        
        baseline = yearly_seasonality.seasonal + trend + weekly_seasonality.seasonal
        changed_data_deseasonalized = modelling_basis - baseline

        values = dict(
            name = col_name,
            detrended_values = changed_data_deseasonalized,
            baseline = baseline,
        )

        dict_of_detrended_data[col_name] = values
    
    return dict_of_detrended_data



def ARIMA_model(data, p, d, q):
    """
    Model the data with an ARIMA-Model; if there is seasonality, a seasonal difference is calculated
    Some warnings are ignored, since many parameters are tested for the best model
    """
    warnings.filterwarnings("ignore", message="Non-stationary starting autoregressive parameters found.")
    warnings.filterwarnings("ignore", message="Non-invertible starting MA parameters found.")
    warnings.filterwarnings("ignore", message="Maximum Likelihood optimization failed to converge.")    

    model = statsmodels.tsa.arima.model.ARIMA(data, order=(p, d, q), freq='D')
    fitted_model = model.fit()
    return fitted_model



def create_model(data):
    """
    Run the process to prepare, detrend and deseasonalize the raw data
    Create the models with the best parameters
    Store the model in the model-directory
    """

    prepared_data = prepare_raw_data(data)
    prepared_data = detrend_and_deseasonalize_data(prepared_data)
    
    parameters = dict(
        wind = SimpleNamespace(p=11, d=1, q=0),
        solar = SimpleNamespace(p=3, d=1, q=1),
        nuclear = SimpleNamespace(p=5, d=0, q=5),
        water_reservoir = SimpleNamespace(p=10, d=1, q=0),
        water_pump = SimpleNamespace(p=10, d=1, q=3),
        water_river = SimpleNamespace(p=3, d=1, q=1),
    )

    for name, data in prepared_data.items():
        param = parameters[name]
        data["model"] = ARIMA_model(data["detrended_values"], param.p, param.d, param.q)

    return prepared_data


def store_model(prepared_data):
    for name, data in prepared_data.items():
        with open(model_directory/f"{name}.pickle", "wb") as fh:
            pickle.dump(data["model"], fh)