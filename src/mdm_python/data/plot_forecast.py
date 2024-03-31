import datetime
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


import mdm_python.data.db_entsoe as db_entsoe
import mdm_python.data.create_model as energy_model


model_directory = Path("../data/models").resolve()
plot_directory = Path("../src/mdm_python/backend_server/static/pictures").resolve()

def energy_forecast(raw_data, energy_type, model, forecast_horizon):

    data_set = energy_model.prepare_raw_data(raw_data)
    data_set = energy_model.detrend_and_deseasonalize_data(data_set)
    
    p, d, q = model["model"].order
    
    modelling = energy_model.ARIMA_model(data_set[energy_type].detrended_values, p, 0, q)
    forecast = modelling.get_forecast(steps=forecast_horizon).summary_frame()

    baseline = data_set[energy_type].baseline.iloc[-1]

    offset = data_set[energy_type].offset
    untransform = lambda v: 10**(v-offset) if offset is not None else v
    
    forecast_df = pd.DataFrame(index=forecast.index, data={
        energy_type:untransform(forecast["mean"] + baseline),
        f"lower_{energy_type}":untransform(forecast["mean_ci_lower"] + baseline),
        f"upper_{energy_type}":untransform(forecast["mean_ci_upper"] + baseline),
    })
    
    return forecast_df


def plot_forecast(raw_data, energy_types, models, forecast_horizon):
    plot_paths = {}

    for energy_type_upper in energy_types:
        energy_type = energy_type_upper.lower()
        
        model = models[f'{energy_type}.pickle']
        
        forecast_df = energy_forecast(raw_data, energy_type, model, forecast_horizon)
        # Plot the observed data
        plt.figure(figsize=(10, 6))
        plt.plot(raw_data.index, raw_data[energy_type], label=f'Observed {energy_type}', color='blue')
        
        # Plot the forecasted values
        plt.plot(forecast_df.index, forecast_df[energy_type], label=f'Forecasted {energy_type}', color='red')
        
        # Add shades for uncertainty intervals
        plt.fill_between(forecast_df.index, forecast_df[f'lower_{energy_type}'], forecast_df[f'upper_{energy_type}'], color='lightcoral', alpha=0.3)
        
        # Add labels, legend, and title
        plt.xlabel('Date')
        plt.ylabel('Value')
        plt.title(f'Observed and Forecasted Data with Confidence Intervals for {energy_type}')
        plt.legend()
        plt.grid(True)

        # Define the path for saving the plot
        plot_path = plot_directory/f"{energy_type}_forecast.png"

        # Save the plot
        plt.savefig(plot_path)
        plt.close()  # Important to free memory

        # Store the relative or absolute path in the dictionary
        plot_paths[energy_type] = plot_path

    return plot_paths


if __name__ == "__main__":
    energy_data = db_entsoe.extract_daily_energy()
    plot_forecast(energy_data)