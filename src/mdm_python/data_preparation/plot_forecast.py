import base64
from io import BytesIO
from pathlib import Path

import pandas as pd
from matplotlib.figure import Figure


plot_directory = Path("../src/mdm_python/backend_server/static/pictures").resolve()


def plot_forecast(models, energy_types, forecast_horizon):
    
    plots = dict()
    forecast_dataset = dict()
    
    for name in energy_types:
        forecast_dataset[name] = models[name.lower()]
    
    for name, values in forecast_dataset.items():
        series = values.transformed_values    
        forecast = values.production_model.apply(series).get_forecast(steps=forecast_horizon)
        forecast_ci = forecast.conf_int()
        
        # Graph
        fig = Figure(figsize=(10, 6))
        ax = fig.subplots()
        ax.set_title(values.name)  # Corrected
            
        # Plot data points
        plot_start = series.index[-1] - pd.Timedelta(days=500)
        series.loc[plot_start:].plot(ax=ax, label='Observed')  # Pandas plotting
        
        # Plot predictions
        mean = pd.concat([series.iloc[-1:], forecast.predicted_mean])
        lo = pd.concat([series.iloc[-1:], forecast_ci.iloc[:, 0]])
        hi = pd.concat([series.iloc[-1:], forecast_ci.iloc[:, 1]])
        mean.plot(ax=ax, style='g', label=f'Forecast')  # Pandas plotting
        ax.fill_between(lo.index, lo, hi, color='g', alpha=0.1)
        
        ax.legend(loc='lower right')
        ax.grid(True)

        buf = BytesIO()
        fig.savefig(buf, format="png", bbox_inches='tight')  # Ensure layout is tight
        # Embed the result in the html output
        data = base64.b64encode(buf.getbuffer()).decode("ascii")

        plots[name] = f"data:image/png;base64,{data}"

    return plots



"""

def plot_forecast(raw_data, energy_types, models, forecast_horizon):
    plots = {}

    for energy_type_upper in energy_types:
        energy_type = energy_type_upper.lower()

        model = models[f"{energy_type}.pickle"]

        forecast_df = energy_forecast(raw_data, energy_type, model, forecast_horizon)
        # Plot the observed data
        fig = Figure(figsize=(10, 6))
        ax = fig.subplots()

        ax.plot(
            raw_data.index,
            raw_data[energy_type],
            label=f"Observed {energy_type}",
            color="blue",
        )

        # Plot the forecasted values
        ax.plot(
            forecast_df.index,
            forecast_df[energy_type],
            label=f"Forecasted {energy_type}",
            color="red",
        )

        # Add shades for uncertainty intervals
        ax.fill_between(
            forecast_df.index,
            forecast_df[f"lower_{energy_type}"],
            forecast_df[f"upper_{energy_type}"],
            color="lightcoral",
            alpha=0.3,
        )

        # Add labels, legend, and title
        ax.set_xlabel("Date")
        ax.set_ylabel("Value")
        ax.set_title(
            f"Observed and Forecasted Data with Confidence Intervals for {energy_type}"
        )
        ax.legend()
        ax.grid(True)

        buf = BytesIO()
        fig.savefig(buf, format="png")
        # Embed the result in the html output.
        data = base64.b64encode(buf.getbuffer()).decode("ascii")

        plots[energy_type] = f"data:image/png;base64,{data}"

    return plots

"""