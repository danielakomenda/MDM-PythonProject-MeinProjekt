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
        forecast_dataset[name] = models[f'{name.lower()}.pickle']
    
    for name, values in forecast_dataset.items():
        series = values.transformed_values    
        forecast = values.production_model.apply(series).get_forecast(steps=forecast_horizon)
        forecast_ci = forecast.conf_int()
        
        # Graph
        fig = Figure(figsize=(10, 6))
        ax = fig.subplots()
        ax.set_title(values.name)
            
        # Plot data points
        plot_start = series.index[-1] - pd.Timedelta(days=1000)
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
        fig.savefig(buf, format="png", bbox_inches='tight')
        data = base64.b64encode(buf.getbuffer()).decode("ascii")

        plots[name] = f"data:image/png;base64,{data}"

    return plots