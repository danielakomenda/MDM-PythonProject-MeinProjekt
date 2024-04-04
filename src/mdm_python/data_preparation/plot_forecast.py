import base64
from io import BytesIO
from pathlib import Path

import pandas as pd
from matplotlib.figure import Figure


plot_directory = Path("../src/mdm_python/backend_server/static/pictures").resolve()


def plot_forecast(models:dict, energy_types:list, forecast_horizon:int):
    """
    Compute the prediction for the given forecast_horizon
    Untransform the data, so that the original values are used
    Plot the untransformed data
    """
    plots = dict()
    forecast_dataset = dict()

    for name in energy_types:
        forecast_dataset[name] = models[f"{name.lower()}.pickle"]

    for name, values in forecast_dataset.items():
        series = values.transformed_values
        forecast = values.production_model.apply(series).get_forecast(
            steps=forecast_horizon
        )
        forecast_ci = forecast.conf_int()

        current = series.iloc[-1:]

        df = pd.DataFrame(
            dict(
                mean=pd.concat([current, forecast.predicted_mean]),
                low=pd.concat([current, forecast_ci.iloc[:, 0]]),
                high=pd.concat([current, forecast_ci.iloc[:, 1]]),
            )
        )

        untransform = lambda x: x if values.offset is None else 10**x - values.offset

        # Graph
        fig = Figure(figsize=(10, 6))
        ax = fig.subplots()
        ax.set_title(values.name)

        # Plot data points
        plot_start = series.index[-1] - pd.Timedelta(days=500)
        untransform(series.loc[plot_start:]).plot(
            ax=ax, label="Observed"
        )  # Pandas plotting

        df_ut = untransform(df)

        # Plot predictions
        df_ut["mean"].plot(ax=ax, style="g", label=f"Forecast")
        ax.fill_between(df_ut.low.index, df_ut.low, df_ut.high, color="g", alpha=0.1)

        ax.legend(loc="lower right")
        ax.grid(True)
        ax.set_ylabel("MW")

        buf = BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        data = base64.b64encode(buf.getbuffer()).decode("ascii")

        plots[name] = f"data:image/png;base64,{data}"

    return plots
