import json
import logging
import sys
import pickle
import os
import pathlib

import bokeh.resources
import markupsafe
import bokeh.embed
import pandas

from flask import Flask, render_template, request
import flask_caching

import mdm_python.data.db_entsoe as db_entsoe 
import mdm_python.data.db_wetter2 as db_wetter2
import mdm_python.data.plots_wetter2 as plots_wetter2
import mdm_python.data.plots_entsoe as plots_entsoe
import mdm_python.data.model_energy as model_energy


logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)


app = Flask(__name__)
cache =flask_caching.Cache(app, config={'CACHE_TYPE': 'SimpleCache'})


def run() -> None:
    app.run()


@app.get('/')
def main_page():
    return web_page('main')


@app.route('/pages/<string:page>')
def web_page(page):
    return render_template(f'{page}.html',
        resources=markupsafe.Markup(bokeh.resources.CDN.render()),
    )


@app.get('/energy-plots')
@cache.cached()
def energy():
    try:
        data_hourly = db_entsoe.extract_hourly_energy()
        data_daily = db_entsoe.extract_daily_energy()

        grouped_bar_plot = plots_entsoe.grouped_bar_plot(data_daily)
        yearly_pie_plot = plots_entsoe.yearly_pie_plot(data_daily)
        stacked_area_plot = plots_entsoe.stacked_area_plot(data_hourly=data_hourly, data_daily=data_daily)

        data = json.dumps(dict(
            plot1=bokeh.embed.json_item(grouped_bar_plot),
            plot2=bokeh.embed.json_item(yearly_pie_plot),
            plot3=bokeh.embed.json_item(stacked_area_plot),
        ))
        response = app.response_class(
            response=data,
            status=200,
            mimetype="application/json",
        )
        return flask_caching.CachedResponse(
            response=response,
            timeout=86400,
        )
    except Exception as ex:
        import traceback
        return dict(
            error=repr(ex),
            traceback=traceback.format_exc(),
        )


@app.get('/weather')
@cache.cached()
def weather():
    try:
        data = db_wetter2.extract_daily_average_weather()
        stacked_area_plot = plots_wetter2.stacked_area_plot(data)
        data = json.dumps(dict(
            plot1=bokeh.embed.json_item(stacked_area_plot),
        ))
        response = app.response_class(
            response=data,
            status=200,
            mimetype="application/json",
        )
        return flask_caching.CachedResponse(
            response=response,
            timeout=86400,
        )
    except Exception as ex:
        import traceback
        return dict(
            error=repr(ex),
            traceback=traceback.format_exc(),
        )



@app.route('/energy-prediction', methods=['POST'])
def energy_predict():
    try:
        energy_models = []
        energy_types = request.json.get('types', [])
        forecast_horizon = int(request.json.get('forecastHorizon', 1))        
        for energy_type in energy_types:
            file_path = pathlib.Path(".", "../models/", f"energy_{energy_type.lower()}.pkl")
            with open(file_path, 'rb') as fid:
                energy_model = pickle.load(fid)
                energy_models.append(energy_model)
        
        energy_data = db_entsoe.extract_daily_energy()
        energy_prediction = model_energy.energy_prediction(energy_data, energy_models, forecast_horizon)
        energy_prediction_plot = model_energy.energy_prediction_plot(energy_prediction)
        
        data = json.dumps(dict(
            energy_prediction_plot=bokeh.embed.json_item(energy_prediction_plot),
        ))
        response = app.response_class(
            response=data,
            status=200,
            mimetype="application/json",
        )
        return response

    except Exception as ex:
        import traceback
        return dict(
            error=repr(ex),
            traceback=traceback.format_exc(),
        )


"""

@app.route('/Weather-Prediction')
def weather_predict():
    try:
        file_path = pathlib.Path(".", "../model/", "weather_arima.pkl")
        with open(file_path, 'rb') as fid:
            model = pickle.load(fid)
        weather_data = db_wetter2.extract_daily_average_weather()
        prediction = energy_model.energy_prediction(weather_data, model)
        weather_prediction_plot = energy_model.energy_prediction_plot(prediction)
        
        data = json.dumps(dict(
            weather_prediction_plot=bokeh.embed.json_item(weather_prediction_plot),
        ))

        response = app.response_class(
            response=data,
            status=200,
            mimetype="application/json",
        )
        return response

    except Exception as ex:
        import traceback
        return dict(
            error=repr(ex),
            traceback=traceback.format_exc(),
        )

"""


if __name__ == "__main__":
    app.run(port=5000)
