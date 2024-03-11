from flask import Flask, render_template, request

import json
import logging
import sys
import pickle
import os

import bokeh.resources
import markupsafe
import bokeh.embed
import pandas

from data.db_entsoe import *
from data.db_wetter2 import *
from mdm_python.data.plots_wetter2 import *
from mdm_python.data.plots_entsoe import *
from mdm_python.data.plots_prediction import *


logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)


app = Flask(__name__)

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


@app.get('/pages/model')
async def weather_features():
    weather_table = pd.read_csv(app.root_path/"assets/weatherfeatures.csv").to_html(index=False)
    energy_table = pd.read_csv(app.root_path / "assets/energyfeatures.csv").to_html(index=False)
    x_weights_table = pd.read_csv(app.root_path / "assets/x_weights.csv").to_html(index=False)
    y_weights_table = pd.read_csv(app.root_path / "assets/y_weights.csv").to_html(index=False)


    return await render_template(
        "model.html",
        resources=markupsafe.Markup(bokeh.resources.CDN.render()),
        weather_table_html=markupsafe.Markup(weather_table),
        energy_table_html=markupsafe.Markup(energy_table),
        x_weights_table_html=markupsafe.Markup(x_weights_table),
        y_weights_table_html=markupsafe.Markup(y_weights_table),
    )

@app.get('/Historic-Energy-Production')
@cache.cached()
async def energy_historic():
    try:
        raw_result = await extract_energy_data_raw(app_state)
        daily_result = await extract_energy_data_daily(app_state)

        # energy_historic_grouped_barplot = energy_grouped_bar_plot(daily_result)
        energy_historic_plot = energy_overview_plot(raw_result=raw_result, daily_result=daily_result)
        energy_historic_pie = energy_yearly_pieplot(daily_result)

        data = json.dumps(dict(
            # plot1=bokeh.embed.json_item(energy_historic_grouped_barplot),
            plot2=bokeh.embed.json_item(energy_historic_plot),
            plot3=bokeh.embed.json_item(energy_historic_pie),
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


@app.get('/Historic-Weather-Data')
@cache.cached()
async def weather_historic():
    try:
        result = await extract_data_daily(app_state)
        weather_historic_plot = weather_overview_plot(result)
        data = json.dumps(dict(
            plot1=bokeh.embed.json_item(weather_historic_plot),
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


@app.get('/Energy-Prediction')
async def energy_predict():
    try:
        lat = float(quart.request.args["lat"])
        lon = float(quart.request.args["lon"])

        result = await extract_predictions_daily(app_state, lon=lon, lat=lat)
        features = prepare_prediction_features(result, lat)
        prediction = energy_prediction(app_state.model, features)

        energy_prediction_plot = energy_prediction_interactive_plot(prediction)
        energy_prediction_pie = energy_prediction_pieplot(prediction)

        data = json.dumps(dict(
            energy_prediction_plot1=bokeh.embed.json_item(energy_prediction_plot),
            energy_prediction_plot2=bokeh.embed.json_item(energy_prediction_pie),
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


@app.get('/Weather-Prediction')
async def weather_predict():
    try:
        lat = float(quart.request.args["lat"])
        lon = float(quart.request.args["lon"])
        result = await extract_predictions_daily(app_state, lon=lon, lat=lat)
        weather_prediction_plot = weather_prediction_interactive_plot(result)
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



if __name__ == "__main__":
    app.run(port=5000)
