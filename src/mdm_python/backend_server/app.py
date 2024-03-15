import json
import logging
import sys
import pickle
import os

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
import mdm_python.data.plots_prediction as plots_prediction


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


"""


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

        """


if __name__ == "__main__":
    app.run(port=5000)
