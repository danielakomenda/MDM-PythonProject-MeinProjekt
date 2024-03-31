import json
import logging
import sys
import pickle
import os
from pathlib import Path

import dotenv
import bokeh.resources
import markupsafe
import bokeh.embed
import pandas
from azure.storage.blob import BlobServiceClient

from flask import Flask, render_template, request
import flask_caching

import mdm_python.data.db_entsoe as db_entsoe
import mdm_python.data.plots_entsoe as plots_entsoe
import mdm_python.data.plot_forecast as plot_forecast


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


def load_models():    
    azure_storage_connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)

    containers = blob_service_client.list_containers(include_metadata=True)
    suffix = 0
    for container in containers:
        existingContainerName = container['name']
        if existingContainerName.startswith("energy-model"):
            parts = existingContainerName.split("-")
            newSuffix = int(parts[-1])
            if newSuffix > suffix:
                suffix = newSuffix

    container_name = f"energy-model-{suffix}"
    print(f'loading from container: {container_name}')


    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs()

    models = dict()
    for blob in blob_list:
        print("\t" + blob.name)
        content = container_client.download_blob(blob.name).readall()
        model = pickle.loads(content)
        models[blob.name]=model["model"]

    print(models)
    return models



@app.route('/energy-prediction', methods=['POST', 'GET'])
def energy_predict():
    energy_data = db_entsoe.extract_daily_energy()
    energy_types = request.json.get('types', [])
    forecast_horizon = int(request.json.get('forecastHorizon', 1)) 
    energy_models = load_models()         

    plots = plot_forecast.plot_forecast(energy_data, energy_types, energy_models, forecast_horizon)
    
    return plots


if __name__ == "__main__":  
    app.run(port=5000)
