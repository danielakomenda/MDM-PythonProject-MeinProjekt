import os
import dotenv

import pymongo
import pandas as pd


def connect_to_db_Wetter_Durchschnitt():
    # Load environment variables from .env file
    dotenv.load_dotenv()
    
    # Get MongoDB-URI
    mongodb_uri = os.getenv("MONGODB_URI")
    DBclient = pymongo.MongoClient(mongodb_uri)
    db = DBclient["MDM-Python-MeinProjekt"]

    if "Wetter_Durchschnitt" in db.list_collection_names():
        return db["Wetter_Durchschnitt"]
    else:
        collection = db["Wetter_Durchschnitt"]
        collection.create_index([
            ("date", pymongo.ASCENDING),
        ], unique=True)
        return collection
    
    

def extract_daily_average_weather():

    collection = connect_to_db_Wetter_Durchschnitt()
    
    pipeline = [
      {
        '$project': {
          '_id': False,
          'date': "$date",
          'avg_temp': "$avg_temp",
          'min_temp': "$min_temp",
          'max_temp': "$max_temp",
          'rain': "$rain",
          'wind_speed': "$wind_speed",
          'clouds': "$clouds",
        },
      },
    ]
    
    
    results = []
    for x in collection.aggregate(pipeline):
        results.append(x)
    
    df = pd.DataFrame(results)
    df = df.set_index("date")
    df = df.set_index(pd.to_datetime(df.index).tz_localize("UTC"))
    df = df.sort_index()

    return df