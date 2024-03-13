import os
import dotenv

import pymongo
import pandas as pd


def connect_to_db():
    """Open the connection to the DB and return the collection
    Create collection with unique index, if there is not yet one"""
    # Load environment variables from .env file
    dotenv.load_dotenv()

    # Get MongoDB-URI
    mongodb_uri = os.getenv("MONGODB_URI")
    DBclient = pymongo.MongoClient(mongodb_uri)
    db = DBclient["MDM-Python-MeinProjekt"]

    return db["Energie"]


def extract_daily_average_energy():

    collection = connect_to_db()

    pipeline = [
        {"$addFields": {"date": {"$substr": ["$datetime", 0, 10]}}},
        {
            "$group": {
                "_id": "$date",
                "wind_power": {"$avg": "$Wind Onshore Generation"},
                "solar_power": {"$avg": "$Solar Generation"},
                "nuclear_power": {"$avg": "$Nuclear Generation"},
                "water_reservoir_power": {"$avg": "$Hydro Water Reservoir Generation"},
                "water_river_power": {"$avg": "$Hydro Run-of-river and poundage Generation"},
                "water_pump_power": {"$avg": "$Hydro Pumped Storage Generation"},
            }
        },
    ]

    results = []
    for x in collection.aggregate(pipeline):
        results.append(x)

    df = pd.DataFrame(results)
    df = df.set_index(("_id"))
    df = df.set_index(pd.to_datetime(df.index).rename("date").tz_localize("UTC"))
    df = df.sort_index()
    df["total"] = df.sum(axis="columns")

    return df


def extract_daily_raw_energy():
    collection = connect_to_db()

    projection = {
        "_id": False,
        "datetime": "$datetime",
        "wind": "$Wind Onshore Generation",
        "solar": "$Solar Generation",
        "nuclear": "$Nuclear Generation",
        "water_reservoir": "$Hydro Water Reservoir Generation",
        "water_river": "$Hydro Run-of-river and poundage Generation",
        "water_pump": "$Hydro Pumped Storage Generation",
    }

    results = collection.find(projection=projection)

    df = pd.DataFrame(results)
    df = df.set_index("datetime")
    df = df.set_index(pd.to_datetime(df.index))
    df = df.sort_index()
    df["total"] = df.sum(axis="columns")

    return df
