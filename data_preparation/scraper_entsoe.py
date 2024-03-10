import asyncio
import datetime
import os
import dotenv
import re
import json

import pymongo
import httpx
import bs4
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

    if "Energie" in db.list_collection_names():
        return db["Energie"]
    else:
        collection = db["Energie"]
        collection.create_index(
            [
                ("country", pymongo.ASCENDING),
                ("datetime", pymongo.ASCENDING),
            ],
            unique=True,
        )
        return collection


async def scrape_website_data(country: str, date: datetime) -> pd.DataFrame:
    """Access the website with the needed parameters; return a PandasDataFrame"""

    # Create List with all 20 Productions-Types
    productiontypes = [("productionType.values", f"B{k:02}") for k in range(1, 21)]

    async with httpx.AsyncClient() as client:
        result = await client.get(
            url="https://transparency.entsoe.eu/generation/r2/actualGenerationPerProductionType/show",
            headers={
                "X-Requested-With": "XMLHttpRequest",
            },
            params=list(
                {
                    "viewType": "GRAPH",
                    "areaType": "CTY",
                    "dateTime.dateTime": f"{date:%d.%m.%Y} 00:00|UTC|DAYTIMERANGE",
                    "dateTime.endDateTime": f"{date:%d.%m.%Y} 00:00|UTC|DAYTIMERANGE",
                    "dateTime.timezone": "UTC",
                    "area.values": f"CTY|{country}!CTY|{country}",
                }.items()
            )
            + productiontypes,
        )

    # make sure the content is UTF-8 and parse the content with bs4
    assert result.headers["content-type"] == "text/html;charset=UTF-8", result.headers[
        "content-type"
    ]
    soup = bs4.BeautifulSoup(result.content.decode("utf-8"))

    # select only the part 'script' and the chart-list of the http-file
    javascript_str = soup.find("script").text
    match = re.search(r"var\s+chart\s*=\s*({.*})\s*;", javascript_str, re.S)
    assert match is not None

    # returns the first element of the group
    data = json.loads(match.group(1))

    # defines the columns for the dataframe
    columns = {k: " ".join(v["title"].split()) for k, v in data["graphDesign"].items()}

    df = (
        pd.DataFrame(data["chartData"])
        .set_index(data["categoryName"])
        .astype(float)
        .rename(columns=columns)
    )

    # combine time with date to get a real timestamp
    df = df.set_index(
        pd.MultiIndex.from_arrays(
            [
                [country] * df.shape[0],
                df.index.to_series()
                .apply(
                    lambda v: datetime.datetime.combine(
                        date, datetime.time.fromisoformat(v)
                    )
                )
                .dt.tz_localize("UTC"),
            ],
            names=["country", "datetime"],
        )
    )

    return df


def insert_data_to_db(collection, df):
    """Insert the data to the collection; if there is already a data-set with the same location and time,
    an Error is raised, but the rest of the inserts will carry on"""

    data = df.reset_index().to_dict("records")

    collection.insert_many(
        data,
        ordered=False,
    )


async def scraping():
    """Run the program: Scraping the website, Insert it to DB"""

    collection = connect_to_db()
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=7)
    country = "10YCH-SWISSGRIDZ"

    date = pd.date_range(start_date, end_date, freq="D")

    collected_dfs = []

    for d in date:
        print(f"Working on {d.year}-{d.month}-{d.day}")
        df = await scrape_website_data(country=country, date=d)
        collected_dfs.append(df)

    df_to_insert = pd.concat(collected_dfs)

    print("all data scraped, ready to insert in db")
    
    try:
        insert_data_to_db(collection, df_to_insert)
    except pymongo.errors.BulkWriteError as ex:
        result = dict(ex.details)
        write_errors = result.pop("writeErrors",[])
        ok = all(err.get("code") == 11000 for err in write_errors)
        ok = ok and not result.get("writeConcernErrors")
        n_success = result['nInserted']
        n_duplicate = len(write_errors)
        ok = ok and (n_success + n_duplicate) == df_to_insert.shape[0]
        if ok:
            print(f"Discarded {n_duplicate} inserts due to duplicate keys, inserted {n_success} documents.")
        else:
            had_write_concern = len(result.get("writeConcernErrors",[]))
            not_discarded = sum(err.get("code") != 11000 for err in write_errors)
            raise RuntimeError(f"Unexpected error; {n_duplicate=} {n_success=} {df_to_insert.shape[0]=} {had_write_concern=} {not_discarded=}")


if __name__ == "__main__":
    asyncio.run(scraping())
