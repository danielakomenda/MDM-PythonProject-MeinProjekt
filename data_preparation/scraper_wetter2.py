import asyncio
import datetime
import os

import dotenv
import pymongo
import httpx
import bs4
import pandas as pd


# 36 Locations in Switzerland
locations = {
    "Aarau,Switzerland": "5a5ecc58df562835e3fcbae5f8c52e64e7247918",
    "Appenzell,Switzerland": "162a2967db482d9de5e1db7b98c7fa5a779f2875",
    "Basel,Switzerland": "e502a0a8fc421e6a8f9f1c4034f6b46ff6f59f62",
    "Bellinzona,Switzerland": "f398035eff9921de0368cc494578468b1d5c99ad",
    "Bern,Switzerland": "94fa2a5396a4723b31142ab413d3ec1be77d62d8",
    "Chur,Switzerland": "6c0da286ee836415b66b138d6f13076fbcdb3899",
    "Davos,Switzerland": "e260b1b791f27abac6a4f7771a2401be6aee67a9",
    "Delemont,Switzerland": "f661d8d34fbb2431d6f02a9613c09a3782655d55",
    "Einsiedeln,Switzerland": "14ee1f6a8ff571616de7f378af94b0a588c67bda",
    "Frauenfeld,Switzerland": "8bf573628b0fd96382f03bef84c063f3aa481e21",
    "Fribourg,Switzerland": "f5240fc4fcffbe2c1680ee6f026349a7c2c0da48",
    "Geneva,Switzerland": "eacb20159164fa15c55f87d7d66068b4b2ceaf39",
    "Glarus,Switzerland": "dcb4a71fe438a4165ee26a4d370d259a2c5896d0",
    "Grindelwald,Switzerland": "ab1c057337ed2fca115ed0e16b4ec2467466aaad",
    "La_Chaux_De_Fonds,Switzerland": "d78dc7da556213f33175d58cc6d1de8534ac3a6c",
    "Laax,Switzerland": "eb8dfc2322338b344ef7f8f1063e01f91b555137",
    "Lausanne,Switzerland": "fb15d1e5d748ce024a944f59f9bf651919032bd1",
    "Lauterbrunnen,Switzerland": "8a9e5db01b10728c85ac4944d19e79e515f3deba",
    "Lenzerheide,Switzerland": "73330944ee4dbe329e5f8cfbf7bc49a0f420ba2f",
    "Leukerbad,Switzerland": "7d2f2e013101f38faf031a38c5d9b348aeb883e3",
    "Lucerne,Switzerland": "470964fac0430b6083c52ddd3e2a400c542e0e60",
    "Lugano,Switzerland": "f9bc6e13323ac7547a608fa227ff7e274284d1f2",
    "Montreux,Switzerland": "0034c59b5b977acd17fe5837e5845ae9f41e5a09",
    "Neuchatel,Switzerland": "684590561854da6d27a36d9e659cc4739f675b1c",
    "Romanshorn,Switzerland": "39fe927062dc9f07e6b3abed0189caf29b9745fd",
    "Saas_Fee,Switzerland": "865c64f03112f377ad70f301a218110eefa322b8",
    "Sarnen,Switzerland": "8a7c9fe3ae4cbce26ca0d3447542641f60ef781c",
    "Savognin,Switzerland": "8bb604df8ccd428e2f37e5c4239c93f4dc55f19f",
    "Schaffhausen,Switzerland": "8b5f38ff71ef8dcc2b857f39361149bb6193e4c3",
    "Sion,Switzerland": "f092059ab917cc1a9a335215a1f4744944feaec3",
    "Solothurn,Switzerland": "a6e3ca1b9fedf679fcc41159f5f5a56a58ca7354",
    "St_Gallen,Switzerland": "c6a7895be45659cd932d951b975b522d5964f9af",
    "Tenero,Switzerland": "cb4313847fec5d64e08e0549340a914da569db0f",
    "Thun,Switzerland": "4e567234358d307fb77c5cb5514150df3cd59a3c",
    "Verbier,Switzerland": "d9ab4f99f16929d6a5dea4a9b3f20d60af8dd3f9",
    "Zurich,Switzerland": "be1ac363913afba07be684e70dcbb7b7dcfd2ba1",
}


def connect_to_db():
    """Open the connection to the DB and return the collection
    Create collection with unique index, if there is not yet one"""

    # Load environment variables from .env file
    dotenv.load_dotenv()

    # Get MongoDB-URI
    mongodb_uri = os.getenv("MONGODB_URI")
    DBclient = pymongo.MongoClient(mongodb_uri)
    db = DBclient["MDM-Python-MeinProjekt"]

    if "Wetter" in db.list_collection_names():
        return db["Wetter"]
    else:
        collection = db["Wetter"]
        # Create unique indexes based on the combination of location and datetime
        collection.create_index(
            [
                ("location", pymongo.ASCENDING),
                ("datetime", pymongo.ASCENDING),
            ],
            unique=True,
        )
        return collection


async def scrape_website_data(
    locations, year: int, month: int, day: int
) -> pd.DataFrame:
    """Access the website with the needed parameters; return a PandasDataFrame"""

    results = []

    for location, authority in locations.items():

        async with httpx.AsyncClient() as client:
            result = await client.post(
                url="https://www.wetter2.com/v1/past-weather/",
                headers={
                    "X-Requested-With": "XMLHttpRequest",
                    "Authority": authority,
                },
                data={
                    "place": location,
                    "day": day,
                    "month": month,
                    "city": location.split(",")[0].replace("_", " "),
                    "country": location.split(",")[1],
                    "language": "german",
                },
            )
            result.raise_for_status()
            result_json = result.json()
            result_years = result_json["data"]["years"]

            # Error, if the result is not in the form of a dictionary
            if not isinstance(result_years, dict):
                raise ValueError(
                    f"Cannot parse data for {day=} {month=}: {str(result_years)[:50]}..."
                )

            for k, v in result_years.items():
                if int(k) != year:
                    continue
                date = datetime.date(year=year, month=month, day=day)
                if v.get("table") is None:
                    continue
                res_table = v["table"]

                soup = bs4.BeautifulSoup(res_table)
                head = soup.table.thead

                # Create Index
                timestamps = []
                for td in head.find_all("td"):
                    dt = datetime.datetime.combine(
                        date, datetime.time.fromisoformat(td.text)
                    )
                    dt = pd.Timestamp(dt).tz_localize("UTC")
                    timestamps.append(dt)
                index = pd.MultiIndex.from_frame(
                    pd.DataFrame(data={"location": location, "datetime": timestamps})
                )

                # Get the data of the html-body and create a dictionary with Temperature, Rain, Wind and Cloudiness
                body = soup.table.tbody
                data = dict(
                    temp_C=[
                        float(span["data-temp"])
                        for span in body.find(
                            "th", string="Temperatur"
                        ).parent.find_all("span", class_="day_temp")
                    ],
                    rain_mm=[
                        float(span["data-length"])
                        for span in body.find(
                            "th", string="Niederschlag"
                        ).parent.find_all("span", attrs={"data-length": True})
                    ],
                    wind_kmh=[
                        float(span["data-wind"])
                        for span in body.find("th", string="Wind").parent.find_all(
                            "span", class_="day_wind"
                        )
                    ],
                    cloud_percent=[
                        float(td.text.strip("%"))
                        for td in body.find("th", string="Wolkendecke").parent.find_all(
                            "td"
                        )
                    ],
                )

                result = pd.DataFrame(data=data, index=index)
                results.append(result)

    if results:
        # Concate the list-entries to a Dataframe
        return pd.concat(results)

    else:
        # Return empty dataframe, if there is no data
        data = dict(
            location=pd.Series([], dtype=str),
            datetime=pd.Series([], dtype="M8[ns]"),  # M8 is Timestamp
            temp_C=pd.Series([], dtype=float),
            rain_mm=pd.Series([], dtype=float),
            wind_kmh=pd.Series([], dtype=float),
            cloud_percent=pd.Series([], dtype=float),
        )
        return pd.DataFrame(data=data).set_index(["location", "datetime"])


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
    start_date = end_date - datetime.timedelta(days=3)

    date = pd.date_range(start_date, end_date, freq="D")

    collected_dfs = []

    for d in date:
        print(f"Working on {d.year}-{d.month}-{d.day}")
        df = await scrape_website_data(
            locations=locations, year=d.year, month=d.month, day=d.day
        )
        collected_dfs.append(df)

    df_to_insert = pd.concat(collected_dfs)

    print("all data scraped, ready to insert in db")
    insert_data_to_db(collection, df_to_insert)


if __name__ == "__main__":
    asyncio.run(scraping())
