import os

from pydantic import BaseModel
from dotenv import load_dotenv
from zzz import generate_id
from datetime import datetime

load_dotenv()

import requests
from entity import DataEntity, PriceEntity
from db_config import CouchDBDatabase


class DBOps(BaseModel):

    @staticmethod
    def ingest_news_data(db: CouchDBDatabase):

        API_KEY = os.getenv("POLYGON_API_KEY")

        ticker = "TSLA"

        url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&order=desc&limit=1000&sort=published_utc&apiKey={API_KEY}"
        r = requests.get(url)
        data = r.json()

        batch = []

        for item in data["results"]:
            doc = DataEntity(
                hash=generate_id(item["title"], item["article_url"]),
                root_ticker=ticker,
                id=item["id"],
                publisher=item["publisher"],
                title=item["title"],
                author=item["author"],
                published_utc=item["published_utc"],
                article_url=item["article_url"],
                tickers=item["tickers"],
                image_url=item["image_url"],
                description=item["description"],
                keywords=item["keywords"],
                insights=item["insights"],
            )
            batch.append(doc.model_dump(by_alias=True))

        db.add_index("root_ticker")
        db.add_index("published_utc")

        db.bulk_docs(batch)

    @staticmethod
    def get_price_data(start_date: str, end_date: str):

        API_KEY = os.getenv("POLYGON_API_KEY")

        ticker = "TSLA"

        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&limit=120&apiKey={API_KEY}"
        r = requests.get(url)
        data = r.json()

        return data

    @staticmethod
    def get_agg_data(db: CouchDBDatabase, ticker: str):

        query = {
            "selector": {
                "root_ticker": ticker,
            },
            "limit": 50,
            "sort": [{"published_utc": "desc"}],
        }
        news_data = db.get_docs(query)

        start_date = news_data["docs"][-1]["published_utc"]
        end_date = news_data["docs"][0]["published_utc"]

        start_date_str = datetime.fromisoformat(start_date).date().isoformat()
        end_date_str = datetime.fromisoformat(end_date).date().isoformat()

        price_data = DBOps.get_price_data(start_date_str, end_date_str)

        return {"price_data": price_data, "news_data": news_data}
