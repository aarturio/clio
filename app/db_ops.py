import os

from pydantic import BaseModel
from dotenv import load_dotenv
from zzz import generate_id

load_dotenv()

import requests
from news_entity import NewsEntity
from db_config import CouchDBDatabase


class DBOps(BaseModel):

    @staticmethod
    def ingest_data(db: CouchDBDatabase):

        API_KEY = os.getenv("POLYGON_API_KEY")

        ticker = "TSLA"

        url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&order=desc&limit=1000&sort=published_utc&apiKey={API_KEY}"
        r = requests.get(url)
        data = r.json()

        batch = []

        for item in data["results"]:
            doc = NewsEntity(
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
    def get_data(db: CouchDBDatabase, ticker: str):

        query = {
            "selector": {
                "root_ticker": ticker,
            },
            "limit": 50,
            "sort": [{"published_utc": "desc"}],
        }
        output = db.get_docs(query)

        return output
