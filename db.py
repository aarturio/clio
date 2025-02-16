import couchdb
import os
import json

from pydantic import BaseModel
from dotenv import load_dotenv
from zzz import generate_id, convert_timestamp

load_dotenv()


class NewsEntity(BaseModel):
    _id: str
    ticker: str
    title: str
    url: str
    body: str
    published_at: str


class DBManager:
    def __init__(self):
        self.conn = couchdb.Server(os.getenv("COUCHDB_URL"))
        self.db_name = os.getenv("COUCHDB_DB_NAME")

        # Create database if not exists
        if self.db_name not in self.conn:
            self.db = self.conn.create(self.db_name)
        else:
            self.db = self.conn[self.db_name]

    def close(self):
        del self.conn[self.db_name]


class DBOps(BaseModel):

    @staticmethod
    def ingest_news(db_manager: DBManager):

        # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
        # API_KEY = os.getenv("API_KEY")
        # url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&apikey={API_KEY}&limit=1000"
        # r = requests.get(url)
        # data = r.json()

        batch = []

        with open("test1.json", "r") as file:
            data = json.load(file)

        for item in data["feed"]:
            new_entry = NewsEntity(
                _id=generate_id(item["title"], item["url"]),
                ticker="NVDA",
                title=item["title"],
                url=item["url"],
                body=item["summary"],
                published_at=convert_timestamp(item["time_published"]),
            )
            batch.append(new_entry.model_dump())

        with open("test2.json", "r") as file:
            data = json.load(file)

        for item in data["feed"]:
            new_entry = NewsEntity(
                _id=generate_id(item["title"], item["url"]),
                ticker="TSLA",
                title=item["title"],
                url=item["url"],
                body=item["summary"],
                published_at=convert_timestamp(item["time_published"]),
            )
            batch.append(new_entry.model_dump())

        with open("test3.json", "r") as file:
            data = json.load(file)

        for item in data["feed"]:
            new_entry = NewsEntity(
                _id=generate_id(item["title"], item["url"]),
                ticker="AAPL",
                title=item["title"],
                url=item["url"],
                body=item["summary"],
                published_at=convert_timestamp(item["time_published"]),
            )
            batch.append(new_entry.model_dump())

        db_manager.db.update(batch)

    @staticmethod
    def get_news(db_manager: DBManager, ticker: str):
        output = [
            db_manager.db.get(x)
            for x in db_manager.db
            if db_manager.db.get(x)["ticker"] == ticker
        ]

        return output
