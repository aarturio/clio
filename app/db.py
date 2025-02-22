import os
import json

from pydantic import BaseModel, Field
from dotenv import load_dotenv
from .zzz import generate_id, convert_timestamp

load_dotenv()

import requests
import json
from typing import List, Dict, Any


class CouchDBConnector:
    def __init__(self, url: str, username: str = None, password: str = None):
        """Initialize with CouchDB server URL and optional credentials."""
        self.url = url.rstrip("/")
        self.auth = (username, password) if username and password else None
        self.session = requests.Session()
        if self.auth:
            self.session.auth = self.auth

    def database(self, db_name: str) -> "CouchDBDatabase":
        """Return a database object for the given database name."""
        return CouchDBDatabase(self, db_name)

    def create_database(self, db_name: str) -> bool:
        """Create a database if it doesn’t exist."""
        response = self.session.put(f"{self.url}/{db_name}")
        return response.status_code in (201, 202)

    def delete_database(self, db_name: str) -> bool:
        """Delete a database."""
        response = self.session.delete(f"{self.url}/{db_name}")
        return response.status_code == 200


class CouchDBDatabase:
    def __init__(self, connector: CouchDBConnector, db_name: str):
        """Initialize with a connector and database name."""
        self.connector = connector
        self.db_url = f"{connector.url}/{db_name}"

    def save_doc(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Save a single document."""
        response = self.connector.session.put(f"{self.db_url}/{doc['_id']}", json=doc)
        response.raise_for_status()
        return response.json()

    def bulk_docs(self, docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Bulk insert/update documents."""
        payload = {"docs": docs}
        response = self.connector.session.post(
            f"{self.db_url}/_bulk_docs", json=payload
        )
        response.raise_for_status()
        return response.json()

    def get_doc(self, doc_id: str) -> Dict[str, Any]:
        """Retrieve a document by ID."""
        response = self.connector.session.get(f"{self.db_url}/{doc_id}")
        response.raise_for_status()
        return response.json()


class NewsEntity(BaseModel):
    id: str = Field(..., alias="_id")
    ticker: str
    title: str
    url: str
    body: str
    published_at: str

    class Config:
        populate_by_name = True  # Allows instantiation with _id or id


class DBOps(BaseModel):

    @staticmethod
    def ingest_news(db: CouchDBDatabase):

        # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
        # API_KEY = os.getenv("API_KEY")
        # url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&apikey={API_KEY}&limit=1000"
        # r = requests.get(url)
        # data = r.json()

        batch = []

        with open("mock_data/test1.json", "r") as file:
            data = json.load(file)

        for item in data["feed"]:
            doc = NewsEntity(
                id=generate_id(item["title"], item["url"]),
                ticker="NVDA",
                title=item["title"],
                url=item["url"],
                body=item["summary"],
                published_at=convert_timestamp(item["time_published"]),
            )
            batch.append(doc.model_dump(by_alias=True))

        # with open("mock_data/test2.json", "r") as file:
        #     data = json.load(file)

        # for item in data["feed"]:
        #     new_entry = NewsEntity(
        #         id=generate_id(item["title"], item["url"]),
        #         ticker="TSLA",
        #         title=item["title"],
        #         url=item["url"],
        #         body=item["summary"],
        #         published_at=convert_timestamp(item["time_published"]),
        #     )
        #     batch.append(new_entry.model_dump())

        # with open("mock_data/test3.json", "r") as file:
        #     data = json.load(file)

        # for item in data["feed"]:
        #     new_entry = NewsEntity(
        #         id=generate_id(item["title"], item["url"]),
        #         ticker="AAPL",
        #         title=item["title"],
        #         url=item["url"],
        #         body=item["summary"],
        #         published_at=convert_timestamp(item["time_published"]),
        #     )
        #     batch.append(new_entry.model_dump())

        db.bulk_docs(batch)

    @staticmethod
    def get_news(db: CouchDBDatabase, ticker: str):
        output = [db.get(x) for x in db if db.get(x)["ticker"] == ticker]

        return output
