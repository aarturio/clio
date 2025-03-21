from zzz import generate_id
from datetime import datetime
import requests
from entity import DataEntity, PriceEntity
from db_config import DBOperations


class Actions:

    @staticmethod
    def ingest_sentiment_data(db: DBOperations, api_key: str, ticker: str):

        url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&order=desc&limit=1000&sort=published_utc&apiKey={api_key}"
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
    def ingest_price_data(
        db: DBOperations, api_key: str, ticker: str, start_date: str, end_date: str
    ):

        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=desc&limit=50000&apiKey={api_key}"
        r = requests.get(url)
        data = r.json()

        batch = []

        for item in data["results"]:
            doc = PriceEntity(
                id=generate_id(item["t"], ticker),
                root_ticker=ticker,
                open=item["o"],
                close=item["c"],
                high=item["h"],
                low=item["l"],
                volume=item["v"],
                num_trades=item["n"],
                timestamp=item["t"],
                date=datetime.fromtimestamp(item["t"] / 1000).strftime("%Y-%m-%d"),
            )
            batch.append(doc.model_dump(by_alias=True))

        db.add_index("root_ticker")
        db.add_index("date")

        db.bulk_docs(batch)

    @staticmethod
    def get_data(
        sentiment_db: DBOperations, price_db: DBOperations, api_key: str, ticker: str
    ):

        query = {
            "selector": {
                "root_ticker": ticker,
            },
            "limit": 1000,
            "sort": [{"published_utc": "desc"}],
        }
        sentiment_data = sentiment_db.get_docs(query)

        query = {
            "selector": {
                "root_ticker": ticker,
            },
            "limit": 1000,
            "sort": [{"date": "desc"}],
        }

        price_data = price_db.get_docs(query)

        return {"sentiment_data": sentiment_data, "price_data": price_data}
