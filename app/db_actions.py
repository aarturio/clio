from zzz import generate_id, format_query
from datetime import datetime, timedelta
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

        timeframes = {
            "30d": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
            "60d": (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d"),
            "90d": (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d"),
        }

        data = {}

        for timeframe, days in timeframes.items():
            data[f"sd{timeframe}"] = sentiment_db.get_docs(
                format_query(ticker, "published_utc", days)
            )

            data[f"pd{timeframe}"] = price_db.get_docs(
                format_query(ticker, "date", days)
            )

        return data
