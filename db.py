import duckdb
from pydantic import BaseModel
import json
from typing import List, Dict
from ticker import Ticker
from zzz import generate_id, convert_timestamp
import uuid
import logging


class DBManager:
    def __init__(self, database_path=":memory:"):
        self.conn = duckdb.connect(database_path)

    def close(self):
        self.conn.close()


class DBOps(BaseModel):

    @staticmethod
    def create_tables(db_manager: DBManager):

        # db_manager.conn.execute("DROP TABLE IF EXISTS news")
        db_manager.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS news (
                id TEXT PRIMARY KEY,
                root_ticker TEXT,
                title TEXT,
                url TEXT,
                time_published TIMESTAMP,
                authors TEXT,
                summary TEXT,
                banner_image TEXT,
                source TEXT,
                category_within_source TEXT,
                source_domain TEXT,
                overall_sentiment_score DOUBLE,
                overall_sentiment_label TEXT
            )
            """
        )

        # db_manager.conn.execute("DROP TABLE IF EXISTS topics")
        db_manager.conn.execute(
            """
                CREATE TABLE IF NOT EXISTS topics (
                    id TEXT,
                    topic_id TEXT,
                    topic TEXT,
                    relevance_score DOUBLE,
                    PRIMARY KEY (id, topic_id)
                )
            """
        )

        # db_manager.conn.execute("DROP TABLE IF EXISTS ticker_sentiment")
        db_manager.conn.execute(
            """
                CREATE TABLE IF NOT EXISTS ticker_sentiment (
                    id TEXT,
                    ticker_sentiment_id TEXT,
                    ticker TEXT,
                    relevance_score DOUBLE,
                    ticker_sentiment_score DOUBLE,
                    ticker_sentiment_label TEXT,
                    PRIMARY KEY (id, ticker_sentiment_id)
                )
            """
        )

    @staticmethod
    def get_ticker_news_data(db_manager: DBManager, ticker: str) -> dict:

        query = f"SELECT * FROM news WHERE root_ticker = ?"
        cursor = db_manager.conn.execute(query, (ticker,))
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()

        response = [dict(zip(columns, row)) for row in rows]
        output = {"data": response}

        return output

    @staticmethod
    def add_data_to_db(db_manager: DBManager, ticker: str, data: Dict):
        news_data = data["feed"]
        for news_item in news_data:

            news_id = generate_id(news_item["title"], news_item["url"])

            # Check if the news_id already exists in the news table
            check_id = db_manager.conn.execute(
                "SELECT 1 FROM news WHERE id = ?", (news_id,)
            )
            if check_id.fetchone():
                logging.warning(
                    f"Item with id {news_id} already exists in the database."
                )
                continue

            # Convert timestamp before insertion
            formatted_timestamp = convert_timestamp(news_item["time_published"])

            # Insert into news table
            db_manager.conn.execute(
                """
                INSERT INTO news (
                    id, root_ticker, title, url, time_published, authors, summary, banner_image, source,
                    category_within_source, source_domain, overall_sentiment_score, overall_sentiment_label
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    news_id,
                    ticker,
                    news_item["title"],
                    news_item["url"],
                    formatted_timestamp,
                    ", ".join(news_item["authors"]),
                    news_item["summary"],
                    news_item["banner_image"],
                    news_item["source"],
                    news_item["category_within_source"],
                    news_item["source_domain"],
                    news_item["overall_sentiment_score"],
                    news_item["overall_sentiment_label"],
                ),
            )

            # Insert into topics table
            for topic in news_item["topics"]:
                topic_id = uuid.uuid4().hex
                db_manager.conn.execute(
                    """
                    INSERT INTO topics (id, topic_id, topic, relevance_score) VALUES (?, ?, ?, ?)
                    """,
                    (news_id, topic_id, topic["topic"], topic["relevance_score"]),
                )

            # Insert into ticker_sentiment table
            for ticker_sentiment in news_item["ticker_sentiment"]:
                ticker_sentiment_id = uuid.uuid4().hex
                db_manager.conn.execute(
                    """
                    INSERT INTO ticker_sentiment (id, ticker_sentiment_id, ticker, relevance_score, ticker_sentiment_score, ticker_sentiment_label) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        news_id,
                        ticker_sentiment_id,
                        ticker_sentiment["ticker"],
                        ticker_sentiment["relevance_score"],
                        ticker_sentiment["ticker_sentiment_score"],
                        ticker_sentiment["ticker_sentiment_label"],
                    ),
                )

        db_manager.conn.commit()
