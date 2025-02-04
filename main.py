import dotenv
import os
from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from db import DBManager, DBOps
from contextlib import asynccontextmanager
from ticker import Ticker
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize resources at startup
    db_manager = DBManager("data.db")
    DBOps.create_tables(db_manager)

    # Pass resources to the app's lifespan
    app.state.db_manager = db_manager

    try:
        yield  # App runs during this block
    finally:
        # Cleanup resources at shutdown
        db_manager.close()


app = FastAPI(lifespan=lifespan)


@app.get("/ticker/{ticker}")
def get_ticker_news(ticker: str):
    try:
        db_manager = app.state.db_manager
        output = DBOps.get_ticker_news_data(db_manager=db_manager, ticker=ticker)
        return output
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/ingest/{ticker}")
def ingest_data(ticker: str):

    try:
        ticker_data = Ticker.fetch_news(ticker)

        db_manager = app.state.db_manager
        DBOps.add_data_to_db(db_manager, ticker, ticker_data)

        return {"message": "Data ingestion completed successfully"}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/ingest_all")
def ingest_data_all():
    try:
        with open("sp500_tickers.json", "r") as file:
            data = json.load(file)

        for tckr in data["tickers"]:
            logging.info(f"Ticker: {tckr}")
            ticker_data = Ticker.fetch_news(tckr)

            db_manager = app.state.db_manager
            DBOps.add_data_to_db(db_manager, tckr, ticker_data)

        return {"message": "Data ingestion completed successfully"}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
