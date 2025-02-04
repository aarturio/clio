from pydantic import BaseModel
import requests
from dotenv import load_dotenv
import os
import json

load_dotenv()


class Ticker(BaseModel):

    def fetch_news(ticker: str):
        # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
        API_KEY = os.getenv("API_KEY")
        url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&apikey={API_KEY}&limit=1000"
        r = requests.get(url)
        data = r.json()

        # with open("test.json", "r") as file:
        #     data = json.load(file)

        return data
