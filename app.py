from fastapi import FastAPI
import requests
from bs4 import BeautifulSoup
from difflib import get_close_matches  # For fuzzy matching

app = FastAPI()

PSX_MARKET_URL = "https://dps.psx.com.pk/market-watch"
PSX_SYMBOLS_URL = "https://dps.psx.com.pk/symbols"

def fetch_market_data():
    response = requests.get(PSX_MARKET_URL, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(response.content, "html.parser")
    
    table = soup.find("table")
    rows = table.find_all("tr")[1:]

    stock_data = {}
    
    for row in rows:
        cols = [col.text.strip() for col in row.find_all("td")]
        if len(cols) >= 11:
            symbol = cols[0]  # Stock symbol from market-watch
            stock_data[symbol] = {
                "SYMBOL": symbol,
                "SECTOR": cols[1],
                "LISTED_IN": cols[2],
                "LDCP": cols[3],
                "OPEN": cols[4],
                "HIGH": cols[5],
                "LOW": cols[6],
                "CURRENT": cols[7],
                "CHANGE": cols[8],
                "CHANGE_%": cols[9],
                "VOLUME": cols[10],
                "NAME": "",  # Default blank value
                "SECTOR_NAME": "",
                "IS_ETF": "",
                "IS_DEBT": ""
            }
    
    return stock_data

def fetch_symbol_data():
    response = requests.get(PSX_SYMBOLS_URL, headers={"User-Agent": "Mozilla/5.0"})
    return response.json()

def merge_data():
    market_data = fetch_market_data()
    symbol_data = fetch_symbol_data()

    for symbol_info in symbol_data:
        main_symbol = symbol_info["symbol"]
        close_matches = get_close_matches(main_symbol, market_data.keys(), n=1, cutoff=0.6)

        if close_matches:
            matched_symbol = close_matches[0]
            market_data[matched_symbol].update({
                "NAME": symbol_info.get("name", ""),
                "SECTOR_NAME": symbol_info.get("sectorName", ""),
                "IS_ETF": symbol_info.get("isETF", ""),
                "IS_DEBT": symbol_info.get("isDebt", "")
            })

    return {"stocks": list(market_data.values())}

@app.get("/psx-data")
def fetch_psx_data():
    return merge_data()
