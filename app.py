from fastapi import FastAPI, HTTPException
import requests
from bs4 import BeautifulSoup

app = FastAPI()

# URLs for PSX data
PSX_MARKET_URL = "https://dps.psx.com.pk/market-watch"
PSX_SYMBOLS_URL = "https://dps.psx.com.pk/symbols"

@app.get("/psx-data")
def fetch_psx_data():
    try:
        headers = {"User-Agent": "Mozilla/5.0"}

        # Fetch market data (live stock prices)
        market_response = requests.get(PSX_MARKET_URL, headers=headers, timeout=10)
        market_response.raise_for_status()
        soup = BeautifulSoup(market_response.content, "html.parser")

        # Find stock table
        table = soup.find("table")
        if not table:
            raise HTTPException(status_code=500, detail="Market data table not found on PSX website.")

        rows = table.find_all("tr")[1:]  # Skip header row
        stock_data = {}

        # Extract market data
        for row in rows:
            cols = [col.text.strip() for col in row.find_all("td")]
            if len(cols) >= 11:
                symbol = cols[0]  # SYMBOL
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
                    "VOLUME": cols[10]
                }

        # Fetch company details
        symbols_response = requests.get(PSX_SYMBOLS_URL, headers=headers, timeout=10)
        symbols_response.raise_for_status()
        company_details = symbols_response.json()

        # Merge company details with market data
        for company in company_details:
            symbol = company["symbol"]
            if symbol in stock_data:
                stock_data[symbol].update({
                    "NAME": company["name"],
                    "SECTOR_NAME": company["sectorName"],
                    "IS_ETF": company["isETF"],
                    "IS_DEBT": company["isDebt"]
                })

        return {"stocks": list(stock_data.values())}

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")
