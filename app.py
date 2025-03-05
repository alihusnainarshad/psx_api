import sqlite3
import time
import requests
import threading
from fastapi import FastAPI, Query
from bs4 import BeautifulSoup
from difflib import get_close_matches
from datetime import datetime

app = FastAPI()

PSX_MARKET_URL = "https://dps.psx.com.pk/market-watch"
PSX_SYMBOLS_URL = "https://dps.psx.com.pk/symbols"
DB_NAME = "psx_data.db"

# === Initialize SQLite Database ===
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_data (
            SYMBOL TEXT PRIMARY KEY,
            SECTOR TEXT,
            LISTED_IN TEXT,
            LDCP TEXT,
            OPEN TEXT,
            HIGH TEXT,
            LOW TEXT,
            CURRENT TEXT,
            CHANGE TEXT,
            CHANGE_PERCENT TEXT,
            VOLUME TEXT,
            NAME TEXT,
            SECTOR_NAME TEXT,
            IS_ETF TEXT,
            IS_DEBT TEXT,
            LAST_UPDATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

# === Fetch Market Data from PSX ===
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
                "CHANGE_PERCENT": cols[9],
                "VOLUME": cols[10],
                "NAME": "",
                "SECTOR_NAME": "",
                "IS_ETF": "",
                "IS_DEBT": ""
            }
    return stock_data

# === Fetch Symbol Data from PSX ===
def fetch_symbol_data():
    response = requests.get(PSX_SYMBOLS_URL, headers={"User-Agent": "Mozilla/5.0"})
    return response.json()

# === Merge Market and Symbol Data ===
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

    return market_data

# === Save Data to SQLite Database ===
def save_to_db(data):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for stock in data.values():
        cursor.execute("""
            INSERT INTO stock_data (SYMBOL, SECTOR, LISTED_IN, LDCP, OPEN, HIGH, LOW, CURRENT, CHANGE, CHANGE_PERCENT, VOLUME, NAME, SECTOR_NAME, IS_ETF, IS_DEBT, LAST_UPDATED)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(SYMBOL) DO UPDATE SET
                SECTOR=excluded.SECTOR,
                LISTED_IN=excluded.LISTED_IN,
                LDCP=excluded.LDCP,
                OPEN=excluded.OPEN,
                HIGH=excluded.HIGH,
                LOW=excluded.LOW,
                CURRENT=excluded.CURRENT,
                CHANGE=excluded.CHANGE,
                CHANGE_PERCENT=excluded.CHANGE_PERCENT,
                VOLUME=excluded.VOLUME,
                NAME=excluded.NAME,
                SECTOR_NAME=excluded.SECTOR_NAME,
                IS_ETF=excluded.IS_ETF,
                IS_DEBT=excluded.IS_DEBT,
                LAST_UPDATED=excluded.LAST_UPDATED
        """, (
            stock["SYMBOL"], stock["SECTOR"], stock["LISTED_IN"], stock["LDCP"], stock["OPEN"], 
            stock["HIGH"], stock["LOW"], stock["CURRENT"], stock["CHANGE"], stock["CHANGE_PERCENT"], 
            stock["VOLUME"], stock["NAME"], stock["SECTOR_NAME"], stock["IS_ETF"], stock["IS_DEBT"], timestamp
        ))
    
    conn.commit()
    conn.close()

# === Fetch Data from DB ===
def get_data_from_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute("SELECT MAX(LAST_UPDATED) FROM stock_data")
    last_updated = cursor.fetchone()[0]

    cursor.execute("SELECT * FROM stock_data ORDER BY SYMBOL")
    columns = [desc[0] for desc in cursor.description]
    data = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    conn.close()
    return {
        "last_updated": last_updated,
        "stocks": data
    }

# === Background Task: Fetch & Save Data Every Minute ===
def update_psx_data():
    while True:
        stock_data = merge_data()
        save_to_db(stock_data)
        print(f"Updated PSX Data: {datetime.now()}")
        time.sleep(360)

# === Start Background Task in a Separate Thread ===
threading.Thread(target=update_psx_data, daemon=True).start()

# === API Endpoints ===

@app.get("/psx-data")
def fetch_psx_data():
    return get_data_from_db()

@app.get("/psx-last-updated")
def get_last_updated():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(LAST_UPDATED) FROM stock_data")
    last_updated = cursor.fetchone()[0]

    conn.close()
    return {"last_updated": last_updated}


@app.get("/psx-live")
def fetch_psx_live():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(LAST_UPDATED) FROM stock_data")
    last_updated = cursor.fetchone()[0]

    # Fetch required fields from the stock_data table
    cursor.execute("""
        SELECT SYMBOL, LDCP, OPEN, HIGH, LOW, CURRENT, CHANGE, CHANGE_PERCENT, VOLUME, NAME
        FROM stock_data
        ORDER BY SYMBOL
    """)

    stocks = []
    for row in cursor.fetchall():
        stocks.append({
            "SMBL": row[0],
            "NAME": row[9],
            "OPEN": row[2],
            "HIGH": row[3],
            "LOW": row[4],
            "CURRENT": row[5],
            "CHNG": row[6],
            "CHNG_%": row[7],
            "VOL": row[8],
            "LDCP": row[1]
        })

    conn.close()
    return {
        "last_updated": last_updated,
        "stocks": stocks
    }

  

# === Initialize DB on Startup ===
init_db()
