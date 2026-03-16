import requests
import pandas as pd
import json
from datetime import datetime

BASE_URL = "https://gamma-api.polymarket.com"

def fetch_markets(limit=100, offset=0):
    url = f"{BASE_URL}/markets"
    params = {
        "limit": limit,
        "offset": offset,
        "active": "true",
        "closed": "false"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def fetch_all_markets(max_markets=500):
    all_markets = []
    offset = 0
    limit = 100

    while len(all_markets) < max_markets:
        print(f"Fetching markets {offset} to {offset + limit}...")
        markets = fetch_markets(limit=limit, offset=offset)
        if not markets:
            break
        all_markets.extend(markets)
        offset += limit

    return all_markets


def classify_category(question):
    question = question.lower() if question else ""
    if any(w in question for w in ["trump", "biden", "election", "president", "congress", "senate", "vote", "democrat", "republican", "political"]):
        return "politics"
    elif any(w in question for w in ["bitcoin", "btc", "eth", "crypto", "solana", "coinbase", "blockchain"]):
        return "crypto"
    elif any(w in question for w in ["nba", "nfl", "mlb", "nhl", "soccer", "football", "basketball", "tennis", "sports", "championship", "world cup"]):
        return "sports"
    elif any(w in question for w in ["fed", "rate", "inflation", "gdp", "recession", "stock", "market", "economy", "earnings"]):
        return "finance"
    elif any(w in question for w in ["ai", "openai", "gpt", "gemini", "claude", "tech", "apple", "google", "microsoft", "meta"]):
        return "tech"
    elif any(w in question for w in ["war", "ukraine", "russia", "israel", "china", "nato", "military", "ceasefire"]):
        return "geopolitics"
    else:
        return "other"


def parse_markets(markets):
    rows = []
    for m in markets:
        rows.append({
            "market_id":        m.get("id"),
            "question":         m.get("question"),
            "category":         classify_category(m.get("question", "")),
            "volume":           float(m.get("volume", 0) or 0),
            "volume_24hr":      float(m.get("volume24hr", 0) or 0),
            "liquidity":        float(m.get("liquidity", 0) or 0),
            "start_date":       m.get("startDate"),
            "end_date":         m.get("endDate"),
            "active":           m.get("active"),
            "closed":           m.get("closed"),
            "fetched_at":       datetime.utcnow().isoformat()
        })
    return pd.DataFrame(rows)

def save_raw(df, path="data/raw_markets.parquet"):
    df.to_parquet(path, index=False)
    print(f"Saved {len(df)} markets to {path}")




if __name__ == "__main__":
    print("Fetching Polymarket data...")
    markets = fetch_all_markets(max_markets=500)
    df = parse_markets(markets)
    print(df.head())
    print(f"\nTotal markets: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    save_raw(df)
    
