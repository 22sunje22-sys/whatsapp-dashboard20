#!/usr/bin/env python3
"""
GA4 → Supabase daily fetcher
Fetches yesterday's data: itemCategory + customEvent:currency → itemRevenue + itemsPurchased
Classifies categories as Attractions / Events / Service
Upserts to ga4_market_daily table
"""

import os, json, datetime, requests
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, Dimension, Metric, DateRange, FilterExpression,
    Filter, FilterExpressionList
)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
GA4_PROPERTY_ID = "349972872"
SUPABASE_URL = "https://kwftlkfvtglnugxsyjci.supabase.co"
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]  # from GitHub secret

# ─── CATEGORY CLASSIFICATION ─────────────────────────────────────────────────
ATTRACTIONS = {
    "Must-see attractions","Top-Rated Attractions","Theme Parks","Zoos and Aquariums",
    "Water Parks","Indoor Attractions","Outdoor Attractions","Gardens & parks","Museums",
    "Observation Decks","Sky View","Burj Khalifa","XDubai Attractions","Ain Dubai",
    "Sightseeing and Tours","Sightseeing","Desert safaris","Boat Tours","Dinner Cruises",
    "Entertainment and Games","Combos","Recently Added Experiences","Attractions Special Offers",
    "Horse Riding","Kids Activities","ADNOC Pro League","Basketball","AFC Asian Cup",
    "Jebel Jais Attractions","Air Adventures","Dubai Dolphinarium","Dubai Frame",
    "Kayaking","Extreme sports","Dinner Shows",
}
SERVICE = {
    "Service","Insurance","membership","Ramadan specials","Ghabga","Season Cards",
    "Brunches","Beach Club","Gaming & ESports","Upsell",
}
VALID_CURRENCIES = {"AED","SAR","QAR","BHD","GBP","EUR","OMR","USD","TRY"}

def classify(category: str) -> str:
    if category in ATTRACTIONS:
        return "Attractions"
    if category in SERVICE:
        return "Service"
    return "Events"

# ─── FETCH GA4 ───────────────────────────────────────────────────────────────
def fetch_ga4(date_str: str) -> dict:
    """
    Returns dict: { (currency, category_type) -> {revenue, transactions} }
    date_str: YYYY-MM-DD
    """
    # Service account JSON from env
    sa_json = os.environ.get("GA4_SERVICE_ACCOUNT_JSON")
    if sa_json:
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write(sa_json)
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name

    client = BetaAnalyticsDataClient()

    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[
            Dimension(name="customEvent:currency"),
            Dimension(name="itemCategory"),
        ],
        metrics=[
            Metric(name="itemRevenue"),
            Metric(name="itemsPurchased"),
        ],
        date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
        limit=10000,
    )

    response = client.run_report(request)

    result = {}
    for row in response.rows:
        currency = row.dimension_values[0].value
        item_cat = row.dimension_values[1].value
        revenue = float(row.metric_values[0].value or 0)
        transactions = int(float(row.metric_values[1].value or 0))

        if currency not in VALID_CURRENCIES:
            continue
        if revenue <= 0:
            continue

        cat_type = classify(item_cat)
        if cat_type == "Service":
            continue  # exclude service

        key = (currency, cat_type)
        if key not in result:
            result[key] = {"revenue": 0.0, "transactions": 0}
        result[key]["revenue"] += revenue
        result[key]["transactions"] += transactions

    return result

# ─── UPSERT TO SUPABASE ───────────────────────────────────────────────────────
def upsert_supabase(date_str: str, data: dict):
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    rows = []
    for (currency, category), vals in data.items():
        rows.append({
            "report_date": date_str,
            "currency": currency,
            "category": category,
            "revenue": round(vals["revenue"], 2),
            "transactions": vals["transactions"],
        })

    if not rows:
        print(f"No data to insert for {date_str}")
        return

    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/ga4_market_daily?on_conflict=report_date,currency,category",
        headers=headers,
        json=rows,
    )
    resp.raise_for_status()
    print(f"Upserted {len(rows)} rows for {date_str}: {[r['currency']+'/'+r['category'] for r in rows]}")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    # By default fetch yesterday (Dubai time); can also accept a date arg
    import sys
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        # Yesterday in Dubai timezone
        dubai_now = datetime.datetime.utcnow() + datetime.timedelta(hours=4)
        target_date = (dubai_now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Fetching GA4 data for: {target_date}")
    data = fetch_ga4(target_date)
    print(f"Got {len(data)} market/category combos")
    upsert_supabase(target_date, data)
    print("Done!")

if __name__ == "__main__":
    main()
