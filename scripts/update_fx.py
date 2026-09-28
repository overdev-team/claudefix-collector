#!/usr/bin/env python3
"""Store a daily USD-based exchange-rate snapshot for build-time estimates.

Rates come from Fawaz Ahmed's free Currency API, published as static JSON on
jsDelivr with a Cloudflare Pages mirror. Only currencies the site uses (store
currencies in prices.json plus the display-currency menu) are kept. On any
failure the previous snapshot is left untouched.
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "src/data/fx.json"
PRICES = ROOT / "src/data/prices.json"
ENDPOINTS = [
    "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json",
    "https://latest.currency-api.pages.dev/v1/currencies/usd.json",
]
SOURCE = "Fawaz Ahmed Currency API"
SOURCE_URL = "https://github.com/fawazahmed0/exchange-api"
# Keep in sync with the currency menu in src/components/PricingAtlas.tsx.
DISPLAY_CURRENCIES = {"USD", "EUR", "CNY", "HKD", "TWD", "JPY", "KRW", "SGD", "GBP", "INR", "BRL", "TRY", "IDR", "RUB"}
MAX_BYTES = 2 * 1024 * 1024


def wanted_currencies() -> set:
    prices = json.loads(PRICES.read_text(encoding="utf-8"))
    codes = set(DISPLAY_CURRENCIES)
    for entry in prices.values():
        for store in ("apple", "google"):
            currency = (entry.get(store) or {}).get("currency")
            if isinstance(currency, str) and re.fullmatch(r"[A-Z]{3}", currency):
                codes.add(currency)
    return codes


def fetch(url: str) -> dict:
    request = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "claudefix-fx/1.0"})
    with urllib.request.urlopen(request, timeout=25) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}")
        payload = response.read(MAX_BYTES + 1)
    if len(payload) > MAX_BYTES:
        raise RuntimeError("response exceeded 2 MB")
    data = json.loads(payload)
    if not isinstance(data, dict) or not isinstance(data.get("usd"), dict) or not isinstance(data.get("date"), str):
        raise ValueError("unexpected response shape")
    return data


def main() -> int:
    data = None
    for url in ENDPOINTS:
        try:
            data = fetch(url)
            break
        except (urllib.error.URLError, TimeoutError, RuntimeError, ValueError) as error:
            print(f"Rate request to {url} failed: {error}", file=sys.stderr)
    if data is None:
        print("No rate source available; keeping the previous snapshot.", file=sys.stderr)
        return 1

    wanted = wanted_currencies()
    rates = {"USD": 1.0}
    for code in sorted(wanted):
        rate = data["usd"].get(code.lower())
        if isinstance(rate, (int, float)) and 0 < rate < 1_000_000_000:
            rates[code] = float(rate)
    missing = sorted(wanted - rates.keys())
    if missing:
        print(f"Rates missing for: {', '.join(missing)}", file=sys.stderr)
    if len(rates) < len(DISPLAY_CURRENCIES):
        print(f"Only {len(rates)} usable rates returned; keeping the previous snapshot.", file=sys.stderr)
        return 1

    previous = json.loads(OUTPUT.read_text(encoding="utf-8")) if OUTPUT.exists() else {}
    if previous.get("rateDate") == data["date"] and previous.get("rates") == rates:
        print(f"Rates for {data['date']} unchanged; snapshot left as is.")
        return 0

    snapshot = {
        "source": SOURCE,
        "base": "USD",
        "rateDate": data["date"],
        "checkedAt": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "sourceUrl": SOURCE_URL,
        "rates": rates,
    }
    temp = OUTPUT.with_suffix(".json.tmp")
    temp.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp.replace(OUTPUT)
    print(f"Saved {len(rates)} rates dated {data['date']} to {OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
