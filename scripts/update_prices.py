#!/usr/bin/env python3
"""
scripts/update_prices.py

Automated public store pricing collector for Claudefix (Linear OPC-223).
Collects public in-app purchase pricing for Claude from:
  1. Apple App Store (country-specific product pages)
  2. Google Play Store (country-specific detail pages)

Data contract:
  - Input: src/data/regions.json (array of {code, nameEn, nameZh})
  - Output: src/data/prices.json (keyed by uppercase ISO2 country code)
    - apple: {currency, items: [{plan, period, displayPrice}], checkedAt, sourceUrl}
    - google: {currency, range, checkedAt, sourceUrl}

Key guarantees:
  - Bounded concurrency (<=4 workers)
  - Retries with exponential backoff and timeout
  - Anti-bot and redirect validation (rejects redirect to store home or wrong region)
  - No fabricated values: only explicitly displayed store values
  - Independent source isolation: Apple failure does not impact Google (and vice versa)
  - Snapshot preservation: preserves previous successful data on fetch failure
  - Safe atomic write to disk
  - Non-zero exit code if all fetches fail when prior data exists
"""

import argparse
import concurrent.futures
from datetime import datetime, timezone
import html
import json
import logging
import os
from pathlib import Path
import random
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple
import urllib.error
import urllib.parse
import urllib.request

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("update_prices")

# Constants
MAX_RESPONSE_BYTES = 5 * 1024 * 1024  # 5 MB max response limit
DEFAULT_TIMEOUT = 10  # seconds
DEFAULT_RETRIES = 1
DEFAULT_CONCURRENCY = 4
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/128.0.0.0 Safari/537.36"
)

# ISO2 Country Code to ISO 4217 Currency Code mapping
ISO2_TO_CURRENCY: Dict[str, str] = {
    "AD": "EUR", "AE": "AED", "AG": "XCD", "AL": "ALL", "AM": "AMD",
    "AO": "AOA", "AR": "ARS", "AT": "EUR", "AU": "AUD", "AZ": "AZN",
    "BA": "BAM", "BB": "BBD", "BD": "BDT", "BE": "EUR", "BF": "XOF",
    "BG": "EUR", "BH": "BHD", "BJ": "XOF", "BN": "BND", "BO": "BOB",
    "BR": "BRL", "BS": "BSD", "BT": "BTN", "BW": "BWP", "BZ": "BZD",
    "BI": "BIF", "CA": "CAD", "CD": "CDF", "CG": "XAF", "CH": "CHF", "CI": "XOF",
    "CL": "CLP", "CM": "XAF", "CO": "COP", "CR": "CRC", "CV": "CVE", "CF": "XAF",
    "CY": "EUR", "CZ": "CZK", "DE": "EUR", "DJ": "DJF", "DK": "DKK",
    "DM": "XCD", "DO": "DOP", "DZ": "DZD", "EC": "USD", "EE": "EUR",
    "EG": "EGP", "ER": "ERN", "ES": "EUR", "ET": "ETB", "FI": "EUR", "FJ": "FJD",
    "FR": "EUR", "GA": "XAF", "GB": "GBP", "GD": "XCD", "GE": "GEL",
    "GH": "GHS", "GM": "GMD", "GN": "GNF", "GQ": "XAF", "GR": "EUR",
    "GT": "GTQ", "GW": "XOF", "GY": "GYD", "HN": "HNL", "HR": "EUR",
    "HT": "HTG", "HU": "HUF", "ID": "IDR", "IE": "EUR", "IL": "ILS",
    "IN": "INR", "IQ": "IQD", "IS": "ISK", "IT": "EUR", "JM": "JMD",
    "JO": "JOD", "JP": "JPY", "KE": "KES", "KG": "KGS", "KH": "KHR",
    "KI": "AUD", "KM": "KMF", "KN": "XCD", "KR": "KRW", "KW": "KWD", "KZ": "KZT",
    "LA": "LAK", "LB": "LBP", "LC": "XCD", "LI": "CHF", "LK": "LKR",
    "LR": "LRD", "LS": "LSL", "LT": "EUR", "LU": "EUR", "LV": "EUR",
    "LY": "LYD", "MA": "MAD", "MC": "EUR", "MD": "MDL", "ME": "EUR",
    "MG": "MGA", "MK": "MKD", "ML": "XOF", "MN": "MNT", "MO": "MOP",
    "MR": "MRU", "MT": "EUR", "MU": "MUR", "MV": "MVR", "MW": "MWK", "MH": "USD",
    "MX": "MXN", "MY": "MYR", "MZ": "MZN", "NA": "NAD", "NE": "XOF", "FM": "USD",
    "NG": "NGN", "NI": "NIO", "NL": "EUR", "NO": "NOK", "NP": "NPR",
    "NR": "AUD", "NZ": "NZD", "OM": "OMR", "PA": "PAB", "PE": "PEN",
    "PG": "PGK", "PH": "PHP", "PK": "PKR", "PL": "PLN", "PT": "EUR",
    "PW": "USD", "PY": "PYG", "QA": "QAR", "RO": "RON", "RS": "RSD", "SM": "EUR",
    "RW": "RWF", "SA": "SAR", "SB": "SBD", "SC": "SCR", "SE": "SEK",
    "SG": "SGD", "SI": "EUR", "SK": "EUR", "SL": "SLE", "SN": "XOF",
    "SR": "SRD", "ST": "STN", "SV": "USD", "SZ": "SZL", "TD": "XAF", "SO": "SOS",
    "TG": "XOF", "TH": "THB", "TJ": "TJS", "TL": "USD", "TN": "TND",
    "TO": "TOP", "TR": "TRY", "TT": "TTD", "TV": "AUD", "TW": "TWD", "SS": "SSP", "SD": "SDG", "TM": "TMT", "VA": "EUR",
    "TZ": "TZS", "UA": "UAH", "UG": "UGX", "US": "USD", "UY": "UYU",
    "UZ": "UZS", "VC": "XCD", "VN": "VND", "VU": "VUV", "WS": "WST",
    "XK": "EUR", "YE": "YER", "ZA": "ZAR", "ZM": "ZMW", "ZW": "USD",
}

# Bare dollar signs are ambiguous. These mappings were checked against the
# named Claude IAP lines in each country's official App Store page.
APPLE_VERIFIED_BARE_DOLLAR_CURRENCY: Dict[str, str] = {
    "AU": "AUD", "BH": "USD", "CA": "CAD", "CL": "CLP",
    "NZ": "NZD", "OM": "USD", "TW": "TWD", "US": "USD",
}

FX_SNAPSHOT = Path(__file__).resolve().parents[1] / "src" / "data" / "fx.json"


def _parse_display_amount(text: str) -> Optional[float]:
    """Parse a store price label such as '$ 99.900,00' or 'R2,499.99' into a number."""
    matched = re.search(r"\d[\d\s\u00a0\u202f.,']*", text)
    if not matched:
        return None
    raw = re.sub(r"[\s\u00a0\u202f']", "", matched.group(0)).rstrip(".,")
    last = max(raw.rfind(","), raw.rfind("."))
    if last < 0:
        return float(raw)
    decimals = len(raw) - last - 1
    digits = re.sub(r"[.,]", "", raw)
    if decimals in (1, 2):
        return float(f"{digits[:-decimals]}.{digits[-decimals:]}")
    return float(digits) if decimals == 3 else None


def _plausible_bare_dollar_currency(country_code: str, items: List[Dict[str, str]]) -> Optional[str]:
    """Pick USD or the local currency for a bare '$' price using the Pro monthly price.

    Only one candidate may put Claude Pro between US$12 and US$60 per month;
    otherwise the price stays unverified.
    """
    pro = next((item for item in items if item["plan"] == "Pro" and item["period"] == "monthly"), None)
    amount = _parse_display_amount(pro["displayPrice"]) if pro else None
    try:
        rates = json.loads(FX_SNAPSHOT.read_text(encoding="utf-8")).get("rates", {})
    except (OSError, ValueError):
        return None
    if not amount or not rates:
        return None
    candidates = {"USD", resolve_currency(country_code) or "USD"}
    plausible = [code for code in candidates if rates.get(code) and 12 <= amount / rates[code] <= 60]
    return plausible[0] if len(plausible) == 1 else None


def get_iso_timestamp() -> str:
    """Return ISO 8601 UTC timestamp with millisecond precision (e.g. 2026-09-28T01:00:00.000Z)."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def resolve_currency(country_code: str, fallback_currency: Optional[str] = None) -> Optional[str]:
    """Resolve standard 3-letter currency code for a given ISO country code."""
    upper = country_code.upper()
    if upper in ISO2_TO_CURRENCY:
        return ISO2_TO_CURRENCY[upper]
    if fallback_currency and len(fallback_currency) == 3 and fallback_currency.isalpha():
        return fallback_currency.upper()
    return None


def safe_http_get(url: str, timeout: int = DEFAULT_TIMEOUT, retries: int = DEFAULT_RETRIES) -> Tuple[str, str]:
    """
    Perform safe HTTP GET with timeout, retries, bounded size, and redirect checking.
    Returns (html_text, final_url).
    Raises urllib.error.URLError, ValueError, or RuntimeError on failure.
    """
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    last_error: Optional[Exception] = None

    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                final_url = response.geturl()
                content_bytes = response.read(MAX_RESPONSE_BYTES + 1)
                content_type = response.headers.get("Content-Type", "")
                if len(content_bytes) > MAX_RESPONSE_BYTES:
                    raise ValueError(f"Response exceeds {MAX_RESPONSE_BYTES} bytes: {url}")
                if "text/html" not in content_type.lower():
                    raise ValueError(f"Unexpected Content-Type {content_type!r}: {url}")
                
                # Check for bot challenge / captcha pages
                body_text = content_bytes.decode("utf-8", errors="replace")
                bot_signatures = [
                    "cf-browser-verification",
                    "challenges.cloudflare.com",
                    "Attention Required! | Cloudflare",
                    "systems have detected unusual traffic",
                    "unusual traffic from your computer network",
                    "<title>Access Denied</title>",
                    "captcha-delivery.com",
                ]
                for sig in bot_signatures:
                    if sig.lower() in body_text.lower():
                        raise RuntimeError(f"Anti-bot or CAPTCHA challenge detected on {url}")

                return body_text, final_url

        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, RuntimeError) as e:
            last_error = e
            # Handle HTTP 404 immediately without retry
            if isinstance(e, urllib.error.HTTPError) and e.code == 404:
                raise e
            if attempt < retries:
                backoff = (1.5 ** attempt) + random.uniform(0.2, 0.6)
                time.sleep(backoff)
            else:
                break

    if last_error:
        raise last_error
    raise RuntimeError(f"Unknown network error fetching {url}")


# ---------------------------------------------------------------------------
# Apple App Store Collector & Parser
# ---------------------------------------------------------------------------

def fetch_apple_price(country_code: str, timeout: int = DEFAULT_TIMEOUT, retries: int = DEFAULT_RETRIES) -> Optional[Dict[str, Any]]:
    """
    Fetch public Apple App Store page for Claude.
    Validates redirect to prevent redirecting away from the target country/app.
    Maps explicitly named Claude Pro/Max plans.
    """
    country_lower = country_code.lower()
    source_url = f"https://apps.apple.com/{country_lower}/app/claude-by-anthropic/id6473753684"

    try:
        body_text, final_url = safe_http_get(source_url, timeout=timeout, retries=retries)
    except Exception as e:
        logger.warning(f"[{country_code}] Apple fetch failed: {e}")
        return None

    # Redirect and identity validation:
    # Reject redirects to store home or completely different regions (e.g. /cn/iphone/today)
    parsed_final = urllib.parse.urlparse(final_url)
    path_segments = [seg for seg in parsed_final.path.split("/") if seg]
    
    if not path_segments or path_segments[0].lower() != country_lower:
        logger.warning(
            f"[{country_code}] Apple redirected to wrong country/path '{parsed_final.path}' "
            f"(expected prefix '/{country_lower}/'). Rejecting."
        )
        return None

    if parsed_final.hostname != "apps.apple.com" or "id6473753684" not in parsed_final.path:
        logger.warning(f"[{country_code}] Apple redirected away from Claude app page to: {final_url}. Rejecting.")
        return None

    # App identity check in HTML
    if "Claude" not in body_text or ("Anthropic" not in body_text and "id6473753684" not in body_text):
        logger.warning(f"[{country_code}] Apple page HTML does not match Claude by Anthropic identity.")
        return None

    raw_items: List[Dict[str, str]] = []

    # 1. Parse JSON-LD or Shoebox scripts embedded in the page
    script_blocks = re.findall(r'<script[^>]*>(.*?)</script>', body_text, re.DOTALL | re.IGNORECASE)
    for block in script_blocks:
        block = block.strip()
        if not block.startswith("{") and not block.startswith("["):
            continue
        try:
            parsed_json = json.loads(block)
            _extract_apple_purchases_from_json(parsed_json, raw_items)
        except Exception:
            pass

    # 2. Parse HTML In-App Purchases markup
    # Typical Apple App Store format:
    # <li class="list-with-numbers__item"><span class="list-with-numbers__item__title">Claude Pro</span><span class="list-with-numbers__item__price">$20.00</span></li>
    li_matches = re.findall(
        r'<li[^>]*class=["\'][^"\']*list-with-numbers__item[^"\']*["\'][^>]*>.*?'
        r'<span[^>]*class=["\'][^"\']*__title[^"\']*["\'][^>]*>(.*?)</span>.*?'
        r'<span[^>]*class=["\'][^"\']*__price[^"\']*["\'][^>]*>(.*?)</span>',
        body_text,
        re.DOTALL | re.IGNORECASE,
    )
    for title, price in li_matches:
        clean_title = html.unescape(re.sub(r'<[^>]+>', '', title)).strip()
        clean_price = html.unescape(re.sub(r'<[^>]+>', '', price)).strip()
        if clean_title and clean_price:
            _classify_and_add_apple_item(clean_title, clean_price, raw_items)

    # 3. Current HTML markup: <div class="text-pair"><span>Claude Pro - Monthly</span> <span>$20.00</span></div>
    if not raw_items:
        pair_matches = re.findall(
            r'<div[^>]*class=["\'][^"\']*\btext-pair\b[^"\']*["\'][^>]*>\s*<span[^>]*>(.*?)</span>\s*<span[^>]*>(.*?)</span>',
            body_text,
            re.DOTALL | re.IGNORECASE,
        )
        for title, price in pair_matches:
            clean_title = html.unescape(re.sub(r'<[^>]+>', '', title)).strip()
            clean_price = html.unescape(re.sub(r'<[^>]+>', '', price)).strip()
            if clean_title and clean_price:
                _classify_and_add_apple_item(clean_title, clean_price, raw_items)

    # 4. Fallback regex for definition lists or generic Claude plan mentions
    if not raw_items:
        generic_matches = re.findall(
            r'(Claude\s+(?:Pro|Max(?:\s+\d+x)?)[^<>\n\r]{0,30})\s*<[^>]+>\s*([^<>\n\r]{1,15}\d[\d,.\s\u00a0]*[^<>\n\r]{0,10})',
            body_text,
            re.IGNORECASE,
        )
        for title, price in generic_matches:
            clean_title = html.unescape(title).strip()
            clean_price = html.unescape(price).strip()
            if re.search(r'\d', clean_price):
                _classify_and_add_apple_item(clean_title, clean_price, raw_items)

    if not raw_items:
        logger.info(f"[{country_code}] Apple page loaded successfully, but no explicit Pro/Max in-app purchases displayed.")
        return None

    # Deduplicate and sort items into standard atlas order:
    # 1. Pro monthly
    # 2. Max 5x monthly
    # 3. Max 20x monthly
    # 4. Pro annual
    unique_items: Dict[Tuple[str, str], str] = {}
    for item in raw_items:
        key = (item["plan"], item["period"])
        # HTML storefront text is appended after embedded JSON, so it wins when
        # both describe the same named item with different price formatting.
        unique_items[key] = item["displayPrice"]

    ordered_keys = [
        ("Pro", "monthly"),
        ("Max 5x", "monthly"),
        ("Max 20x", "monthly"),
        ("Pro", "annual"),
    ]

    final_items: List[Dict[str, str]] = []
    for k in ordered_keys:
        if k in unique_items:
            final_items.append({
                "plan": k[0],
                "period": k[1],
                "displayPrice": unique_items.pop(k),
            })
    # Append any remaining classified items
    for (plan, period), price in unique_items.items():
        final_items.append({
            "plan": plan,
            "period": period,
            "displayPrice": price,
        })

    # Determine the IAP storefront currency from displayed prices first. A free app's
    # country or metadata is not sufficient: some stores bill in USD (BH/OM/KH/PA).
    explicit_codes = {
        match.group(1)
        for item in final_items
        for match in re.finditer(r"\b([A-Z]{3})\b", item["displayPrice"])
        if match.group(1) in ISO2_TO_CURRENCY.values()
    }
    if len(explicit_codes) > 1:
        logger.warning(f"[{country_code}] Conflicting Apple IAP currencies: {explicit_codes}.")
        return None
    display_prices = " ".join(item["displayPrice"] for item in final_items)
    if explicit_codes:
        currency = next(iter(explicit_codes))
    elif "€" in display_prices:
        currency = "EUR"
    elif "£" in display_prices:
        currency = "GBP"
    elif "S$" in display_prices:
        currency = "SGD"
    elif "R$" in display_prices:
        currency = "BRL"
    elif "NT$" in display_prices:
        currency = "TWD"
    elif "MX$" in display_prices:
        currency = "MXN"
    elif "US$" in display_prices:
        currency = "USD"
    elif "¥" in display_prices and country_code.upper() == "JP":
        currency = "JPY"
    elif "￦" in display_prices or "₩" in display_prices:
        currency = "KRW"
    elif "₹" in display_prices:
        currency = "INR"
    elif "฿" in display_prices:
        currency = "THB"
    elif "₪" in display_prices:
        currency = "ILS"
    elif "₫" in display_prices:
        currency = "VND"
    elif "₱" in display_prices:
        currency = "PHP"
    elif "₦" in display_prices:
        currency = "NGN"
    elif "₾" in display_prices:
        currency = "GEL"
    elif "₺" in display_prices:
        currency = "TRY"
    elif "₴" in display_prices:
        currency = "UAH"
    elif country_code.upper() == "ZA" and re.search(r"(?:^|\s)R\s?\d", display_prices):
        currency = "ZAR"
    elif "$" in display_prices:
        currency = (APPLE_VERIFIED_BARE_DOLLAR_CURRENCY.get(country_code.upper())
                    or _plausible_bare_dollar_currency(country_code, final_items))
    elif re.search(r"[^\d\s,.-]", display_prices):
        # Localized labels such as `99,99 zł`, `Rp 349ribu`, or `249,00 kr`
        # carry a currency marker even when the ISO code is omitted.
        currency = resolve_currency(country_code)
    else:
        currency = None
    if currency is None:
        metadata_match = re.search(
            r'property=["\'](?:og|product):price:currency["\']\s+content=["\']([A-Z]{3})["\']',
            body_text,
            re.IGNORECASE,
        )
        if metadata_match:
            currency = metadata_match.group(1).upper()
    if currency is None:
        logger.info(f"[{country_code}] Apple IAP currency is ambiguous; skipping unverified price.")
        return None

    return {
        "currency": currency,
        "items": final_items,
        "checkedAt": get_iso_timestamp(),
        "sourceUrl": source_url,
    }


def _classify_and_add_apple_item(title: str, price: str, out_items: List[Dict[str, str]]) -> None:
    """Classify in-app purchase title into plan/period and add if valid."""
    title_lower = re.sub(r"\s+", " ", title.lower()).strip()
    plan_match = re.match(r"^claude (pro|max(?: (?:5x|20x))?)\b", title_lower)
    period_match = re.search(r"\b(monthly|annual|yearly)\b", title_lower)
    if not plan_match or not period_match:
        return
    plan = {"pro": "Pro", "max": "Max", "max 5x": "Max 5x", "max 20x": "Max 20x"}[plan_match.group(1)]
    period = "monthly" if period_match.group(1) == "monthly" else "annual"

    # Price validation
    clean_price = price.strip().replace("\xa0", " ")
    currency_marker = re.search(r"[^\d\s,.-]", clean_price)
    if not re.search(r'\d', clean_price) or not currency_marker or len(clean_price) > 40:
        return

    out_items.append({
        "plan": plan,
        "period": period,
        "displayPrice": clean_price,
    })


def _extract_apple_purchases_from_json(node: Any, out_items: List[Dict[str, str]]) -> None:
    """Recursively search JSON structures for inAppPurchases or offers."""
    if isinstance(node, dict):
        # Check for inAppPurchases array
        for key in ["inAppPurchases", "in_app_purchases", "inAppOffers"]:
            if key in node and isinstance(node[key], list):
                for item in node[key]:
                    if isinstance(item, dict):
                        title = item.get("name") or item.get("title") or item.get("description") or ""
                        price = item.get("formattedPrice") or item.get("displayPrice") or ""
                        if title and price:
                            _classify_and_add_apple_item(str(title), str(price), out_items)

        # Current App Store pages list IAPs as [title, price] pairs, e.g.
        # {"title": "In-App Purchases", "items": [{"textPairs": [["Claude Pro - Monthly", "$20.00"]]}]}
        pairs = node.get("textPairs")
        if isinstance(pairs, list):
            for pair in pairs:
                if isinstance(pair, list) and len(pair) == 2 and all(isinstance(v, str) for v in pair):
                    _classify_and_add_apple_item(pair[0], pair[1], out_items)

        for v in node.values():
            _extract_apple_purchases_from_json(v, out_items)
    elif isinstance(node, list):
        for item in node:
            _extract_apple_purchases_from_json(item, out_items)


# ---------------------------------------------------------------------------
# Google Play Store Collector & Parser
# ---------------------------------------------------------------------------

def fetch_google_price(country_code: str, timeout: int = DEFAULT_TIMEOUT, retries: int = DEFAULT_RETRIES) -> Optional[Dict[str, Any]]:
    """
    Fetch public Google Play Store page for Claude.
    Extracts the publicly exposed In-app purchases range.
    Never infers plan-specific prices from range.
    """
    country_upper = country_code.upper()
    source_url = f"https://play.google.com/store/apps/details?id=com.anthropic.claude&gl={country_upper}&hl=en"

    try:
        body_text, final_url = safe_http_get(source_url, timeout=timeout, retries=retries)
    except Exception as e:
        logger.warning(f"[{country_code}] Google fetch failed: {e}")
        return None

    # Identity verification
    parsed_final = urllib.parse.urlparse(final_url)
    final_query = urllib.parse.parse_qs(parsed_final.query)
    if (
        parsed_final.hostname != "play.google.com"
        or parsed_final.path != "/store/apps/details"
        or final_query.get("id") != ["com.anthropic.claude"]
        or final_query.get("gl", [""])[0].upper() != country_upper
    ):
        logger.warning(f"[{country_code}] Google redirected away from Claude package: {final_url}. Rejecting.")
        return None

    if "Claude" not in body_text or "Anthropic" not in body_text:
        logger.warning(f"[{country_code}] Google Play HTML does not match Claude app identity.")
        return None

    # Anchor to the Claude app's own data entry so recommendation ranges cannot be mistaken
    # for the target product. Google varies the suffix by region.
    range_match = re.search(
        r'\["Claude by Anthropic"\].{0,3000}?\["([^"<>]{3,120}\s-\s[^"<>]{3,120}(?:per item|if billed through Play))",\[0\]\]',
        body_text,
        re.DOTALL | re.IGNORECASE,
    )
    if not range_match:
        logger.info(f"[{country_code}] Google Play page loaded, but no in-app purchases range found.")
        return None
    range_str = html.unescape(range_match.group(1)).strip().replace("\xa0", " ")

    # Determine currency
    currency = resolve_currency(country_code)
    # An explicit storefront currency label overrides the country's customary currency.
    # Bulgaria adopted EUR in 2026; several Play storefronts show USD even when their
    # country's legal currency is different. Never label a visible $ value as BHD etc.
    code_match = re.search(r'\b([A-Z]{3})\b', range_str)
    if code_match and code_match.group(1) in ISO2_TO_CURRENCY.values():
        currency = code_match.group(1)
    elif "€" in range_str:
        currency = "EUR"
    elif "£" in range_str:
        currency = "GBP"
    elif range_str.startswith("US$"):
        currency = "USD"
    elif range_str.startswith("$") and country_code.upper() in {"BH", "KH", "KW", "OM", "PA"}:
        currency = "USD"
    if currency is None:
        logger.info(f"[{country_code}] Google storefront currency is unknown; skipping unverified range.")
        return None

    return {
        "currency": currency,
        "range": range_str,
        "checkedAt": get_iso_timestamp(),
        "sourceUrl": source_url,
    }


# ---------------------------------------------------------------------------
# File Operations & Validation
# ---------------------------------------------------------------------------

def load_regions(regions_path: Path) -> List[Dict[str, str]]:
    """Load officially supported Claude regions from src/data/regions.json."""
    if not regions_path.exists():
        logger.error(f"Regions file not found at {regions_path}")
        return []
    try:
        with open(regions_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return [
                    {
                        "code": str(item.get("code", "")).strip().upper(),
                        "nameEn": str(item.get("nameEn", "")).strip(),
                        "nameZh": str(item.get("nameZh", "")).strip(),
                    }
                    for item in data
                    if item.get("code")
                ]
    except Exception as e:
        logger.error(f"Failed to parse regions file {regions_path}: {e}")
    return []


def load_prices(prices_path: Path) -> Dict[str, Any]:
    """Load an existing snapshot; fail closed if its JSON is corrupt."""
    if not prices_path.exists():
        logger.info(f"Prices file {prices_path} does not exist yet. Initializing empty.")
        return {}
    try:
        content = prices_path.read_text(encoding="utf-8").strip()
        if not content:
            return {}
        data = json.loads(content)
        if not isinstance(data, dict):
            raise ValueError("prices root must be an object")
        return data
    except (OSError, json.JSONDecodeError, ValueError) as e:
        raise ValueError(f"Cannot read existing price snapshot {prices_path}: {e}") from e


def atomic_write_json(target_path: Path, data: Dict[str, Any]) -> None:
    """Safely and atomically write JSON data to target file via temporary file."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_name(f".{target_path.name}.tmp.{os.getpid()}")
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.write("\n")
        os.replace(temp_path, target_path)
        logger.info(f"Successfully atomic-wrote updated prices to {target_path}")
    except Exception as e:
        if temp_path.exists():
            temp_path.unlink()
        raise e


def validate_prices_schema(prices: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Validate prices data contract."""
    errors: List[str] = []
    if not isinstance(prices, dict):
        return False, ["Prices root must be a dictionary keyed by ISO2 country code."]

    for code, country_data in prices.items():
        if not re.match(r'^[A-Z]{2}$', code):
            errors.append(f"Invalid country key '{code}'; must be uppercase 2-letter ISO.")
        if not isinstance(country_data, dict):
            errors.append(f"[{code}] Value must be a dictionary.")
            continue
        if not any(channel in country_data for channel in ("apple", "google")):
            errors.append(f"[{code}] At least one verified store channel is required.")

        for channel in ("apple", "google"):
            if channel not in country_data or not isinstance(country_data[channel], dict):
                continue
            observation = country_data[channel]
            if not isinstance(observation.get("currency"), str) or not re.fullmatch(r"[A-Z]{3}", observation["currency"]):
                errors.append(f"[{code}] {channel} currency must be a three-letter ISO code.")
            checked_at = observation.get("checkedAt")
            try:
                if not isinstance(checked_at, str) or not checked_at.endswith("Z"):
                    raise ValueError("UTC timestamp required")
                datetime.fromisoformat(checked_at.replace("Z", "+00:00"))
            except ValueError:
                errors.append(f"[{code}] {channel} checkedAt must be a UTC ISO timestamp.")
            source_url = observation.get("sourceUrl")
            if not isinstance(source_url, str) or not source_url.startswith("https://"):
                errors.append(f"[{code}] {channel} sourceUrl must be an HTTPS URL.")

        # Validate Apple
        if "apple" in country_data:
            apple = country_data["apple"]
            if not isinstance(apple, dict):
                errors.append(f"[{code}] apple must be a dictionary.")
            else:
                if not apple.get("currency") or not isinstance(apple.get("currency"), str):
                    errors.append(f"[{code}] apple missing valid currency.")
                if not isinstance(apple.get("items"), list):
                    errors.append(f"[{code}] apple items must be a list.")
                else:
                    if not apple["items"]:
                        errors.append(f"[{code}] apple items cannot be empty.")
                    seen = set()
                    for idx, item in enumerate(apple["items"]):
                        if not isinstance(item, dict):
                            errors.append(f"[{code}] apple item #{idx} must be a dict.")
                        elif not item.get("plan") or not item.get("period") or not item.get("displayPrice"):
                            errors.append(f"[{code}] apple item #{idx} missing required fields (plan, period, displayPrice).")
                        else:
                            key = (item["plan"], item["period"])
                            if item["plan"] not in {"Pro", "Max", "Max 5x", "Max 20x"} or item["period"] not in {"monthly", "annual"}:
                                errors.append(f"[{code}] apple item #{idx} has an unknown plan or period.")
                            if key in seen:
                                errors.append(f"[{code}] apple has duplicate item {key}.")
                            seen.add(key)
                            if not isinstance(item["displayPrice"], str) or not re.search(r"\d", item["displayPrice"]):
                                errors.append(f"[{code}] apple item #{idx} has no displayed price.")

        # Validate Google
        if "google" in country_data:
            google = country_data["google"]
            if not isinstance(google, dict):
                errors.append(f"[{code}] google must be a dictionary.")
            else:
                if not google.get("currency") or not isinstance(google.get("currency"), str):
                    errors.append(f"[{code}] google missing valid currency.")
                if not google.get("range") or not isinstance(google.get("range"), str):
                    errors.append(f"[{code}] google missing valid range string.")
                elif not re.search(r"\d.+\s-\s.+\d", google["range"]):
                    errors.append(f"[{code}] google range must show two endpoints.")

    return len(errors) == 0, errors


# ---------------------------------------------------------------------------
# Country Collection Worker
# ---------------------------------------------------------------------------

def process_country(country_code: str, existing_entry: Optional[Dict[str, Any]], timeout: int, retries: int) -> Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, Any]], bool, bool]:
    """
    Process both Apple and Google sources for a country.
    Returns:
      (country_code, new_apple_dict, new_google_dict, apple_succeeded, google_succeeded)
    """
    # Fetch Apple
    apple_data = fetch_apple_price(country_code, timeout=timeout, retries=retries)
    apple_success = apple_data is not None

    # Fetch Google
    google_data = fetch_google_price(country_code, timeout=timeout, retries=retries)
    google_success = google_data is not None

    return country_code, apple_data, google_data, apple_success, google_success


# ---------------------------------------------------------------------------
# Main Routine
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Claudefix Scheduled Store Pricing Collector (Linear OPC-223)"
    )
    parser.add_argument(
        "--regions-file",
        type=Path,
        default=Path("src/data/regions.json"),
        help="Path to regions.json (default: src/data/regions.json)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("src/data/prices.json"),
        help="Path to prices.json (default: src/data/prices.json)",
    )
    parser.add_argument(
        "--country",
        type=str,
        default="",
        help="Comma-separated country code filter (e.g. 'US,JP,GB'). Default checks all regions.",
    )
    parser.add_argument(
        "--concurrency",
        "-c",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help="Concurrency limit (max 4, default 3).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="HTTP request timeout in seconds (default: 10).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help="Number of network retries (default: 1).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate collection without modifying prices.json on disk.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate syntax and data contract of current prices.json and exit.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # If --check mode, validate existing output file and exit
    if args.check:
        logger.info(f"Running data contract validation on {args.output}...")
        try:
            prices = load_prices(args.output)
        except ValueError as e:
            logger.error(str(e))
            return 1
        if not prices:
            logger.error(f"Cannot validate: {args.output} is empty or missing.")
            return 1
        valid, errors = validate_prices_schema(prices)
        if valid:
            logger.info(f"Data contract validation PASSED for {args.output} ({len(prices)} countries).")
            return 0
        else:
            logger.error(f"Data contract validation FAILED with {len(errors)} error(s):")
            for err in errors[:20]:
                logger.error(f"  - {err}")
            return 1

    # Bounded concurrency constraint (<= 4)
    concurrency = min(max(1, args.concurrency), 4)

    # Load regions
    regions = load_regions(args.regions_file)
    if not regions:
        logger.error(f"No regions loaded from {args.regions_file}. Aborting.")
        return 1

    # Filter countries if requested
    if args.country:
        allowed = {c.strip().upper() for c in args.country.split(",") if c.strip()}
        regions = [r for r in regions if r["code"] in allowed]
        logger.info(f"Filtering collection to {len(regions)} specified country/countries: {allowed}")

    if not regions:
        logger.error("No matching regions to process.")
        return 1

    # Load existing prices snapshot
    try:
        existing_prices = load_prices(args.output)
    except ValueError as e:
        logger.error(str(e))
        return 1
    valid, errors = validate_prices_schema(existing_prices)
    if not valid:
        logger.error(f"Existing price snapshot is invalid: {errors[:5]}")
        return 1
    logger.info(f"Loaded existing prices database with {len(existing_prices)} regions.")

    updated_prices: Dict[str, Any] = dict(existing_prices)

    total_apple_attempts = len(regions)
    total_google_attempts = len(regions)
    apple_success_count = 0
    google_success_count = 0

    logger.info(
        f"Starting collection for {len(regions)} regions with concurrency={concurrency}, "
        f"timeout={args.timeout}s, retries={args.retries}..."
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        future_to_code = {
            executor.submit(
                process_country,
                region["code"],
                existing_prices.get(region["code"]),
                args.timeout,
                args.retries,
            ): region["code"]
            for region in regions
        }

        for future in concurrent.futures.as_completed(future_to_code):
            code = future_to_code[future]
            try:
                c_code, new_apple, new_google, apple_ok, google_ok = future.result()

                if apple_ok:
                    apple_success_count += 1
                if google_ok:
                    google_success_count += 1

                # Update or preserve snapshot
                country_entry = dict(updated_prices.get(c_code, {}))

                # Handle Apple source
                if apple_ok and new_apple:
                    country_entry["apple"] = new_apple
                elif "apple" in country_entry:
                    # Fetch failed; preserve previous snapshot and previous checkedAt
                    logger.info(f"[{c_code}] Apple check failed; preserved prior snapshot.")

                # Handle Google source
                if google_ok and new_google:
                    country_entry["google"] = new_google
                elif "google" in country_entry:
                    # Fetch failed; preserve previous snapshot and previous checkedAt
                    logger.info(f"[{c_code}] Google check failed; preserved prior snapshot.")

                if country_entry:
                    updated_prices[c_code] = country_entry

            except Exception as exc:
                logger.error(f"[{code}] Unexpected error processing country: {exc}")

    total_successes = apple_success_count + google_success_count
    total_attempts = total_apple_attempts + total_google_attempts

    logger.info("=" * 60)
    logger.info("COLLECTION SUMMARY:")
    logger.info(f"  Regions evaluated:       {len(regions)}")
    logger.info(f"  Apple Store successes:   {apple_success_count}/{total_apple_attempts}")
    logger.info(f"  Google Play successes:   {google_success_count}/{total_google_attempts}")
    logger.info(f"  Total source successes:  {total_successes}/{total_attempts}")
    logger.info("=" * 60)

    # If all fetches failed when prior prices exist, avoid writing changed file and exit non-zero
    if total_successes == 0:
        logger.error(
            "ALL store page fetches failed! Refusing to write empty or corrupted file. "
            "Preserving prior prices database and exiting with non-zero status."
        )
        return 1

    # Validate output schema before committing to disk
    valid, errors = validate_prices_schema(updated_prices)
    if not valid:
        logger.error(f"Generated data failed schema validation ({len(errors)} errors). Aborting write.")
        for err in errors[:10]:
            logger.error(f"  - {err}")
        return 1

    # Sort dictionary keys alphabetically (e.g. AU, BR, CA...)
    sorted_prices = {k: updated_prices[k] for k in sorted(updated_prices.keys())}

    if args.dry_run:
        logger.info(f"[DRY-RUN] Verification complete. {len(sorted_prices)} regions in atlas. File not written.")
        return 0

    try:
        atomic_write_json(args.output, sorted_prices)
    except Exception as e:
        logger.error(f"Failed to write updated prices to {args.output}: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
