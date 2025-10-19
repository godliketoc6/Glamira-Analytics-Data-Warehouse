import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright
import os
import time
import json
import re
import random
import signal

load_dotenv()

# --- PostgreSQL setup ---
POSTGRES_URL = os.getenv("POSTGRES_URL")
url_parts = POSTGRES_URL.replace("jdbc:postgresql://", "").split("/")
host_port = url_parts[0].split(":")
POSTGRES_HOST = host_port[0]
POSTGRES_PORT = host_port[1]
POSTGRES_DB = url_parts[1]
if POSTGRES_HOST == "postgres":
    POSTGRES_HOST = "localhost"
    print("⚙️  Replaced 'postgres' hostname with 'localhost'")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# --- Global shutdown flag ---
SHUTDOWN_REQUESTED = False
def signal_handler(signum, frame):
    global SHUTDOWN_REQUESTED
    print("\n🛑 Shutdown signal received. Finishing current item...")
    SHUTDOWN_REQUESTED = True
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# --- User agents for rotation ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# --- Database connection ---
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        return conn
    except psycopg2.Error as e:
        print(f"❌ Database connection error: {e}")
        return None

def create_product_info_table():
    conn = get_db_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS product_info (
            product_name TEXT,
            price TEXT,
            description TEXT,
            current_url TEXT
        );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Table product_info ready")
        return True
    except psycopg2.Error as e:
        print(f"❌ Error creating table: {e}")
        if conn:
            conn.close()
        return False

def get_products_to_crawl():
    conn = get_db_connection()
    if not conn:
        return []
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
        SELECT DISTINCT product_id, current_url
        FROM dim_prd
        WHERE current_url IS NOT NULL AND current_url != ''
        ORDER BY product_id
        """)
        products = cur.fetchall()
        cur.close()
        conn.close()
        return products
    except psycopg2.Error as e:
        print(f"❌ Error fetching products: {e}")
        if conn:
            conn.close()
        return []

# --- Helpers ---
def clean_text(text):
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text.strip())
    suffixes = [
        " | GLAMIRA.ae", " - GLAMIRA.ae", " | GLAMIRA", " - GLAMIRA",
        " | GLAMIRA.de", " - GLAMIRA.de", " | GLAMIRA.com", " - GLAMIRA.com",
        " | Ring-Paare.de", " - Ring-Paare.de"
    ]
    for s in suffixes:
        if text.endswith(s):
            text = text[:-len(s)].strip()
    return text

def get_realistic_headers(domain):
    ua = random.choice(USER_AGENTS)
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": f"https://{domain}/",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    }
    return headers

def extract_json_ld_data(soup):
    scripts = soup.find_all("script", type="application/ld+json")
    for s in scripts:
        try:
            data = json.loads(s.string)
            if isinstance(data, list):
                for item in data:
                    if item.get("@type") == "Product":
                        return item
            elif data.get("@type") == "Product":
                return data
        except Exception:
            continue
    return None

def extract_price(soup, json_ld_data):
    if json_ld_data and "offers" in json_ld_data:
        offers = json_ld_data["offers"]
        if isinstance(offers, list):
            offers = offers[0]
        price = offers.get("price")
        curr = offers.get("priceCurrency", "")
        if price:
            return f"{curr} {price}".strip()
    price_tags = [".price", ".product-price", ".sale-price", "[data-price]"]
    for t in price_tags:
        el = soup.select_one(t)
        if el:
            txt = clean_text(el.get_text())
            if any(c.isdigit() for c in txt):
                return txt
    return "N/A"

def extract_description(soup, json_ld_data):
    if json_ld_data and json_ld_data.get("description"):
        return clean_text(json_ld_data["description"])
    desc_tags = [
        ".product-description", ".description", ".product-details",
        "meta[name='description']", "meta[property='og:description']"
    ]
    for t in desc_tags:
        if t.startswith("meta"):
            el = soup.select_one(t)
            if el and el.get("content"):
                return clean_text(el["content"])
        else:
            el = soup.select_one(t)
            if el:
                return clean_text(el.get_text())
    return "N/A"

# --- Fetching ---
session = requests.Session()

def try_fetch_url(url, max_attempts=3):
    domain = urlparse(url).netloc
    for attempt in range(1, max_attempts + 1):
        try:
            headers = get_realistic_headers(domain)
            print(f"   Attempt {attempt}: {url}")
            res = session.get(url, headers=headers, timeout=25, allow_redirects=True)
            if res.status_code == 200 and "blocked" not in res.text.lower():
                return res
            elif res.status_code == 403:
                print("   ⚠️  403 Forbidden — possibly bot protection")
            elif res.status_code in [429, 503]:
                print(f"   ⏳ Rate limited ({res.status_code}), waiting...")
                time.sleep(random.uniform(10, 20))
            time.sleep(random.uniform(3, 6))
        except requests.exceptions.RequestException as e:
            print(f"   Error: {e}")
            time.sleep(random.uniform(3, 6))
    return None

def fetch_with_playwright(url):
    print("   ⚙️  Using Playwright fallback...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=random.choice(USER_AGENTS))
            page.goto(url, timeout=60000)
            time.sleep(random.uniform(2, 4))
            html = page.content()
            browser.close()
            return html
    except Exception as e:
        print(f"   ❌ Playwright failed: {e}")
        return None

def crawl_product_data(url):
    response = try_fetch_url(url)
    html = None
    if response:
        html = response.text
    else:
        html = fetch_with_playwright(url)

    if not html:
        return {"product_name": "N/A", "price": "N/A", "description": "N/A", "error": "Failed to fetch"}

    soup = BeautifulSoup(html, "html.parser")
    json_ld = extract_json_ld_data(soup)

    name = "N/A"
    if json_ld and json_ld.get("name"):
        name = clean_text(json_ld["name"])
    if name == "N/A":
        meta = soup.find("meta", property="og:title")
        if meta and meta.get("content"):
            name = clean_text(meta["content"])
    if name == "N/A":
        h1 = soup.select_one("h1")
        if h1:
            name = clean_text(h1.get_text())
    if name == "N/A" and soup.title:
        name = clean_text(soup.title.get_text())

    price = extract_price(soup, json_ld)
    desc = extract_description(soup, json_ld)

    return {"product_name": name, "price": price, "description": desc, "error": None}

# --- Save to DB ---
def save_product_info(data):
    conn = get_db_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO product_info (product_name, price, description, current_url) VALUES (%s, %s, %s, %s)",
            (data["product_name"], data["price"], data["description"], data["current_url"]),
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except psycopg2.Error as e:
        print(f"❌ DB error: {e}")
        if conn:
            conn.close()
        return False


# --- Main ---
def main():
    global SHUTDOWN_REQUESTED
    print("🚀 Starting PostgreSQL Web Crawler with Playwright fallback")
    if not create_product_info_table():
        print("❌ Failed to create table. Exiting.")
        return

    products = get_products_to_crawl()
    total = len(products)
    print(f"📊 Found {total} products to crawl\n")
    if not total:
        return

    success, errors = 0, 0
    for i, p in enumerate(products):
        if SHUTDOWN_REQUESTED:
            print(f"\n🛑 Graceful stop after {i}/{total}")
            break

        print(f"🔍 [{i+1}/{total}] Product ID: {p['product_id']}")
        result = crawl_product_data(p["current_url"])
        result["current_url"] = p["current_url"]

        if save_product_info(result):
            if result["error"]:
                print(f"⚠️  Saved with error: {result['error']}")
                errors += 1
            else:
                print(f"✅ Saved: {result['product_name'][:80]}")
                success += 1
        else:
            print("❌ Failed to save")
            errors += 1

        if i < total - 1:
            time.sleep(random.uniform(5, 10))

    print("\n🎉 Done!")
    print(f"✅ Successful: {success} | ❌ Errors: {errors}")

if __name__ == "__main__":
    main()
