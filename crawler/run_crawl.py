import asyncio, json, os, aiofiles, time, dotenv
from datetime import datetime
from single_fetch import fetch_product

from dotenv import load_dotenv

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common.cdc import is_price_changed
from db import upsert_price
dotenv.load_dotenv()
POLITE = float(os.getenv("POLITENESS_DELAY", "2.5"))

async def crawl_one(store, url, changes):
    prod = await fetch_product(store, url)
    if is_price_changed(store, prod.sku, prod.price):
        changes.append(prod)
        upsert_price(store, prod.sku, prod.price)

async def main():
    store = "coopmart"
    urls  = [u.strip() for u in open("urls.txt")]
    print(f"Found {len(urls)} URLs to crawl")
    changes = []
    sem = asyncio.Semaphore(5)
    async def worker(url):
        async with sem:
            # await crawl_one(store, url, changes)
            try:
                await crawl_one(store, url, changes)
            except Exception as e:
                print(f"Error fetching {url}: {e}")
            finally:
                print(f"Finished fetching {url}")
            await asyncio.sleep(POLITE)
    await asyncio.gather(*[worker(u) for u in urls])

    # if changes:
    #     payload = "\n".join(f"*{c.title}* ➜ {c.price:,.0f}" for c in changes)
    #     send_slack(f"🔔 Price changes ({len(changes)})\n{payload}")

if __name__ == "__main__":
    print("Starting crawl...")
    asyncio.run(main())
