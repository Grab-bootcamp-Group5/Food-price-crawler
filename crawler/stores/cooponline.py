import asyncio
import json
from typing import List, Dict
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import asyncpg
from .base import BranchCrawler
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from pathlib import Path

import re
import unicodedata

class CoopOnlineCrawler(BranchCrawler):
    chain = "cooponline"

    def __init__(self, store_id: str):
        self.store_id = store_id
        self.browser = None
        self.context = None
        self.page = None

    async def init(self):
        p = await async_playwright().start()
        self.browser = await p.chromium.launch(headless=True)
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()

        await self.page.goto("https://cooponline.vn", timeout=30000)
        await self.page.evaluate(f"""
            () => {{
                localStorage.setItem('store_id', '{self.store_id}');
                location.reload();
            }}
        """)
        await self.page.wait_for_load_state("networkidle")
        await self.page.wait_for_selector("#wrapper")

    async def close(self):
        if self.browser:
            await self.browser.close()

    # --- PRODUCT CRAWLING ---
    async def fetch_products_by_page(self, page_number=1):
        print(f"Fetching products for store {self.store_id} page {page_number}...")
        
        # Print local storage for debugging
        local_storage = await self.page.evaluate("() => JSON.stringify(localStorage)")
        print(f"Local Storage: {local_storage}")
        response = await self.page.evaluate(
            """async ({ store_id, page }) => {
                const formData = new URLSearchParams();

                const res = await fetch("https://cooponline.vn/groups/rau-cu-trai-cay/", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
                    },
                    body: formData
                });

                return await res.text();
            }""",
            {"store_id": self.store_id, "page": str(page_number)}
        )
        if not response:
            print(f"Failed to fetch products for store {self.store_id} page {page_number}")
            return []
        # print(response)
        soup = BeautifulSoup(response, "html.parser")
        products = []
        tag = soup.find("module-taxonomy")
        if not tag:
            return None

        taxonomy = tag.get("taxonomy")
        term_id = tag.get("term_id")
        items_raw = tag.get("items")
        try:
            items = json.loads(items_raw) if items_raw else []
        except:
            items = [i.strip() for i in items_raw.split(",") if i.strip().isdigit()]
        # Fetch products by taxonomy
        products = await self.fetch_products_by_taxonomy(term_id, taxonomy, self.store_id, items)
        print(f"Total products: {len(products)}")
        # for item in soup.select(".product-item"):
        #     title = item.select_one(".product-title")
        #     price = item.select_one(".price")
        #     unit = item.select_one(".product-unit")
        #     sku = item.get("data-id") or ""
        #     href = item.select_one("a")
        #     products.append({
        #         "store": self.store_id,
        #         "title": title.text.strip() if title else None,
        #         "price": self._parse_price(price.text if price else "0"),
        #         "unit": unit.text.strip() if unit else None,
        #         "sku": sku,
        #         "url": f"https://cooponline.vn{href['href']}" if href else None,
        #         "crawled_at": datetime.utcnow().isoformat()
        #     })

        return products
    async def fetch_products_by_taxonomy(self, termid: str, taxonomy: str, store: str, items: List[str]) -> List[dict]:
        all_products = []
        page_number = 1

        while True:
            print(f"Fetching taxonomy page {page_number} for store {store}")

            response = await self.page.evaluate(
                """async ({ termid, taxonomy, store, items, page }) => {
                    const formData = new URLSearchParams();
                    formData.append("request", "w_getProductsTaxonomy");
                    formData.append("termid", termid);
                    formData.append("taxonomy", taxonomy);
                    formData.append("store", store);
                    formData.append("items", items.join(","));
                    formData.append("trang", page.toString());

                    const res = await fetch("https://cooponline.vn/ajax/", {
                        method: "POST",
                        headers: {
                            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
                        },
                        body: formData
                    });

                    return await res.text();
                }""",
                {
                    "termid": str(termid),
                    "taxonomy": taxonomy,
                    "store": str(store),
                    "items": items,
                    "page": page_number
                }
            )
            if not response:
                print(f"Failed to fetch products for store {store} page {page_number}")
                break
            
            products = json.loads(response)
            if not products:
                print("No more products found. Stopping.")
                break
            print(f"Fetched {len(products)} products for store {store} page {page_number}")
            for item in products:
                all_products.append({
                    "sku": item.get("sku"),
                    "name": item.get("name"),
                    "name_normalized": self._normalize_name(item.get("name", "")),
                    "unit": item.get("unit"),
                    "price": float(item.get("price", "0")),
                    "discount": float(item.get("discount", "0")),
                    "promotion": item.get("promotion"),
                    "excerpt": item.get("excerpt"),
                    "image": item.get("image"),
                    "link": item.get("link"),
                    "date_begin": item.get("date_begin"),
                    "date_end": item.get("date_end"),
                    "store": item.get("store"),
                    "crawled_at": datetime.utcnow().isoformat()
                })
            page_number += 1
        print(f"Total products fetched: {len(all_products)}")
        # Save to database
        return all_products

    async def get_store_ids_from_db(db_path="sqlite+aiosqlite:///prices.db") -> List[str]:
        db_path = Path(__file__).resolve().parent.parent.parent / "prices.db"
        print(f"Using database path: {db_path}")
        if not db_path.exists():
            print(f"Database file {db_path} does not exist.")
            return []
        engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
        Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with Session() as session:
            result = await session.execute(
                text("SELECT id FROM store_branch WHERE chain = 'cooponline'")
            )
            rows = result.fetchall()
            return [row[0] for row in rows]

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Convert Vietnamese to ASCII + lowercase, remove punctuation."""
        nfkd = unicodedata.normalize("NFKD", name)
        ascii_str = "".join([c for c in nfkd if not unicodedata.combining(c)])
        return re.sub(r"[^\w\s-]", "", ascii_str).lower().strip()

    @staticmethod
    def _parse_price(self, price_str):
        digits = ''.join(filter(str.isdigit, price_str))
        return float(digits) / 1000 if digits else 0.0

    # --- STORE CRAWLING ---
    @staticmethod
    def _merge_city_ward(citys, wards):
        result = {}
        for city_id, city_info in citys.items():
            result[city_id] = {
                "name": city_info["name"],
                "dsquan": {}
            }
            for district_id, district_name in city_info["dsquan"].items():
                result[city_id]["dsquan"][district_id] = {
                    "name": district_name,
                    "wards": {
                        wid: wname
                        for wid, wname in wards.get(district_id, {}).items()
                    }
                }
        return result

    @staticmethod
    def _parse_stores(html: str, city_name: str, district_name: str, ward_name: str):
        results = []
        try:
            data = json.loads(html)
            for item in data:
                results.append({
                    "id": item.get("id", "unknown"),
                    "chain": "cooponline",
                    "name": item.get("ten"),
                    "address": item.get("diachi"),
                    "phone": item.get("dienthoai"),
                    "city": city_name,
                    "district": district_name,
                    "ward": ward_name,
                    "lat": float(item["Lat"]) if item.get("Lat") not in (None, "") else None,
                    "lon": float(item["Lng"]) if item.get("Lng") not in (None, "") else None
                })
            return results
        except json.JSONDecodeError:
            soup = BeautifulSoup(html, "html.parser")
            for li in soup.select("li"):
                store_id = li.get("data-id") or li.get("id") or "unknown"
                name = li.select_one("strong") or li.select_one(".store-name")
                address = li.select_one(".store-address")
                phone = li.select_one(".store-phone")
                results.append({
                    "id": store_id,
                    "chain": "cooponline",
                    "name": name.text.strip() if name else None,
                    "address": address.text.strip() if address else None,
                    "phone": phone.text.strip() if phone else None,
                    "city": city_name,
                    "district": district_name,
                    "ward": ward_name
                })
            return results

    async def crawl_branches(self) -> List[Dict]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context()
            page = await ctx.new_page()

            await page.goto("https://cooponline.vn", timeout=30000)
            await page.evaluate(f"""
                () => {{
                    localStorage.setItem('store_id', '{self.store_id}');
                    location.reload();
                }}
            """)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_selector("#wrapper")

            vue = await (await page.query_selector("#wrapper")).evaluate_handle("el=>el.__vue__")
            citys = await vue.evaluate("vm=>vm.citys")
            wards = await vue.evaluate("vm=>vm.wards")
            merged = self._merge_city_ward(citys, wards)

            store_map = {}
            for city in merged.values():
                for did, district in city["dsquan"].items():
                    for wid in district["wards"]:
                        html = await page.evaluate(
                            """async ({ district_id, ward_id }) => {
                                const formData = new URLSearchParams();
                                formData.append("request", "w_load_stores");
                                formData.append("selectDistrict", district_id);
                                formData.append("selectWard", ward_id);

                                const res = await fetch("https://cooponline.vn/ajax/", {
                                    method: "POST",
                                    headers: {
                                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
                                    },
                                    body: formData
                                });

                                return await res.text();
                            }""",
                            {
                                "district_id": str(did),
                                "ward_id": str(wid)
                            }
                        )
                        print(f"Crawling: {city['name']} – {district['name']} – {district['wards'][wid]}")
                        stores = self._parse_stores(html, city["name"], district["name"], district['wards'][wid])
                        print(stores)
                        for store in stores:
                            key = (store["id"], store["chain"])
                            store_map[key] = store
            await browser.close()
            return list(store_map.values())

    async def crawl_prices(self) -> List[Dict]:
        store_ids = await self.get_store_ids_from_db()
        print(f"Found {len(store_ids)} store IDs in the database.")
        crawler = CoopOnlineCrawler(store_id=571)
        try:
            await crawler.init()
            products = await crawler.fetch_products_by_page(1)
            print(f"{self.store_id}: {len(products)} products")
            return products
        except Exception as e:
            print(f"Failed for store {self.store_id}:", e)
        finally:
            await crawler.close()
        # for store_id in store_ids:
        #     crawler = CoopOnlineCrawler(store_id=store_id)
        #     try:
        #         await crawler.init()
        #         products = await crawler.fetch_products_by_page(1)
        #         print(f"✅ {store_id}: {len(products)} products")
        #         print(json.dumps(products, indent=2, ensure_ascii=False))
        #     except Exception as e:
        #         print(f"❌ Failed for store {store_id}:", e)
        #     finally:
        #         await crawler.close()



if __name__ == "__main__":
    asyncio.run(main())
