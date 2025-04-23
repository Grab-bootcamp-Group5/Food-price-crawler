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

from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

tokenizer_vi2en = AutoTokenizer.from_pretrained(
    "vinai/vinai-translate-vi2en-v2",
    use_fast=False,
    src_lang="vi_VN",     
    tgt_lang="en_XX"      
)
model_vi2en = AutoModelForSeq2SeqLM.from_pretrained("vinai/vinai-translate-vi2en-v2")

def translate_vi2en(vi_text: str) -> str:
    inputs = tokenizer_vi2en(vi_text, return_tensors="pt")
    decoder_start_token_id = tokenizer_vi2en.lang_code_to_id["en_XX"]
    outputs = model_vi2en.generate(
        **inputs,
        decoder_start_token_id=decoder_start_token_id,
        num_beams=5,
        early_stopping=True
    )
    return tokenizer_vi2en.decode(outputs[0], skip_special_tokens=True)

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
    async def fetch_products_by_page(self, category_url: str, page_number: int = 1) -> List[dict]:
        print(f"[Info] Fetching category: {category_url} | store={self.store_id} | page={page_number}")

        # Phải load trang danh mục để browser context có JS và cookies đúng
        await self.page.goto(category_url)

        # Dùng browser context để gọi fetch như browser thực sự
        response = await self.page.evaluate(
            """async () => {
                const formData = new URLSearchParams();

                const res = await fetch("", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
                    },
                    body: formData
                });

                return await res.text(); // HTML
            }"""
        )

        if not response:
            print(f"[Error] Empty response for store {self.store_id} page {page_number}")
            return []

        # Parse từ response HTML, không từ self.page.content()
        soup = BeautifulSoup(response, "html.parser")
        tag = soup.find("module-taxonomy")
        if not tag:
            print("[Error] module-taxonomy not found in response HTML")
            return []

        taxonomy = tag.get("taxonomy")
        term_id = tag.get("term_id")
        items_raw = tag.get("items")
        # print(f"[Info] Taxonomy: {taxonomy}, Term ID: {term_id}, Items: {items_raw}")
        if not (taxonomy and term_id and items_raw):
            print(f"[Error] Missing taxonomy info: {taxonomy}, {term_id}, {items_raw}")
            return []

        try:
            items = json.loads(items_raw)
        except Exception:
            items = [i.strip() for i in items_raw.split(",") if i.strip().isdigit()]

        return await self.fetch_products_by_taxonomy(term_id, taxonomy, self.store_id, category_url, items)


    async def fetch_products_by_taxonomy(self, termid: str, taxonomy: str, store: str, category_url, items: List[str]) -> List[dict]:
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
                english_name = translate_vi2en(item.get("name", ""))
                if not english_name:
                    print(f"Failed to translate name: {item.get('name', '')}")
                    continue
                all_products.append({
                    "sku": item.get("sku"),
                    "name": item.get("name"),
                    "name_en": english_name,
                    "unit": item.get("unit"),
                    "price": float(item.get("price", "0")),
                    "discount": float(item.get("discount", "0")),
                    "promotion": item.get("promotion"),
                    "excerpt": item.get("excerpt"),
                    "image": item.get("image"),
                    "link": item.get("link"),
                    "date_begin": item.get("date_begin"),
                    "date_end": item.get("date_end"),
                    "category": category_url,
                    "store_id": store,
                    "crawled_at": datetime.utcnow().isoformat()
                })
                print(all_products[-1])
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
    async def fetch_categories(self) -> List[Dict]:
        # Fetch html from the page
        html = await self.page.content()
        soup = BeautifulSoup(html, "html.parser")
        categories = []
        # Fetch categories by extract from div container-mega then ul megamenu with li class item-vertical with-sub-menu hover
        category_items = soup.select("li.item-vertical.with-sub-menu.hover")

        for li in category_items:
            title = li.select_one("a span")  # Lấy tên danh mục chính
            href = li.select_one("a")["href"] if li.select_one("a") else None
            # print(f"Category: {title.text.strip() if title else 'N/A'}")
            # print(f"Link: {href}")
            categories.append({
                "title": title.text.strip() if title else None,
                "link": href
            })
        return categories
    async def crawl_prices(self) -> List[Dict]:
        store_ids = await self.get_store_ids_from_db()
        print(f"Found {len(store_ids)} store IDs in the database.")
        crawler = CoopOnlineCrawler(store_id=571)
        try:
            await crawler.init()

            categories = await crawler.fetch_categories()
            all_products_in_store = []
            for category in categories:
                print(f"Category: {category['title']}, Link: {category['link']}")

                products = await crawler.fetch_products_by_page(category["link"])
                print(f"{self.store_id}: {len(products)} products")
                all_products_in_store.extend(products)

            return all_products_in_store
        except Exception as e:
            print(f"Failed for store {self.store_id}:", e)
        finally:
            await crawler.close()
        # for store_id in store_ids:
        #     crawler = CoopOnlineCrawler(store_id=store_id)
        #     try:
        #         await crawler.init()
        #         products = await crawler.fetch_products_by_page(1)
        #         print(f"{store_id}: {len(products)} products")
        #         print(json.dumps(products, indent=2, ensure_ascii=False))
        #     except Exception as e:
        #         print(f"Failed for store {store_id}:", e)
        #     finally:
        #         await crawler.close()



if __name__ == "__main__":
    asyncio.run(main())
