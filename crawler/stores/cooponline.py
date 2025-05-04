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
import sys
import os
import re
import unicodedata
import requests
from dotenv import load_dotenv
load_dotenv()

from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import torch
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from db import upsert_product

torch.set_num_threads(15) 

tokenizer_vi2en = AutoTokenizer.from_pretrained(
    "vinai/vinai-translate-vi2en-v2",
    use_fast=False,
    src_lang="vi_VN",     
    tgt_lang="en_XX"      
)
model_vi2en = AutoModelForSeq2SeqLM.from_pretrained("vinai/vinai-translate-vi2en-v2")


api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
def get_lat_lng(address, api_key):
    url = "https://geocode.maps.co/search"
    params = {"q": address, "api_key": api_key}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if data:
            lat = data[0]['lat']
            lng = data[0]['lon']
            return lat, lng
        else:
            print("No results found")
    return 0, 0

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

    def __init__(self):
        self.store_id = 571
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

        if not isinstance(items, list) or len(items) == 0:
            print(f"[Warning] No valid items found for category {category_url}")
            return []

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
                        # print(f"Crawled stores: {stores}")
                        for store in stores:
                            store["store_id"] = store.pop("id")
                            key = (store["store_id"], store["chain"])
                            store_map[key] = store
                            print(store["name"])
            api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
            for store in store_map.values():
                address = store.get("address", "")
                lat, lon = get_lat_lng(address, api_key)
                store["lat"] = lat
                store["lon"] = lon
                print(f"Store {store['name']} updated with lat/lng: {lat}, {lon}")

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
            grid_blocks = li.select("div.col-lg-12.col-md-12.col-sm-12")
            for block in grid_blocks:
                static_menus = block.select("div.static-menu")
                for menu in static_menus:
                    # Lấy thẻ a.main-menu đầu tiên trong menu
                    a_tag = menu.select_one("a.main-menu")
                    if a_tag:
                        categories.append({
                            "title": a_tag.text.strip(),
                            "link": a_tag.get("href")
                        })
        return categories
    async def crawl_prices(self) -> List[Dict]:
        store_ids = await self.get_store_ids_from_db()
        print(f"Found {len(store_ids)} store IDs in the database.")
        for store_id in store_ids:
            crawler = CoopOnlineCrawler(store_id=store_id)
            try:
                await crawler.init()
                categories = await crawler.fetch_categories()
                valid_titles = set([
    "Nước rửa rau, củ, quả", "Rau Củ", "Trái cây", "Thịt", "Thủy hải sản", "Trứng",
    "Bún tươi, bánh canh", "Thức ăn chế biến", "Kem", "Thực phẩm đông lạnh",
    "Thực phẩm trữ mát", "Bánh", "Hạt, trái cây sấy", "Kẹo", "Mứt, thạch, rong biển",
    "Snack", "Sản phẩm từ sữa khác", "Sữa các loại", "Sữa chua", "Sữa đặc, sữa bột",
    "Thức uống có cồn", "Thức uống dinh dưỡng", "Thức uống không cồn", "Dầu ăn",
    "Đồ hộp", "Gạo, nếp, đậu, bột", "Gia vị nêm", "Lạp xưởng, xúc xích, khô sấy",
    "Ngũ cốc, bánh ăn sáng", "Nui, mì, bún, bánh tráng", "Nước chấm, mắm các loại",
    "Thực phẩm ăn liền", "Tương, sốt"
    # "Gia vị, gạo, thực phẩm khô",
    # "Rau củ, trái cây", "Sữa, sản phẩm từ sữa", "Thịt, trứng, hải sản",
    # "Thức ăn chế biến, bún tươi", "Thực phẩm đông, mát", "Thức uống"
])   
                categories_mapping = {
                    'Nước rửa rau, củ, quả': 'Prepared Vegetables',
                    'Rau Củ': 'Vegetables',
                    'Trái cây': 'Fresh Fruits',
                    'Thịt': 'Fresh Meat',
                    'Thủy hải sản': 'Seafood & Fish Balls',
                    'Trứng': 'Fresh Meat',
                    'Bún tươi, bánh canh': 'Instant Foods',
                    'Thức ăn chế biến': 'Instant Foods',
                    'Kem': 'Ice Cream & Cheese',
                    'Thực phẩm đông lạnh': 'Instant Foods',
                    'Thực phẩm trữ mát': 'Instant Foods',
                    'Bánh': 'Cakes',
                    'Hạt, trái cây sấy': 'Dried Fruits',
                    'Kẹo': 'Candies',
                    'Mứt, thạch, rong biển': 'Fruit Jam',
                    'Snack': 'Snacks',
                    'Sản phẩm từ sữa khác': 'Milk',
                    'Sữa các loại': 'Milk',
                    'Sữa chua': 'Yogurt',
                    'Sữa đặc, sữa bột': 'Milk',
                    'Thức uống có cồn': 'Alcoholic Beverages',
                    'Thức uống dinh dưỡng': 'Beverages',
                    'Thức uống không cồn': 'Beverages',
                    'Dầu ăn': 'Seasonings',
                    'Đồ hộp': 'Instant Foods',
                    'Gạo, nếp, đậu, bột': 'Grains & Staples',
                    'Gia vị nêm': 'Seasonings',
                    'Lạp xưởng, xúc xích, khô sấy': 'Cold Cuts: Sausages & Ham',
                    'Ngũ cốc, bánh ăn sáng': 'Cereals & Grains',
                    'Nui, mì, bún, bánh tráng': 'Instant Foods',
                    'Nước chấm, mắm các loại': 'Seasonings',
                    'Thực phẩm ăn liền': 'Instant Foods',
                    'Tương, sốt': 'Seasonings',
                    'Gia vị, gạo, thực phẩm khô': 'Grains & Staples',
                    'Rau củ, trái cây': 'Vegetables',
                    'Sữa, sản phẩm từ sữa': 'Milk',
                    'Thịt, trứng, hải sản': 'Fresh Meat',
                    'Thức ăn chế biến, bún tươi': 'Instant Foods',
                    'Thực phẩm đông, mát': 'Instant Foods',
                    'Thức uống': 'Beverages'
                    }

                for category in categories:
                    title = category['title']
                    if title in valid_titles:
                        # Mapping category to English
                        category["title"] = categories_mapping[title]
                        print(f"Category: {category['title']}, Link: {category['link']}")
                        products = await crawler.fetch_products_by_page(category["link"])
                        print(f"{self.store_id}: {len(products)} products")
                        for product in products:
                            await upsert_product(product, category["title"])
                # print(f"{store_id}: {len(products)} products")
            except Exception as e:
                print(f"Failed for store {store_id}:", e)
            finally:
                await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
