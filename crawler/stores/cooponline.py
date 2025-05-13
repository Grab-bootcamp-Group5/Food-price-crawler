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
from motor.motor_asyncio import AsyncIOMotorClient
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

from db import upsert_product, upsert_branch, fetch_branches

torch.set_num_threads(16) 

tokenizer_vi2en = AutoTokenizer.from_pretrained(
    "vinai/vinai-translate-vi2en-v2",
    use_fast=False,
    src_lang="vi_VN",     
    tgt_lang="en_XX"      
)
model_vi2en = AutoModelForSeq2SeqLM.from_pretrained("vinai/vinai-translate-vi2en-v2")
import re

def extract_net_value_and_unit_from_name(name: str, fallback_unit: str):
    tmp_name = name.lower()
    match = re.search(r"(\d+(\.\d+)?)\s*(g|ml|lít|kg|gói|l)", tmp_name)
    if match:
        value = float(match.group(1))
        unit = match.group(3)
        return value, unit
    return 1, fallback_unit

def normalize_net_value(unit: str, net_value: float, name: str):
    unit = unit.lower()
    name_lower = name.lower()

    ## Net value can only find on name_lower it may be not exist like kg or exists with 250 ml 250ml 259g 250 g so we need to match reges
    # 0. Trường hợp không có net value thì lấy từ name
    net_value, unit= extract_net_value_and_unit_from_name(name, unit)

    if unit == "kg":
        return float(net_value) * 1000, "g"
    elif unit == "l":
        return float(net_value) * 1000, "ml"

    if unit in ["g", "ml"]:
        match_kg = re.search(r"(\d+(\.\d+)?)?\s*kg", name_lower)
        if match_kg:
            # value may be not exist in name
            value = match_kg.group(1)
            # check if value is number
            if str(value).isdigit():
                value = float(value)
                return value * 1000, unit
            return 1000, unit
    if unit in ["cái"]:
        return float(net_value) * 1000, "g"
    if unit in ["g", "hộp", "vĩ"] and "trứng" in name_lower:
        match = re.search(r"(\d+)\s*trứng", name_lower)
        if match:
            return int(match.group(1)), "hộp"

    if unit in ["vĩ"] and "kg" in name_lower:
        return float(net_value) * 1000, "g"
    if unit in ["trái", "túi", "bịch"]:
        return float(net_value) * 1000, unit

    # 3. Hộp hoặc vỉ có số lượng (quả trứng, ...)
    if unit in ["hộp"] and "quả" in name_lower:
        matches = re.findall(rf"{unit}\s*(\d+)", name_lower)
        if matches:
            return sum(map(int, matches)), unit

    # 4. Thùng / Lốc X đơn vị Y ml/g
    match_pack = re.search(r"(thùng|lốc)\s*(\d+).*?(\d+(\.\d+)?)\s*(g|ml)", name_lower)
    if match_pack:
        count = int(match_pack.group(2))
        per_item = float(match_pack.group(3))
        return count * per_item, unit

    # 5. Trường hợp fallback (gói, khay, ống...) — giữ nguyên unit gốc, chỉ đổi value nếu có thông tin
    extracted_value, _ = extract_net_value_and_unit_from_name(name, unit)
    if extracted_value > 0:
        return extracted_value, unit

    return float(net_value) if net_value != 0 else 1000, unit

def extract_best_price(product: dict) -> dict:
    name = product.get("name", "")
    original_unit = product.get("unit", "").lower()
    def build_result(info: dict, unit: str, net_value: float):
        # calculate price by discount if exist
        discountPercent = 0
        if info.get("discount") and float(info.get("discount")) > 0:
            price = float(info.get("discount"))
            ## round to 1 decimal
            
            discountPercent = 1- price/float(info.get("price"))
            # print(f"[BasePrice] Product name: {name}, discountPercent: {discountPercent}")
        else:
            price = float(info.get("price", 0))
        return {
            "name": name,
            "unit": unit, 
            "netUnitValue": net_value,
            "price": price,
            "sysPrice": float(info.get("price")),
            "discount": round(discountPercent,2),
            "date_begin": info.get("date_begin"),
            "date_end": info.get("date_end"),
        }

    net_value, converted_unit = normalize_net_value(original_unit, 0, name)
    # print(f"[BasePrice] Product name: {name}, old unit: {original_unit}, netUnitValue: {net_value}")
    return build_result(product, converted_unit, net_value)


def tokenize_by_whitespace(text: str) -> List[str]:
    if text is None:
        return []
    return [token for token in text.lower().split() if len(token) >= 2]

def generate_ngrams(token: str, n: int) -> List[str]:
    if token is None or len(token) < n:
        return []
    return [token[i:i+n] for i in range(len(token) - n + 1)]

def generate_token_ngrams(text: str, n: int) -> List[str]:
    tokens = tokenize_by_whitespace(text)
    ngrams = []
    for token in tokens:
        ngrams.extend(generate_ngrams(token, n))
    return ngrams

def get_lat_lng(store_name, api_key):
    api_url = f"https://api.openrouteservice.org/geocode/search?api_key={api_key}&text={store_name}"
    print("Geocoding:", api_url)
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        if data.get("features"):
            first_feature = data["features"][0]
            lon, lat = first_feature["geometry"]["coordinates"]
            return lat, lon
        else:
            print(f"No features returned for: {store_name}")
    except Exception as e:
        print(f"Error fetching geocode for {store_name}: {e}")
    
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

    def __init__(self, store_id: int = 571):
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
    async def fetch_products_by_page(self, store_object, category, page_number: int = 1) -> List[dict]:
        # print(f"[Info] Fetching category: {category_url} | store={self.store_id} | page={page_number}")

        # Phải load trang danh mục để browser context có JS và cookies đúng
        category_url = category.get("link", "")
        if not category_url:
            print(f"[Error] Invalid category URL: {category_url}")
            return []
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

        return await self.fetch_products_by_taxonomy(term_id, taxonomy, store_object, category, items)



    async def fetch_products_by_taxonomy(self, termid: str, taxonomy: str, store_object, category, items: List[str]) -> List[dict]:
        all_products = []
        page_number = 1
        store = store_object.get("store_id", 571)
        category_url = category.get("link", "")
        category = category.get("title", "")
        # print(f"[Info] Fetching products for store {store} category {category}")
        while True:
            # print(f"Fetching taxonomy page {page_number} for store {store}")
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
            # print(f"Fetched {len(products)} products for store {store} page {page_number}")
            for item in products:
                english_name = translate_vi2en(item.get("name", ""))
                if not english_name:
                    print(f"Failed to translate name: {item.get('name', '')}")
                    continue
                price_info = extract_best_price(item)
                token_ngrams = generate_token_ngrams(english_name, 2)
                # print(f"Product: {price_info['name']}, Discount Percent: {price_info['discount']}, SysPrice {price_info['sysPrice']}, Price: {price_info['price']}, Unit: {price_info['unit']}, netUnitValue: {price_info['netUnitValue']}")
                product_data = {
                    "sku": item.get("sku"),
                    "name": item.get("name"),
                    "name_en": english_name,
                    "unit": price_info.get("unit"),
                    "netUnitValue": price_info.get("netUnitValue"),
                    "token_ngrams": token_ngrams,
                    "sysPrice": float(price_info.get("sysPrice", "0")),
                    "price": float(price_info.get("price", "0")),
                    "discountPercent": float(price_info.get("discount", "0")),
                    "promotion": item.get("promotion"),
                    "excerpt": item.get("excerpt"),
                    "image": item.get("image"),
                    "url": item.get("link"),
                    "date_begin": price_info.get("date_begin"),
                    "date_end": price_info.get("date_end"),
                    "category": category,
                    "store_id": store_object["_id"],
                    "crawled_at": datetime.utcnow().isoformat()
                }
                all_products.append(product_data)
                await upsert_product(product_data, category)
            page_number += 1
        print(f"Total products fetched: {len(all_products)}")
        return all_products

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
                "id": city_id,
                "name": city_info["name"],
                "dsquan": {}
            }
            for district_id, district_name in city_info["dsquan"].items():
                result[city_id]["dsquan"][district_id] = {
                    "id": district_id,
                    "name": district_name,
                    "wards": {
                        wid: wname
                        for wid, wname in wards.get(district_id, {}).items()
                    }
                }
        return result

    @staticmethod
    def _parse_stores(html: str, provinceId, districtId, wardId):
        results = []
        try:
            data = json.loads(html)
            for item in data:
                results.append({
                    "store_id": item.get("id", "unknown"),
                    "chain": "cooponline",
                    "name": item.get("ten"),
                    "storeLocation": item.get("diachi"),
                    "phone": item.get("dienthoai"),
                    "provinceId": provinceId,
                    "districtId": districtId,
                    "wardId": wardId,
                    "lat": float(item["Lat"]) if item.get("Lat") not in (None, "") else None,
                    "lng": float(item["Lng"]) if item.get("Lng") not in (None, "") else None
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

            meta_client = AsyncIOMotorClient("mongodb://103.172.79.235:27017")
            meta_db = meta_client.metadata_db_v3
            store_branches = meta_db.store_branches
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
                        # print(f"Crawling: {city['name']} – {district['name']} – {district['wards'][wid]}")
                        provinceName = city["name"]
                        # provinceName contain Tinhrh/Thành Phố so please remove
                        provinceName = provinceName.replace("Tỉnh ", "").replace("Thành phố ", "")
                        if provinceName == "Thừa Thiên Huế":
                            provinceName = "Huế"
                            print(f"Province name: {provinceName}")
                        districtName = district["name"]
                        # print(f"Crawling: {provinceName} – {districtName}")
                        stores = self._parse_stores(html, did, did, wid)

                        for store in stores:
                            ## Update store_branches provinceId, distrinctId provinceName districtName by find storelocation
                            ## to get provinceId please use current city index in array like city [421] = city
                            provinceId = city["id"]
                            # print(f"Store provinceId: {provinceId}, districtId: {did}, provinceName: {provinceName}, districtName: {districtName}")
                            await store_branches.update_one(
                                {"address": store["storeLocation"]},
                                {
                                    "$set": {
                                        "provinceId": provinceId,
                                        "districtId": did,
                                        "provinceName": provinceName,
                                        "districtName": districtName
                                    }
                                },
                                upsert=True
                            )


            api_key = os.environ.get("OPENROUTE_API_KEY")
            for store in store_map.values():
                store_name = store.get("name", "")
                result = get_lat_lng(store_name, api_key)
                if result and isinstance(result, tuple):
                    lat, lon = result
                else:
                    lat, lon = store.get("lat", 0), store.get("lng", 0)

                store["lat"] = lat
                store["lng"] = lon
                await upsert_branch(store)
                print(f"Upserted store: {store['name']} with lat: {lat}, lon: {lon}")

            await browser.close()
            return list(store_map.values())
    async def fetch_categories(self) -> List[Dict]:
        # Fetch html from the page
        html = await self.page.content()
        soup = BeautifulSoup(html, "html.parser")
        categories = []
        # Fetch categories by extract from div container-mega then ul megamenu with li class item-vertical with-subC