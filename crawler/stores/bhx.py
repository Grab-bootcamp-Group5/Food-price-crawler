import asyncio
import json
from typing import List, Dict
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import asyncpg
import requests
import sys
import os
import re
import unicodedata
from urllib.parse import unquote, quote
import httpx
from motor.motor_asyncio import AsyncIOMotorClient
import torch
import uuid
from curl_cffi.requests import Session
session = Session(impersonate="chrome110")
from dotenv import load_dotenv
load_dotenv()
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import torch
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from db import upsert_product
from typing import List
session = Session(impersonate="chrome110")
torch.set_num_threads(15) 

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
import re

def extract_net_value_and_unit_from_name(name: str, fallback_unit: str):
    tmp_name = name.lower()
    matches = re.findall(r"(\d+(?:\.\d+)?)\s*(g|ml|lít|kg|gói|l)\b", tmp_name)
    if matches:
        value, unit = matches[-1]  # use the LAST match
        return float(value), unit
    return 1, fallback_unit

def normalize_net_value(unit: str, net_value: float, name: str):
    unit = unit.lower()
    name_lower = name.lower()

    # 1. Nếu là đơn vị quy đổi thì nhân để tính lại netValue, NHƯNG GIỮ UNIT GỐC
    if unit == "kg":
        return float(net_value) * 1000, "g"
    elif unit == "lít":
        return float(net_value) * 1000, "ml"
    if unit not in ["kg", "g", "ml", "lít"]:
        match_kg = re.search(r"(\d+(\.\d+)?)\s*kg", name_lower)
        if match_kg:
            value = float(match_kg.group(1))
            return value * 1000, unit
    if unit == "túi 1kg":
        return float(net_value) * 1000, "túi"
    # 2. Túi có trái thì giả định 0.7kg
    if unit == "túi" and "trái" in name_lower:
        return 0.7 * 1000, unit

    # 3. Hộp hoặc vỉ có số lượng (quả trứng, ...)
    if unit in ["hộp", "vỉ"] and "quả" in name_lower:
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

    return float(net_value) if net_value != 0 else 1, unit

def extract_best_price(product: dict) -> dict:
    base_price_info = product.get("productPrices", [])
    campaign_info = product.get("lstCampaingInfo", [])
    name = product.get("name", "")
    original_unit = product.get("unit", "").lower()
    def build_result(info: dict, unit: str, net_value: float):
        return {
            "name": name,
            "unit": unit, 
            "netUnitValue": net_value,
            "price": info.get("price"),
            "sysPrice": info.get("sysPrice"),
            "discountPercent": info.get("discountPercent"),
            "date_begin": info.get("startTime") or info.get("poDate"),
            "date_end": info.get("dueTime") or info.get("poDate"),
        }
    # 1. Campaign ưu tiên
    if campaign_info:
        campaign = campaign_info[0]
        campaign_price = campaign.get("productPrice", {})
        net_value = campaign_price.get("netUnitValue", 0)

        net_value, converted_unit = normalize_net_value(original_unit, net_value, name)
        # print(f"[Campaign] Product name: {name}, old unit: {original_unit}, netUnitValue: {net_value}")
        return build_result(campaign_price, converted_unit, net_value)

    # 2. Fallback sang base_price
    if base_price_info:
        price_info = base_price_info[0]
        net_value = price_info.get("netUnitValue", 0)

        net_value, converted_unit = normalize_net_value(original_unit, net_value, name)
        # print(f"[BasePrice] Product name: {name}, old unit: {original_unit}, netUnitValue: {net_value}")
        return build_result(price_info, converted_unit, net_value)

    # 3. No info then u
    return {
        "name": name,
        "unit": original_unit,
        "netUnitValue": 1,
        "price": None,
        "sysPrice": None,
        "discountPercent": None,
        "date_begin": None,
        "date_end": None,
    }

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

from db import upsert_branch
from .base import BranchCrawler

class BHXOnlineCrawler(BranchCrawler):
    chain = "bhx"

    def __init__(self):
        self.browser = None
        self.context = None
        self.page = None
        self.playwright: Playwright = None 
        self.token = None

    async def init(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()
        
        def log_request(request):
            if "Menu/GetMenuV2" in request.url:
                print("Intercepted request to:", request.url)
                for k, v in request.headers.items():
                    if k.lower() == "authorization":
                        self.token = v
                        print(f"Found Bearer token: {v}")
                    else:
                        print(f"{k}: {v}")

            if "Location/V2/GetStoresByLocation" in request.url:
                print("Intercepted store request:", request.url)
                for k, v in request.headers.items():
                    if k.lower() == "authorization":
                        self.token = v
                        print(f"Found Store Bearer token: {v}")

        self.page.on("request", log_request)
        try:
            await self.page.goto("https://www.bachhoaxanh.com/he-thong-cua-hang", wait_until="domcontentloaded", timeout=20000)
        except Exception as e:
            print("Failed to load page:", e)
            return


        await self.page.wait_for_timeout(5000)

    async def fetch_categories(self) -> List[Dict]:
        async with self.page.expect_response("**/Menu/GetMenuV2**") as resp_info:
            await self.page.goto("https://www.bachhoaxanh.com/", timeout=30000)
        response = await resp_info.value

        if response.status != 200:
            print(f"Failed to fetch categories: {response.status}")
            return []

        data = await response.json()
        valid_titles = set([
            # Thịt, cá, trứng
            "Thịt heo", "Thịt bò", "Thịt gà, vịt, chim", "Thịt sơ chế", "Trứng gà, vịt, cút",
            "Cá, hải sản, khô", "Cá hộp", "Lạp xưởng", "Xúc xích", "Heo, bò, pate hộp",
            "Chả giò, chả ram", "Chả lụa, thịt nguội", "Xúc xích, lạp xưởng tươi",
            "Cá viên, bò viên", "Thịt, cá đông lạnh",

            # Rau, củ, quả, nấm
            "Trái cây", "Rau lá", "Củ, quả", "Nấm các loại", "Rau, củ làm sẵn",
            "Rau củ đông lạnh",

            # Đồ ăn chay
            "Đồ chay ăn liền", "Đậu hũ, đồ chay khác", "Đậu hũ, tàu hũ",

            # Ngũ cốc, tinh bột
            "Ngũ cốc", "Ngũ cốc, yến mạch", "Gạo các loại", "Bột các loại",
            "Đậu, nấm, đồ khô",

            # Mì, bún, phở, cháo
            "Mì ăn liền", "Phở, bún ăn liền", "Hủ tiếu, miến", "Miến, hủ tiếu, phở khô",
            "Mì Ý, mì trứng", "Cháo gói, cháo tươi", "Bún các loại", "Nui các loại",
            "Bánh tráng các loại", "Bánh phồng, bánh đa", "Bánh gạo Hàn Quốc",

            # Gia vị, phụ gia, dầu
            "Nước mắm", "Nước tương", "Tương, chao các loại", "Tương ớt - đen, mayonnaise",
            "Dầu ăn", "Dầu hào, giấm, bơ", "Gia vị nêm sẵn", "Muối",
            "Hạt nêm, bột ngọt, bột canh", "Tiêu, sa tế, ớt bột", "Bột nghệ, tỏi, hồi, quế,...",
            "Nước chấm, mắm", "Mật ong, bột nghệ",

            # Sữa & các sản phẩm từ sữa
            "Sữa tươi", "Sữa đặc", "Sữa pha sẵn", "Sữa hạt, sữa đậu", "Sữa ca cao, lúa mạch",
            "Sữa trái cây, trà sữa", "Sữa chua ăn", "Sữa chua uống liền", "Bơ sữa, phô mai",

            # Đồ uống
            "Bia, nước có cồn", "Rượu", "Nước trà", "Nước ngọt", "Nước ép trái cây",
            "Nước yến", "Nước tăng lực, bù khoáng", "Nước suối", "Cà phê hoà tan",
            "Cà phê pha phin", "Cà phê lon", "Trà khô, túi lọc",

            # Bánh kẹo, snack
            "Bánh tươi, Sandwich", "Bánh bông lan", "Bánh quy", "Bánh snack, rong biển",
            "Bánh Chocopie", "Bánh gạo", "Bánh quế", "Bánh que", "Bánh xốp",
            "Kẹo cứng", "Kẹo dẻo, kẹo marshmallow", "Kẹo singum", "Socola",
            "Trái cây sấy", "Hạt khô", "Rong biển các loại", "Rau câu, thạch dừa",
            "Mứt trái cây", "Cơm cháy, bánh tráng",

            # Món ăn chế biến sẵn, đông lạnh
            "Làm sẵn, ăn liền", "Sơ chế, tẩm ướp", "Nước lẩu, viên thả lẩu",
            "Kim chi, đồ chua", "Mandu, há cảo, sủi cảo", "Bánh bao, bánh mì, pizza",
            "Kem cây, kem hộp", "Bánh flan, thạch, chè", "Trái cây hộp, siro",

            "Cá mắm, dưa mắm", "Đường", "Nước cốt dừa lon", "Sữa chua uống", "Khô chế biến sẵn"
        ])

        categories_mapping = {
            # Thịt, cá, trứng
            "Thịt heo": "Fresh Meat",
            "Thịt bò": "Fresh Meat",
            "Thịt gà, vịt, chim": "Fresh Meat",
            "Thịt sơ chế": "Fresh Meat",
            "Trứng gà, vịt, cút": "Fresh Meat",
            "Cá, hải sản, khô": "Seafood & Fish Balls",
            "Cá hộp": "Instant Foods",
            "Lạp xưởng": "Cold Cuts: Sausages & Ham",
            "Xúc xích": "Cold Cuts: Sausages & Ham",
            "Heo, bò, pate hộp": "Instant Foods",
            "Chả giò, chả ram": "Instant Foods",
            "Chả lụa, thịt nguội": "Cold Cuts: Sausages & Ham",
            "Xúc xích, lạp xưởng tươi": "Cold Cuts: Sausages & Ham",
            "Cá viên, bò viên": "Instant Foods",
            "Thịt, cá đông lạnh": "Instant Foods",

            # Rau, củ, quả, nấm
            "Trái cây": "Fresh Fruits",
            "Rau lá": "Vegetables",
            "Củ, quả": "Vegetables",
            "Nấm các loại": "Vegetables",
            "Rau, củ làm sẵn": "Vegetables",
            "Rau củ đông lạnh": "Vegetables",

            # Đồ ăn chay
            "Đồ chay ăn liền": "Instant Foods",
            "Đậu hũ, đồ chay khác": "Instant Foods",
            "Đậu hũ, tàu hũ": "Instant Foods",

            # Ngũ cốc, tinh bột
            "Ngũ cốc": "Cereals & Grains",
            "Ngũ cốc, yến mạch": "Cereals & Grains",
            "Gạo các loại": "Grains & Staples",
            "Bột các loại": "Grains & Staples",
            "Đậu, nấm, đồ khô": "Grains & Staples",

            # Mì, bún, phở, cháo
            "Mì ăn liền": "Instant Foods",
            "Phở, bún ăn liền": "Instant Foods",
            "Hủ tiếu, miến": "Instant Foods",
            "Miến, hủ tiếu, phở khô": "Instant Foods",
            "Mì Ý, mì trứng": "Instant Foods",
            "Cháo gói, cháo tươi": "Instant Foods",
            "Bún các loại": "Instant Foods",
            "Nui các loại": "Instant Foods",
            "Bánh tráng các loại": "Instant Foods",
            "Bánh phồng, bánh đa": "Instant Foods",
            "Bánh gạo Hàn Quốc": "Cakes",

            # Gia vị, phụ gia, dầu
            "Nước mắm": "Seasonings",
            "Nước tương": "Seasonings",
            "Tương, chao các loại": "Seasonings",
            "Tương ớt - đen, mayonnaise": "Seasonings",
            "Dầu ăn": "Seasonings",
            "Dầu hào, giấm, bơ": "Seasonings",
            "Gia vị nêm sẵn": "Seasonings",
            "Muối": "Seasonings",
            "Hạt nêm, bột ngọt, bột canh": "Seasonings",
            "Tiêu, sa tế, ớt bột": "Seasonings",
            "Bột nghệ, tỏi, hồi, quế,...": "Seasonings",
            "Nước chấm, mắm": "Seasonings",
            "Mật ong, bột nghệ": "Seasonings",

            # Sữa & các sản phẩm từ sữa
            "Sữa tươi": "Milk",
            "Sữa đặc": "Milk",
            "Sữa pha sẵn": "Milk",
            "Sữa hạt, sữa đậu": "Milk",
            "Sữa ca cao, lúa mạch": "Milk",
            "Sữa trái cây, trà sữa": "Milk",
            "Sữa chua ăn": "Yogurt",
            "Sữa chua uống liền": "Yogurt",
            "Bơ sữa, phô mai": "Milk",

            # Đồ uống
            "Bia, nước có cồn": "Alcoholic Beverages",
            "Rượu": "Alcoholic Beverages",
            "Nước trà": "Beverages",
            "Nước ngọt": "Beverages",
            "Nước ép trái cây": "Beverages",
            "Nước yến": "Beverages",
            "Nước tăng lực, bù khoáng": "Beverages",
            "Nước suối": "Beverages",
            "Cà phê hoà tan": "Beverages",
            "Cà phê pha phin": "Beverages",
            "Cà phê lon": "Beverages",
            "Trà khô, túi lọc": "Beverages",

            # Bánh kẹo, snack
            "Bánh tươi, Sandwich": "Cakes",
            "Bánh bông lan": "Cakes",
            "Bánh quy": "Cakes",
            "Bánh snack, rong biển": "Snacks",
            "Bánh Chocopie": "Cakes",
            "Bánh gạo": "Cakes",
            "Bánh quế": "Cakes",
            "Bánh que": "Cakes",
            "Bánh xốp": "Cakes",
            "Kẹo cứng": "Candies",
            "Kẹo dẻo, kẹo marshmallow": "Candies",
            "Kẹo singum": "Candies",
            "Socola": "Candies",
            "Trái cây sấy": "Dried Fruits",
            "Hạt khô": "Dried Fruits",
            "Rong biển các loại": "Snacks",
            "Rau câu, thạch dừa": "Fruit Jam",
            "Mứt trái cây": "Fruit Jam",
            "Cơm cháy, bánh tráng": "Snacks",

            # Món ăn chế biến sẵn, đông lạnh
            "Làm sẵn, ăn liền": "Instant Foods",
            "Sơ chế, tẩm ướp": "Instant Foods",
            "Nước lẩu, viên thả lẩu": "Instant Foods",
            "Kim chi, đồ chua": "Instant Foods",
            "Mandu, há cảo, sủi cảo": "Instant Foods",
            "Bánh bao, bánh mì, pizza": "Instant Foods",
            "Kem cây, kem hộp": "Ice Cream & Cheese",
            "Bánh flan, thạch, chè": "Cakes",
            "Trái cây hộp, siro": "Fruit Jam",

            # Khác
            "Cá mắm, dưa mắm": "Seasonings",
            "Đường": "Seasonings",
            "Nước cốt dừa lon": "Seasonings",
            "Sữa chua uống": "Yogurt",
            "Khô chế biến sẵn": "Instant Foods"
        }

        categories = []
        if data.get("data"):
            menus = data["data"]["menus"]
            for menu in menus:
                print(f"Danh mục cha: {menu['name']}")
                for child in menu.get("childrens", []):
                    # print(f"{child['name']} (ID: {child['id']}, URL: {child['url']})")
                    category = categories_mapping.get(child["name"])

                    if child["name"] in valid_titles:
                        categories.append({
                            "title": category,
                            "link": child["url"]
                        })
                        print(f"category: {category} (ID: {child['id']}, URL: {child['url']})")
                    else:
                        print(f"  {child['name']} (ID: {child['id']}, URL: {child['url']}) - Bỏ qua")
        else:
            print("No data intercepted")

        return categories

    async def fetch_stores_by_province(self, province_id: int) -> List[Dict]:
        try:
            await self.page.goto("https://www.bachhoaxanh.com/he-thong-cua-hang", wait_until="domcontentloaded", timeout=20000)
        except Exception as e:
            print("Failed to load page:", e)
            return []
        if not self.token:
            raise ValueError("Missing Bearer token. You must intercept it first before making API calls.")

        cookies = await self.context.cookies()
        ck_bhx_cookie = next((c["value"] for c in cookies if c["name"] == "ck_bhx_us_log"), None)

        deviceid = None
        if ck_bhx_cookie:
            try:
                decoded = unquote(ck_bhx_cookie)
                deviceid = json.loads(decoded).get("did")
            except Exception as e:
                print("Failed to parse deviceid from cookie:", e)

        print("Extracted deviceid:", deviceid)

        headers = {
            "Authorization": self.token,
            "xapikey": "bhx-api-core-2022",
            "platform": "webnew",
            "reversehost": "http://bhxapi.live",
            "origin": "https://www.bachhoaxanh.com",
            "referer": "https://www.bachhoaxanh.com/he-thong-cua-hang",
            "referer-url": "https://www.bachhoaxanh.com/he-thong-cua-hang",
            "content-type": "application/json",
            "deviceid": str(deviceid) if deviceid else str(uuid.uuid4()),
        }

        stores_list = []
        page_index = 0
        page_size = 50

        while True:
            url = (
                "https://apibhx.tgdd.vn/Location/V2/GetStoresByLocation"
                f"?provinceId={province_id}&districtId=0&wardId=0&pageSize={page_size}&pageIndex={page_index}"
            )
            print(f"Sending GET to GetStoresByLocation with provinceId = {province_id}, pageIndex = {page_index}")

            try:
                res = session.get(url, headers=headers, timeout=15)
            except Exception as e:
                print(f"Request failed: {e}")
                break

            if res.status_code != 200:
                print(f"Failed to fetch stores for province {province_id}, status = {res.status_code}")
                break

            data = res.json()
            stores = data.get("data", {}).get("stores", [])
            total = data.get("data", {}).get("total", 0)

            if not stores:
                print(f"No more stores found on page {page_index}.")
                break

            for store in stores:
                store_dict = {
                    "store_id": store["storeId"],
                    "chain": self.chain,
                    "name": "Bách Hóa Xanh",
                    "lat": store["lat"],
                    "lon": store["lng"],
                    "address": store["storeLocation"],
                    "provinceId": store["provinceId"],
                    "districtId": store["districtId"],
                    "wardId": store["wardId"],
                }
                await upsert_branch(store_dict)
                stores_list.append(store_dict)
                
            print(f"Found {len(stores)} stores on page {page_index}.")
            page_index += 1

            if len(stores_list) >= total:
                print(f"All {total} stores fetched for province {province_id}.")
                break

        print(f"Total stores fetched: {len(stores_list)}")
        return stores_list

    async def crawl_branches(self):
        
        async with self.page.expect_response("**/Location/V2/GetFull**") as resp_info:
            await self.page.goto("https://www.bachhoaxanh.com/he-thong-cua-hang", timeout=30000)

        response = await resp_info.value
        if response.status != 200:
            print(f"Failed to fetch categories: {response.status}")
            return []

        data = await response.json()
        provinces = data["data"]["provinces"]
        for province in provinces:
            province_id = province["id"]
            print(f"Fetching stores for province ID: {province_id} - {province['name']}")
            stores = await self.fetch_stores_by_province(province_id)
                # You can save the store data to your database here
        return []
    async def fetch_branches(self):
        meta_client = AsyncIOMotorClient("mongodb://103.172.79.235:27017")
        meta_db = meta_client.metadata_db_v3
        store_branches = meta_db.store_branches

        ## Get all branches that have chain = bhx
        branches = await store_branches.find({"chain": self.chain}).to_list(length=None)
        print(f"Found {len(branches)} branches for chain {self.chain}")
        return branches
    async def crawl_prices(self):
        branches = await self.fetch_branches()
        try:
            await self.page.goto("https://www.bachhoaxanh.com/he-thong-cua-hang", wait_until="domcontentloaded", timeout=20000)
        except Exception as e:
            print("Failed to load page:", e)
            return []
        if not self.token:
            raise ValueError("Missing Bearer token. You must intercept it first before making API calls.")
        cookies = await self.context.cookies()
        ck_bhx_cookie = next((c["value"] for c in cookies if c["name"] == "ck_bhx_us_log"), None)

        deviceid = None
        if ck_bhx_cookie:
            try:
                decoded = unquote(ck_bhx_cookie)
                deviceid = json.loads(decoded).get("did")
            except Exception as e:
                print("Failed to parse deviceid:", e)

        headers_base = {
            "Authorization": self.token,
            "xapikey": "bhx-api-core-2022",
            "platform": "webnew",
            "reversehost": "http://bhxapi.live",
            "origin": "https://www.bachhoaxanh.com",
            "referer": "",
            "referer-url": "",
            "content-type": "application/json",
            "deviceid": str(deviceid) if deviceid else str(uuid.uuid4()),
        }
        # filter branches by provinceId in 3 and 109
        branches = [branch for branch in branches if branch["provinceId"] in [3, 109]]
        for branch in branches:
            if branch["store_id"] == 14623:
                continue
            print(f"Fetching prices for branch {branch['store_id']}")
            categories = await self.fetch_categories()
            if not categories:
                print("No categories found.")
                return
            print(f"Found {len(categories)} categories")
            for cat in categories:
                page_index = 1
                products = []
                while True:
                    url = (
                        "https://apibhx.tgdd.vn/Category/V2/GetCate"
                        f"?provinceId={branch['provinceId']}&wardId={branch['wardId']}"
                        f"&districtId={branch['districtId']}&storeId={branch['store_id']}"
                        f"&categoryUrl={quote(cat['link'].strip('/'))}"
                        f"&isMobile=true&isV2=true&pageSize={1000}&pageIndex={page_index}"
                    )

                    headers = headers_base.copy()
                    headers["referer"] = f"https://www.bachhoaxanh.com/{cat['link'].strip('/')}"
                    headers["referer-url"] = headers["referer"]

                    try:
                        res = session.get(url, headers=headers)
                    except Exception as e:
                        print(f"Request failed: {e}")
                        break

                    if res.status_code != 200:
                        print(f"Failed [{res.status_code}] for {cat['title']} at store {branch['store_id']}")
                        break

                    data = res.json()

                
                    batch = data.get("data", {}).get("products", [])
                    total = data.get("data", {}).get("total", 0)
                    
                    for product in batch:
                        english_name = translate_vi2en(product.get("name", ""))
                        if not english_name:
                            print(f"Failed to translate name: {item.get('name', '')}")
                            continue
                        price_info = extract_best_price(product)
                        token_ngrams = generate_token_ngrams(english_name, 2)
                        product_data = {
                            "sku": product["id"],
                            "name": product["name"],
                            "name_en": english_name,
                            "unit": price_info["unit"].lower(),
                            "netUnitValue": price_info["netUnitValue"], 
                            "token_ngrams": token_ngrams,
                            "category": cat["title"],
                            "store_id": branch["_id"],
                            "url": f"https://www.bachhoaxanh.com{product['url']}",
                            "image": product["avatar"],
                            "promotion": product.get("promotionText", ""),
                            "price": price_info["price"],
                            "sysPrice": price_info["sysPrice"],
                            "discountPercent": price_info["discountPercent"],
                            "date_begin": price_info["date_begin"],
                            "date_end": price_info["date_end"],
                            "crawled_at": datetime.utcnow().isoformat(),
                        }
                        # print(f"Product data: {product_data}")
                        await upsert_product(product_data, cat["title"])
                    products.extend(batch)

                    if len(products) >= total:
                        break

                    page_index += 1

                print(f"Fetched {len(products)} / {total} products in category '{cat['title']}' for store {branch['store_id']}")
                # for product in products:
                #     english_name = translate_vi2en(product.get("name", ""))
                #     if not english_name:
                #         print(f"Failed to translate name: {item.get('name', '')}")
                #         continue
                #     # check if have number before "hũ" "chai" "gói" "hộp" "túi" "lon" "thùng" "lốc" in name
                #     # match = re.search(r"(\d+)\s*(hũ|chai|gói|hộp|túi|lon|thùng|lốc)", product["name"].lower())
                #     price_info = extract_best_price(product)
                #     # print(f"Product name: {product['name']}, unit: {price_info['unit']}, netUnitValue: {price_info['netUnitValue']}")
                #     product_data = {
                #         "sku": product["id"],
                #         "name": product["name"],
                #         "name_en": english_name,
                #         "unit": price_info["unit"].lower(),  # giữ nguyên unit gốc
                #         "netUnitValue": price_info["netUnitValue"],  # <-- bổ sung trường này
                #         "category": cat["title"],
                #         "store_id": branch["_id"],
                #         "ts": datetime.now(),
                #         "url": f"https://www.bachhoaxanh.com{product['url']}",
                #         "image": product["avatar"],
                #         "promotion": product.get("promotionText", ""),
                #         "price": price_info["price"],
                #         "sysPrice": price_info["sysPrice"],
                #         "discountPercent": price_info["discountPercent"],
                #         "date_begin": price_info["date_begin"],
                #         "date_end": price_info["date_end"],
                #         "crawled_at": datetime.utcnow().isoformat(),
                #     }
                #     # upsert_product(product_data)
                #     await upsert_product(product_data, cat["title"])
                # print(f"Fetched {len(products)} products for category {cat['title']} at store {branch['store_id']}")



            
    async def close(self):
        await self.browser.close()
        await self.playwright.stop()