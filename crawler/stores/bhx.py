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
from curl_cffi.requests import Session
import httpx
from motor.motor_asyncio import AsyncIOMotorClient
import torch
import uuid

from dotenv import load_dotenv
load_dotenv()
from curl_cffi.requests import Session
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import torch
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from db import upsert_product
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
            "Rau, củ làm sẵn": "Prepared Vegetables",
            "Rau củ đông lạnh": "Instant Foods",

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
            async with httpx.AsyncClient() as client:
                res = await client.get(url, headers=headers)

            if res.status_code != 200:
                print(f"Failed to fetch stores for province {province_id}, status = {res.status_code}")
                print(f"Body: {res.text}")
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
        meta_db = meta_client.metadata_db
        category_shard_meta = meta_db.category_shards
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

        branch = branches[0]
        print(f"Fetching prices for branch {branch['store_id']}")
        categories = await self.fetch_categories()
        if not categories:
            print("No categories found.")
            return
        print(f"Found {len(categories)} categories")
        for cat in categories:
            url = (
                "https://apibhx.tgdd.vn/Category/V2/GetCate"
                f"?provinceId={branch['provinceId']}&wardId={branch['wardId']}"
                f"&districtId={branch['districtId']}&storeId={branch['store_id']}"
                f"&categoryUrl={quote(cat['link'].strip('/'))}"
                f"&isMobile=true&isV2=true&pageSize=100"
            )

            headers = headers_base.copy()
            headers["referer"] = f"https://www.bachhoaxanh.com/{cat['link'].strip('/')}"
            headers["referer-url"] = headers["referer"]

            try:
                res = session.get(url, headers=headers)
            except Exception as e:
                print(f"Request failed: {e}")
                continue

            if res.status_code != 200:
                print(f"Failed [{res.status_code}] for {cat['title']} at store {branch['store_id']}")
                continue

            data = res.json()

            products = data.get("data", {}).get("products", [])
            for product in products:
                english_name = translate_vi2en(product.get("name", ""))
                if not english_name:
                    print(f"Failed to translate name: {item.get('name', '')}")
                    continue
                productPrices = product.get("productPrices", [])
                product = {
                    "product_id": product["id"],
                    "name": product["name"],
                    "name_ev": english_name,
                    "unit": product["unit"],
                    "category": cat["title"],
                    "store_id": branch["_id"],
                    "ts": datetime.now(),
                    "url": f"https://www.bachhoaxanh.com{product['url']}",
                    "image": product["avatar"],
                    "promotion": product["promotionText"],
                    "price": productPrices[0]["price"] if productPrices else None,
                    "sysPrice": productPrices[0]["sysPrice"] if productPrices else None,
                    "dicountPercent": productPrices[0]["discountPercent"] if productPrices else None,
                    "date_begin": 
                    "date_end":
                    "crawled_at": datetime.utcnow().isoformat(),

                }
            
    async def close(self):
        await self.browser.close()
        await self.playwright.stop()