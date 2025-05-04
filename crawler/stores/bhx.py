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
from urllib.parse import unquote
import httpx

import torch
import uuid
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
        print(data)
        print(f"Fetched {len(data.get('Data', []))} categories")
        return data.get("Data", [])

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

    async def close(self):
        await self.browser.close()
        await self.playwright.stop()