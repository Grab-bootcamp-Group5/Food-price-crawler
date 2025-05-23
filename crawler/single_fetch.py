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
import json
from urllib.parse import unquote
import httpx


class CoopOnlineCrawler():
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

    async def fetch_categories(self) -> List[Dict]:
        html = await self.page.content()
        soup = BeautifulSoup(html, "html.parser")
        categories = []

        # Mỗi li chứa danh mục lớn
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

class BHXOnlineCrawler:
    chain = "bhxonline"

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
                stores_list.append({
                    "storeId": store["storeId"],
                    "lat": store["lat"],
                    "lng": store["lng"],
                    "storeLocation": store["storeLocation"],
                    "provinceId": store["provinceId"],
                    "districtId": store["districtId"],
                    "wardId": store["wardId"],
                })
                # print(f"Store ID: {store['storeId']}, Location: {store['storeLocation']}")
                # print(f"Latitude: {store['lat']}, Longitude: {store['lng']}")

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



async def crawl_prices():
    crawler = BHXOnlineCrawler()
    try:
        await crawler.init()
        branches = await crawler.crawl_branches()
        print(f"Found {len(branches)} branches")
        # categories = await crawler.fetch_categories()
        # print(f"Found {len(categories)} categories")
        # for category in categories:
        #     print(f"Category: {category['title']}, Link: {category['link']}")
    finally:
        await crawler.close()
    # crawler = CoopOnlineCrawler(store_id=571)
    # try:
    #     await crawler.init()
    #     categories = await crawler.fetch_categories()
    #     print(f"Found {len(categories)} categories")
    #     for category in categories:
    #         print(f"Category: {category['title']}, Link: {category['link']}")
    # finally:
    #     await crawler.close()
async def intercept_response():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        result = {}

        async def handle_response(response):
            if "Menu/GetMenuV2" in response.url:
                print("Intercepted API Response:", response.url)
                try:
                    json_data = await response.json()
                    result["data"] = json_data
                except:
                    print("⚠️ Could not decode JSON")

        page.on("response", handle_response)

        await page.goto("https://www.bachhoaxanh.com/banh-caramen-banh-flan", timeout=30000)
        await page.wait_for_timeout(8000)

        await browser.close()
        if result.get("data"):
            menus = result["data"]["data"]["menus"]
            for menu in menus:
                print(f"Danh mục cha: {menu['name']}")
                for child in menu.get("childrens", []):
                    print(f"  ↳ {child['name']} (ID: {child['id']}, URL: {child['url']})")
        else:
            print("No data intercepted")
if __name__ == "__main__":
    asyncio.run(crawl_prices())
    # asyncio.run(intercept_response())

