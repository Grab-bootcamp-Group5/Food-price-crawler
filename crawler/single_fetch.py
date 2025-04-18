import asyncio
import json
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from pydantic import BaseModel

from datetime import datetime

class Product(BaseModel):
    store: str
    url: str
    sku: str
    title: str
    price: float
    unit: str
    crawled_at: datetime
    
def merge_city_ward(citys, wards):
    result = {}
    for city_id, city_info in citys.items():
        result[city_id] = {
            "name": city_info["name"],
            "dsquan": {}
        }
        for dsquan_id, dsquan_name in city_info["dsquan"].items():
            result[city_id]["dsquan"][dsquan_id] = {
                "name": dsquan_name,
                "wards": wards.get(dsquan_id, {})
            }
    return result


def parse_stores(html, district_id, ward_id):
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for li in soup.select("li"):
        name = li.select_one("strong") or li.select_one(".store-name")
        address = li.select_one(".store-address")
        phone = li.select_one(".store-phone")
        results.append({
            "district_id": district_id,
            "ward_id": ward_id,
            "name": name.text.strip() if name else None,
            "address": address.text.strip() if address else None,
            "phone": phone.text.strip() if phone else None
        })
    return results


# --- PREPARE BROWSER (với store đã chọn) ---
async def prepare_browser_with_store(context, store_id: str):
    page = await context.new_page()
    await page.goto("https://cooponline.vn", timeout=30000)

    # ⚡️ Chọn store bằng localStorage (như user chọn)
    await page.evaluate(f"""
        () => {{
            localStorage.setItem('store', '{store_id}');
            location.reload();
        }}
    """)
    await page.wait_for_load_state("networkidle")
    await page.wait_for_selector("#wrapper")
    return page


# --- LẤY DANH SÁCH CITY / WARD ---
async def fetch_citys_and_wards(page):
    wrapper = await page.query_selector("#wrapper")
    vue_instance = await wrapper.evaluate_handle("node => node.__vue__")
    citys = await vue_instance.evaluate("vm => vm.citys")
    wards = await vue_instance.evaluate("vm => vm.wards")
    return merge_city_ward(citys, wards)


# --- LẤY STORE THEO QUẬN / PHƯỜNG ---
async def fetch_stores_by_browser_context(page, district_id, ward_id):
    print(f"📦 Fetching stores for district {district_id} - ward {ward_id}...")

    js_response = await page.evaluate(
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
            "district_id": str(district_id),
            "ward_id": str(ward_id)
        }
    )

    print(js_response)
    return parse_stores(js_response, district_id, ward_id)


async def crawl_all_stores(page, citys_merged):
    results = []
    for city in citys_merged.values():
        for district_id, district in city["dsquan"].items():
            for ward_id in district["wards"]:
                stores = await fetch_stores_by_browser_context(page, district_id, ward_id)
                results.extend(stores)
    return results

def main
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        page = await prepare_browser_with_store(context, store_id="132")

        print("Fetching citys & wards...")
        citys_merged = await fetch_citys_and_wards(page)

        print("Done. Now fetching all stores...")
        stores = await crawl_all_stores(page, citys_merged)

        print(f"Collected {len(stores)} stores.")
        with open("coop_stores.json", "w", encoding="utf-8") as f:
            json.dump(stores, f, indent=2, ensure_ascii=False)

        await browser.close()

        
if __name__ == "__main__":
    asyncio.run(main())
