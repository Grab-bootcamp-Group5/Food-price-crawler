import asyncio
import json
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from pydantic import BaseModel

from datetime import datetime
    
def merge_city_ward(citys, wards):
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


def parse_stores(html, city_name, district_name, ward_name):
    soup = BeautifulSoup(html, "html.parser")
    results = []
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
        city_name = city["name"]
        for district_id, district in city["dsquan"].items():
            district_name = district["name"]
            for ward_id, ward_name in district["wards"].items():
                print(f"📦 Crawling: {city_name} – {district_name} – {ward_name}")
                html = await page.evaluate(
                    """async ({ district_id, ward_id }) => {
                        const formData = new URLSearchParams();
                        formData.append("request", "w_load_stores");
                        formData.append("selectDistrict", district_id);
                        formData.append("selectWard", ward_id);

                        const res = await fetch("https://cooponline.vn/ajax/", {
                            method: "POST",
                            headers: {
                                "Content-Type": "application/x-www-form-urlencoded"
                            },
                            body: formData
                        });
                        return await res.text();
                    }""",
                    {"district_id": str(district_id), "ward_id": str(ward_id)}
                )
                stores = parse_stores(html, city_name, district_name, ward_name)
                results.extend(stores)
    return results
