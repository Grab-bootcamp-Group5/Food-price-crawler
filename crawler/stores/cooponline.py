import asyncio, json, re
from typing import List, Dict
from bs4 import BeautifulSoup
from datetime import datetime
from playwright.async_api import async_playwright
from .base import BranchCrawler

class CoopOnlineCrawler(BranchCrawler):
    chain = "cooponline"

    def __init__(self, store_id="132"):
        self.store_id = store_id

    # ---------- helpers ----------
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
            # Try parse as JSON
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
                print(results[-1])
            return results
        except json.JSONDecodeError:
            # Fallback to HTML parse
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


    # ---------- playwright core ----------
    async def _prepare_page(self, context):
        page = await context.new_page()
        await page.goto("https://cooponline.vn", timeout=30000)
        await page.evaluate(f"""
            () => {{
              localStorage.setItem('store', '{self.store_id}');
              location.reload();
            }}
        """)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_selector("#wrapper")
        return page

    async def crawl_branches(self) -> List[Dict]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context()
            page = await self._prepare_page(ctx)

            vue = await (await page.query_selector("#wrapper")).evaluate_handle("el=>el.__vue__")
            citys = await vue.evaluate("vm=>vm.citys")
            wards = await vue.evaluate("vm=>vm.wards")
            merged = self._merge_city_ward(citys, wards)
            store_map = {}
            results=[]
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
                        print(f"📦 Crawling: {city['name']} – {district['name']} – {district['wards'][wid]}")
                        stores = self._parse_stores(html, city["name"], district["name"], district['wards'][wid])

                        for store in stores:
                            key = (store["id"], store["chain"])
                            store_map[key] = store
                            print(stores)
            final_results = list(store_map.values())
            print(f"Found {len(final_results)} stores")
            await browser.close()
        return final_results
