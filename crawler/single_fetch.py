import asyncio
import json
from typing import List, Dict
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import asyncpg

import sys
import os
import re
import unicodedata


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

class BHXOnlineCrawler():
    chain = "bhxonline"

    def __init__(self):
        self.browser = None
        self.context = None
        self.page = None

    async def init(self):
        p = await async_playwright().start()
        self.browser = await p.chromium.launch(headless=True)
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()

        await self.page.goto("https://www.bachhoaxanh.com/", timeout=30000)

    async def fetch_categories(self) -> List[Dict]:
        ## Extract from file bhx.html and get  class mb-2 flex flex-wrap
        bhxhtml = open("bhx.html", "r", encoding="utf-8").read()
        soup = BeautifulSoup(bhxhtml, "html.parser")
        # Then get class "mb-2 flex flex-wrap"
        category_items = soup.select("div.mb-2.flex.flex-wrap")
        print(f"Found {len(category_items)} categories")
        for category in category_items:
            # print(f"Category: {category.prettify()}")
            div_tags = category.select("div.cate.w-full.bg-white")

            for tag in div_tags:
                title = tag.text.strip()
                print(f"Category: {title}")

        categories = []




        # # Mỗi li chứa danh mục lớn
        # category_items = soup.select("li.item-vertical.with-sub-menu.hover")
        # for li in category_items:
        #     grid_blocks = li.select("div.col-lg-12.col-md-12.col-sm-12")
        #     for block in grid_blocks:
        #         static_menus = block.select("div.static-menu")
        #         for menu in static_menus:
        #             # Lấy thẻ a.main-menu đầu tiên trong menu
        #             a_tag = menu.select_one("a.main-menu")
        #             if a_tag:
        #                 categories.append({
        #                     "title": a_tag.text.strip(),
        #                     "link": a_tag.get("href")
        #                 })

        return categories

    async def close(self):
        if self.browser:
            await self.browser.close()

async def crawl_prices():
    crawler = BHXOnlineCrawler()
    try:
        await crawler.init()
        categories = await crawler.fetch_categories()
        print(f"Found {len(categories)} categories")
        for category in categories:
            print(f"Category: {category['title']}, Link: {category['link']}")
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

if __name__ == "__main__":
    asyncio.run(crawl_prices())

