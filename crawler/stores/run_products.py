import asyncio, importlib
import typer
import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from db import upsert_product

app = typer.Typer(help="Branch crawler driver")

@app.command()
def run(domain: str = "cooponline"):
    """domain → 'cooponline' or future 'lotte'"""
    asyncio.run(async_main(domain))

async def async_main(domain: str):
    module = importlib.import_module(f"crawler.stores.{domain}")
    crawler_cls = next(
        c for c in module.__dict__.values()
        if isinstance(c, type) and hasattr(c, "chain") and c.chain == domain
    )
    crawler = crawler_cls()
    await crawler.init()
    await crawler.crawl_prices()



if __name__ == "__main__":
    app()
