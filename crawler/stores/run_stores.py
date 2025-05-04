import asyncio, importlib
import typer
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from db import upsert_branch

app = typer.Typer(help="Branch crawler driver")
async def async_main(domain: str):
    module = importlib.import_module(f"crawler.stores.{domain}")

    try:
        crawler_cls = next(
            c for c in module.__dict__.values()
            if isinstance(c, type) and hasattr(c, "chain") and c.chain == domain
        )
    except StopIteration:
        print(f"Cannot find crawler for domain: {domain}")
        return
    crawler = crawler_cls()
    await crawler.init()
    branches = await crawler.crawl_branches()

    print(f"Inserted/updated {len(branches)} branches in DB.")

@app.command()
def run(domain: str = "cooponline"):
    """domain → 'cooponline' or future 'lotte'"""
    asyncio.run(async_main(domain))


if __name__ == "__main__":
    app()
