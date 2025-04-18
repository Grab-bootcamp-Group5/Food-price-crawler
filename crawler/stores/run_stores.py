import asyncio, importlib
import typer
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from db import upsert_branch

app = typer.Typer(help="Branch crawler driver")

@app.command()
def run(domain: str = "cooponline"):
    """domain → 'cooponline' or future 'lotte'"""
    module = importlib.import_module(f"crawler.stores.{domain}")
    crawler_cls = next(
        c for c in module.__dict__.values()
        if isinstance(c, type) and hasattr(c, "chain") and c.chain == domain
    )
    crawler = crawler_cls()
    branches = asyncio.run(crawler.crawl_branches())
    for b in branches:
        asyncio.run(upsert_branch(b))  # ✅ properly executed
    print(f"Inserted/updated {len(branches)} branches in DB.")

if __name__ == "__main__":
    app()
