#!/usr/bin/env python3
"""
crawler.py — SRU 2.0 compliant crawler for Officiële Publicaties
- Paginates via `startRecord`
- Extracts identifier and full text content
- Stores batches as JSONL shards
- Uploads shards to Hugging Face dataset repo with resumable logic
"""
import os
import json
import time
import random
import logging
import tempfile
from typing import List
from pathlib import Path
from lxml import etree
import requests
from huggingface_hub import HfApi, create_repo

# ─────────────── CONFIG ─────────────── #
SRU_URL = "https://zoek.officielebekendmakingen.nl/sru/Search"
CQL_QUERY = 'c.product-area=="officielepublicaties"'
BATCH_SIZE = 1000
STATE_FILE = "sru_state.json"
SHARD_DIR = Path("shards")
SHARD_SIZE = 300
SOURCE_NAME = "Officiële Publicaties"
HF_REPO_ID = "vGassen/Dutch-Officiele-Publicaties"
HF_TOKEN = os.getenv("HF_TOKEN")
MAX_RETRIES = 5
TIMEOUT = 60
LOG_PATH = "crawler.log"

# ─────────────── LOGGER ─────────────── #
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("crawler")

# ─────────────── STATE ─────────────── #
def load_state() -> int:
    if Path(STATE_FILE).exists():
        return json.load(open(STATE_FILE)).get("start", 1)
    return 1

def save_state(start: int):
    with open(STATE_FILE, "w") as f:
        json.dump({"start": start}, f)

# ─────────────── FETCH + PARSE ─────────────── #
def fetch_batch(start: int) -> List[etree._Element]:
    params = {
        "version": "2.0",
        "operation": "searchRetrieve",
        "query": CQL_QUERY,
        "startRecord": start,
        "maximumRecords": BATCH_SIZE,
        "recordSchema": "gzd",
    }
    for attempt in range(1, MAX_RETRIES+1):
        try:
            r = requests.get(SRU_URL, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            tree = etree.fromstring(r.content)
            return tree.findall(".//{*}recordData")
        except Exception as e:
            wait = 2 ** attempt + random.random()
            log.warning(f"Fetch failed (attempt {attempt}): {e} – retrying in {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError("Failed to fetch batch after retries")

def extract_content(record: etree._Element) -> dict | None:
    try:
        url_el = record.find(".//{*}identifier")
        content = etree.tostring(record, encoding="unicode", method="text")
        return {
            "URL": url_el.text.strip() if url_el is not None else None,
            "Content": content.strip(),
            "Source": SOURCE_NAME
        }
    except Exception as e:
        log.error(f"Failed to extract record: {e}")
        return None

# ─────────────── STORAGE ─────────────── #
def write_shard(batch: List[dict], start_index: int) -> str:
    SHARD_DIR.mkdir(exist_ok=True)
    shard_path = SHARD_DIR / f"shard_{start_index:06d}_{start_index + len(batch):06d}.jsonl"
    with open(shard_path, "w", encoding="utf-8") as f:
        for rec in batch:
            json.dump(rec, f, ensure_ascii=False)
            f.write("\n")
    return str(shard_path)

# ─────────────── HF UPLOAD ─────────────── #
def upload_shard(local_path: str, remote_path: str):
    api = HfApi()
    create_repo(HF_REPO_ID, repo_type="dataset", exist_ok=True, token=HF_TOKEN)
    api.upload_file(
        path_or_fileobj=local_path,
        path_in_repo=remote_path,
        repo_id=HF_REPO_ID,
        repo_type="dataset",
        token=HF_TOKEN
    )
    log.info(f"Uploaded {remote_path} to {HF_REPO_ID}")

# ─────────────── MAIN LOOP ─────────────── #
def main():
    start = load_state()
    batch = []

    while True:
        log.info(f"Fetching batch from startRecord={start}")
        records = fetch_batch(start)
        if not records:
            log.info("No more records.")
            break

        for rec in records:
            doc = extract_content(rec)
            if doc:
                batch.append(doc)

            if len(batch) >= SHARD_SIZE:
                shard = write_shard(batch, start - len(records) + 1)
                upload_shard(shard, f"shards/{Path(shard).name}")
                batch.clear()

        start += len(records)
        save_state(start)
        time.sleep(2)

    if batch:
        shard = write_shard(batch, start - len(batch))
        upload_shard(shard, f"shards/{Path(shard).name}")
        batch.clear()

    log.info("Finished crawling.")

if __name__ == "__main__":
    main()
