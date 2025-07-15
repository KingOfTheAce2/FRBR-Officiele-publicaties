import os
import json
import time
import random
import requests
from pathlib import Path
from lxml import etree
from huggingface_hub import HfApi, create_repo

# ─────────────── CONFIG ─────────────── #
SRU_URL = "https://repository.overheid.nl/sru/"
CQL_QUERY = 'c.product-area=="officielepublicaties"'  # SRU 2.0 query
BATCH_SIZE = 100
STATE_FILE = "sru_state.json"
OUTPUT_JSONL = "output.jsonl"
SOURCE_NAME = "Officiële Publicaties"
HF_REPO_ID = "vGassen/Dutch-Officiele-Publicaties"
HF_TOKEN = os.getenv("HF_TOKEN")
RETRY_LIMIT = 5
TIMEOUT = 60
SLEEP_BETWEEN = 2

# ─────────────── HELPERS ─────────────── #
def load_state() -> int:
    if Path(STATE_FILE).exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("startRecord", 1)
    return 1

def save_state(start: int):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"startRecord": start}, f)

def fetch_batch(start: int) -> list[dict]:
    params = {
        "version": "2.0",
        "operation": "searchRetrieve",
        "query": CQL_QUERY,
        "startRecord": start,
        "maximumRecords": BATCH_SIZE,
        "recordSchema": "gzd",
        "httpAccept": "application/xml",
    }
    for attempt in range(RETRY_LIMIT):
        try:
            resp = requests.get(SRU_URL, params=params, timeout=TIMEOUT)
            resp.raise_for_status()
            root = etree.fromstring(resp.content)
            records = root.findall(".//{*}recordData")
            return records
        except Exception as e:
            time.sleep((2 ** attempt) + random.random())
            if attempt == RETRY_LIMIT - 1:
                raise e

def extract_content(record) -> dict | None:
    try:
        identifier = record.find(".//{*}identifier")
        url = identifier.text if identifier is not None else None
        content = etree.tostring(record, encoding="unicode", method="text")
        return {"URL": url, "Content": content.strip(), "Source": SOURCE_NAME}
    except Exception:
        return None

# ─────────────── MAIN ─────────────── #
def main():
    start = load_state()
    collected = []

    while True:
        records = fetch_batch(start)
        if not records:
            break
        for rec in records:
            data = extract_content(rec)
            if data:
                collected.append(data)
        start += len(records)
        save_state(start)
        with open(OUTPUT_JSONL, "a", encoding="utf-8") as f:
            for doc in collected:
                json.dump(doc, f, ensure_ascii=False)
                f.write("\n")
        collected.clear()
        time.sleep(SLEEP_BETWEEN)

    if HF_TOKEN:
        upload_to_hf()

def upload_to_hf():
    api = HfApi()
    create_repo(repo_id=HF_REPO_ID, repo_type="dataset", token=HF_TOKEN, exist_ok=True)
    api.upload_file(
        path_or_fileobj=OUTPUT_JSONL,
        path_in_repo="data/officielepublicaties.jsonl",
        repo_id=HF_REPO_ID,
        repo_type="dataset",
        token=HF_TOKEN
    )

if __name__ == "__main__":
    main()
