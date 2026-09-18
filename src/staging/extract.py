import json
import logging
import os

from utils.api_client import APIClient

logger = logging.getLogger(__name__)


def _completed_offsets(staging_dir: str) -> set:
    completed = set()
    if not os.path.isdir(staging_dir):
        return completed

    for filename in os.listdir(staging_dir):
        if not filename.startswith("page_") or not filename.endswith(".json"):
            continue
        offset_str = filename[len("page_"):-len(".json")]
        if offset_str.isdigit():
            completed.add(int(offset_str))

    return completed


def _atomic_write_json(path: str, data: dict) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w") as f:
        json.dump(data, f)
    os.rename(tmp_path, path)


def ingest_table_to_volume(client: APIClient, endpoint: str, staging_dir: str, limit: int = 1000) -> None:
    os.makedirs(staging_dir, exist_ok=True)
    completed_offsets = _completed_offsets(staging_dir)

    offset = 0
    total = None
    pages_fetched = 0
    pages_skipped = 0
    pages_seen = 0

    while total is None or offset < total:
        page_path = os.path.join(staging_dir, f"page_{offset:07d}.json")

        if offset in completed_offsets:
            with open(page_path, "r") as f:
                page = json.load(f)
            pages_skipped += 1
        else:
            page = client.fetch_page(endpoint, limit=limit, offset=offset)
            _atomic_write_json(page_path, page)
            pages_fetched += 1

        total = page["total"]
        items_count = len(page["items"])
        pages_seen += 1

        if pages_seen % 10 == 0:
            logger.info(
                "endpoint=%s fetched=%d skipped=%d offset=%d total=%d",
                endpoint, pages_fetched, pages_skipped, offset, total,
            )

        offset += limit
        if items_count < limit:
            break

    logger.info(
        "endpoint=%s ingestion complete: fetched=%d skipped=%d final_offset=%d",
        endpoint, pages_fetched, pages_skipped, offset,
    )
