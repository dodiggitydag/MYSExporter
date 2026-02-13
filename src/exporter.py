import requests
import csv
import re
import logging
from typing import List, Dict, Any, Optional

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

logger = logging.getLogger(__name__)


def fetch_data(api_url: str, api_username: str, api_password: str, api_show_code: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    headers = {}
    # Use Basic authentication with provided username and password
    import base64
    credentials = base64.b64encode(f"{api_username}:{api_password}".encode()).decode()
    headers["Authorization"] = f"Basic {credentials}"
    # Add show code as a parameter
    params = params or {}
    params["showCode"] = api_show_code
    resp = requests.get(api_url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # Normalise common envelope shapes
    if isinstance(data, dict):
        if "items" in data and isinstance(data["items"], list):
            return data["items"]
        if "data" in data and isinstance(data["data"], list):
            return data["data"]
        # If top-level dict with numeric keys or single object, wrap
        # If it's a single record dict, return [data]
        # Heuristic: if there are keys and none are lists of dicts, return [data]
        return [data]
    if isinstance(data, list):
        return data
    # Fallback
    return []


def detect_available_fields(records: List[Dict[str, Any]]) -> List[str]:
    fields = set()
    for r in records:
        if isinstance(r, dict):
            fields.update(r.keys())
    # Exclude columns whose key contains 'email' (case-insensitive)
    return sorted([f for f in fields if "email" not in f.lower()])


def sanitize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in record.items():
        # Skip any email-key fields entirely
        if "email" in str(k).lower():
            continue
        if isinstance(v, str) and EMAIL_RE.search(v):
            out[k] = ""
        else:
            out[k] = v
    return out


def filter_and_sanitize(records: List[Dict[str, Any]], fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    sanitized = [sanitize_record(r) for r in records]
    if not fields:
        return sanitized
    # Only keep intersection of requested fields and available keys
    out = []
    for r in sanitized:
        out.append({k: r.get(k, "") for k in fields if k in r})
    return out


def export_csv(records: List[Dict[str, Any]], out_path: str, fields: Optional[List[str]] = None) -> None:
    if not records:
        logger.info("No records to export")
        return
    if fields is None:
        # Derive header from first record (stable order by sorted keys)
        fields = sorted({k for r in records for k in r.keys()})
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for r in records:
            writer.writerow({k: (v if v is not None else "") for k, v in r.items()})


def run_export(api_url: str, api_username: str, api_password: str, api_show_code: str, output_file: str, requested_fields: Optional[List[str]] = None, params: Optional[Dict[str, Any]] = None) -> None:
    logger.info("Fetching data from API: %s", api_url)
    records = fetch_data(api_url, api_username=api_username, api_password=api_password, api_show_code=api_show_code, params=params)
    logger.info("Fetched %d records", len(records))
    available = detect_available_fields(records)
    logger.info("Available fields (excluding email keys): %s", ", ".join(available))
    if requested_fields:
        # intersect
        fields = [f for f in requested_fields if f in available]
    else:
        fields = available
    filtered = filter_and_sanitize(records, fields)
    export_csv(filtered, output_file, fields)
    logger.info("Exported %d records to %s", len(filtered), output_file)
