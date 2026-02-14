import requests
import csv
import re
import logging
from typing import List, Dict, Any, Optional

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

logger = logging.getLogger(__name__)


def fetch_data(api_username: str, api_password: str, api_show_code: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    user_agent = "mysexporter/1.0.0"
    import base64
    
    # Step 1: Get GUID from Authorize endpoint
    headers = {}
    credentials = base64.b64encode(f"{api_username}:{api_password}".encode()).decode()
    headers["Authorization"] = f"Basic {credentials}"
    headers["Content-Type"] = "application/json"
    headers["User-Agent"] = user_agent

    params = params or {}
    params["showCode"] = api_show_code
    
    logger.info("Requesting authorization GUID from Authorize endpoint")
    auth_resp = requests.get("https://api.mapyourshow.com/mysRest/v2/Authorize", headers=headers, params=params, timeout=30)
    auth_resp.raise_for_status()
    auth_data = auth_resp.json()
    
    # Extract GUID from response
    if isinstance(auth_data, list) and len(auth_data) > 0:
        guid_value = auth_data[0].get("mysGUID")
    elif isinstance(auth_data, dict):
        guid_value = auth_data.get("mysGUID")
    else:
        raise ValueError("Could not extract GUID from authorization response")
    
    if not guid_value:
        raise ValueError("GUID value is empty from authorization response")
    
    logger.info("Obtained GUID: %s", guid_value)
    
    # Step 2: Use GUID to fetch data from Sessions/Proposals endpoint
    data_headers = {
        "Authorization": f"Bearer {guid_value}",
        "User-Agent": user_agent,
    }
    
    data_params = {"conferenceid": api_show_code}
    
    logger.info("Requesting data from Sessions/Proposals endpoint")
    data_resp = requests.get("https://api.mapyourshow.com/mysRest/v2/Sessions/Proposals", headers=data_headers, params=data_params, timeout=30)
    data_resp.raise_for_status()
    data = data_resp.json()
    
    if isinstance(data, list) and len(data) == 1:
        if "proposals" in data[0] and isinstance(data[0]["proposals"], list):
            return data[0]["proposals"]

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


def run_export(api_username: str, api_password: str, api_show_code: str, output_file: str, requested_fields: Optional[List[str]] = None, params: Optional[Dict[str, Any]] = None) -> None:
    logger.info("Fetching data from API")
    records = fetch_data(api_username=api_username, api_password=api_password, api_show_code=api_show_code, params=params)
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
