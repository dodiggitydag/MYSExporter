import requests
import csv
import re
import logging
from typing import List, Dict, Any, Optional
from .merger import merge_sessions

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
    """Sanitize a record by removing or clearing email addresses."""
    out = {}
    for k, v in record.items():
        # Skip any email-key fields entirely
        if "email" in str(k).lower():
            continue
        if isinstance(v, str) and EMAIL_RE.search(v):
            out[k] = ""
        elif EMAIL_RE.search(str(v)):
            out[k] = ""
        else:
            out[k] = v
    return out


def filter_and_sanitize(records: List[Dict[str, Any]], fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Filter records to only include specified fields and sanitize email addresses."""
    sanitized = [sanitize_record(r) for r in records]
    if not fields:
        return sanitized
    # Only keep intersection of requested fields and available keys
    out = []
    for r in sanitized:
        out.append({k: r.get(k, "") for k in fields if k in r})
    return out


import pandas as pd
import numpy as np
import ast
import html as ihtml

def transform_proposals_dataframe(
    df: pd.DataFrame,
    json_cols=None,
    html_col=None,
    remove_cols=None
) -> pd.DataFrame:
    """
    Applies the data cleaning and transformation logic to a DataFrame.
    Args:
        df: Input DataFrame
        json_cols: List of columns to expand as JSON-like columns
        html_col: Name of column to strip HTML from
        remove_cols: List of columns to remove if present
    Returns:
        Transformed DataFrame
    """
    if json_cols is None:
        json_cols = []
    if remove_cols is None:
        remove_cols = []

    def parse_py_literal(x):
        # Handle both scalar and array-like input for pd.isna(x)
        is_na = pd.isna(x)
        try:
            # If is_na is array-like, check if all elements are NA
            if hasattr(is_na, 'all'):
                if is_na.all():
                    return None
            else:
                if is_na:
                    return None
        except Exception:
            if is_na:
                return None
        if isinstance(x, (dict, list)):
            return x
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return None
        try:
            return ast.literal_eval(s)
        except Exception:
            return None

    def strip_html(text):
        if pd.isna(text):
            return np.nan
        s = str(text)
        if not s.strip():
            return np.nan
        s = ihtml.unescape(s)
        s = re.sub(r"(?is)<(script|style).*?>.*?</\\1>", " ", s)
        s = re.sub(r"(?s)<[^>]*>", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s if s else np.nan

    def drop_fully_empty_columns(df):
        tmp = df.copy()
        tmp = tmp.replace(r"^\\s*$", np.nan, regex=True)
        return tmp.dropna(axis=1, how="all")

    def normalize_json_column(df, col):
        if col not in df.columns:
            return df
        parsed = df[col].map(parse_py_literal)
        def row_to_flat(v):
            if v is None:
                return None
            if isinstance(v, dict):
                return {f"{col}." + str(k): v.get(k) for k in v.keys()}
            if isinstance(v, list):
                out = {}
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        for k, val in item.items():
                            out[f"{col}.{i}.{k}"] = val
                    else:
                        out[f"{col}.{i}"] = item
                return out
            return {f"{col}": v}
        flat = parsed.map(row_to_flat)
        if flat.dropna().empty:
            return df
        keys = set()
        for d in flat.dropna():
            keys.update(d.keys())
        keys = sorted(keys)
        new_df = pd.DataFrame(index=df.index, columns=keys)
        for idx, d in flat.items():
            if isinstance(d, dict):
                for k, v in d.items():
                    new_df.at[idx, k] = v
        out = df.drop(columns=[col]).copy()
        out = pd.concat([out, new_df], axis=1)
        return out

    # --- Begin transformation ---
    df = df.replace(r"^\\s*$", np.nan, regex=True)
    for jc in json_cols:
        df = normalize_json_column(df, jc)
    if html_col and html_col in df.columns:
        df[html_col] = df[html_col].map(strip_html)
    for column_name in remove_cols:
        if column_name in df.columns:
            df = df.drop(columns=[column_name])

    # Add LinkedIn search columns for proposalcontacts.0 and .1
    def create_linkedin_search_url(row, idx):
        first_name = row.get(f"proposalcontacts.{idx}.firstname", "")
        last_name = row.get(f"proposalcontacts.{idx}.lastname", "")
        if pd.isna(first_name) or pd.isna(last_name) or not str(first_name).strip() or not str(last_name).strip():
            return np.nan
        query = f"{first_name} {last_name}".strip()
        return f"https://www.linkedin.com/search/results/all/?keywords={query.replace(' ', '%20')}"
    df["proposalcontacts.0.LinkedInSearch"] = df.apply(lambda row: create_linkedin_search_url(row, 0), axis=1)
    df["proposalcontacts.1.LinkedInSearch"] = df.apply(lambda row: create_linkedin_search_url(row, 1), axis=1)

    df = drop_fully_empty_columns(df)
    df = df.dropna(axis=0, how="all")
    return df


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
    """Main function to run the export process. Fetches data from API, processes it, and merges into an XLSX workbook."""
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

    # Don't need this column no matter what
    if "conferenceid" in fields:
        fields.remove("conferenceid")

    # Make proposalid the first column if it exists
    if "proposalid" in fields:
        fields.remove("proposalid")
        fields.insert(0, "proposalid")

    if "type" in fields:
        fields.remove("type")
        fields.insert(1, "type")
    if "title" in fields:
        fields.remove("title")
        fields.insert(1, "title")
    if "track" in fields:
        fields.remove("track")
        fields.insert(1, "track")

    filtered = filter_and_sanitize(records, fields)

    # Convert records to DataFrame for further transformation before exporting to CSV
    import pandas as pd
    df = pd.DataFrame(filtered)

    # The JSON in these columns will be read and expanded into their own columns so that every row remains one submission record
    json_cols = [c for c in ["categories", "proposalcontacts"] if c in df.columns]
    html_col = "description" if "description" in df.columns else None

    # Remove columns
    remove_cols = [
        "categories.0.categorygroup","categories.0.categoryid", "categories.0.categorygroupid",
        "categories.1.categorygroup","categories.1.categoryid", "categories.1.categorygroupid",
        "categories.2.categorygroup","categories.2.categoryid", "categories.2.categorygroupid",
        "categories.3.categorygroup","categories.3.categoryid", "categories.3.categorygroupid",
        "proposalcontacts.0.middlename","proposalcontacts.0.contactid","proposalcontacts.0.phone","proposalcontacts.0.prefix",
        "proposalcontacts.1.middlename","proposalcontacts.1.contactid","proposalcontacts.1.phone","proposalcontacts.1.prefix",
        "proposalcontacts.2.middlename","proposalcontacts.2.contactid","proposalcontacts.2.phone","proposalcontacts.2.prefix",
        "proposalcontacts.3.middlename","proposalcontacts.3.contactid","proposalcontacts.3.phone","proposalcontacts.3.prefix",
        "alpha"
    ]
    df = transform_proposals_dataframe(df, json_cols=json_cols, html_col=html_col, remove_cols=remove_cols)
    # Merge into existing XLSX (or create on first run)
    records = df.fillna("").to_dict(orient="records")
    stats = merge_sessions(records, output_file)
    print(f"Sync complete: {stats}")
    logger.info("Sync complete: %s", stats)


