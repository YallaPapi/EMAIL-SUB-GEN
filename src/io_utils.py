import os
import re
from typing import Dict, List, Tuple
import pandas as pd


CANONICAL_COLUMNS = [
    "firstName",
    "title",
    "email",
    "organization_website_url",
    "city",
    "industry",
    "linkedin_url",
]


SNAKE_TO_CANONICAL = {
    "first_name": "firstName",
    "job_title": "title",
}


def read_csv(input_path: str) -> pd.DataFrame:
    return pd.read_csv(input_path, dtype=str)


def write_csv(df: pd.DataFrame, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)


def normalize_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[int, Dict[str, str]]]:
    df_cols = {c: c for c in df.columns}
    # Map known snake_case to canonical columns (add if missing)
    for snake, canon in SNAKE_TO_CANONICAL.items():
        if snake in df_cols and canon not in df_cols:
            df[canon] = df[snake]
    if "firstName" not in df.columns and "first_name" in df.columns:
        df["firstName"] = df["first_name"]

    row_maps: Dict[int, Dict[str, str]] = {}
    for idx, row in df.iterrows():
        entry: Dict[str, str] = {}
        for key in CANONICAL_COLUMNS:
            val = row.get(key, "")
            entry[key] = "" if pd.isna(val) else str(val)
        entry["shortName"] = derive_short_name(entry.get("organization_website_url", ""))
        row_maps[idx] = entry

    return df, row_maps


_COMPANY_STRIP_WORDS = re.compile(
    r"\b(incorporated|inc|llc|ltd|limited|insurance|agency|corp|corporation|group|co|company|holdings)\b",
    re.IGNORECASE,
)


def derive_short_name(website_url: str) -> str:
    if not website_url:
        return ""
    m = re.search(r"https?://([^/]+)/?", website_url)
    domain = m.group(1) if m else website_url
    domain = re.sub(r"^(www\.|m\.)", "", domain, flags=re.IGNORECASE)
    parts = domain.split('.')
    label = parts[-2] if len(parts) >= 2 else parts[0]
    name = _COMPANY_STRIP_WORDS.sub("", label)
    name = re.sub(r"[^A-Za-z0-9]+", " ", name).strip()
    return name.title()


def sanitize_text(text: str) -> str:
    if text is None:
        return ""
    t = str(text).strip().strip('`').strip('"').strip("'")
    t = re.sub(r"\s+", " ", t).strip()
    return t

