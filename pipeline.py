import base64
import io
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
from playwright.sync_api import sync_playwright

# ----------------- CONFIG -----------------
ELECTION_URL = "https://web.sos.ky.gov/CandidateFilings/countyfilings.aspx?elecid=86"

# Files will be written to this folder in your repo (create automatically).
# Use "" for repo root, or "counties/" to keep things organized.
REPO_PATH_PREFIX = "counties/"

# "csv" or "xlsx"
OUTPUT_EXT = "csv"

# GitHub target (pre-filled for your repo)
GITHUB_OWNER = os.environ.get("GITHUB_OWNER", "alexbkirk")
GITHUB_REPO  = os.environ.get("GITHUB_REPO",  "county-candidate-filings")
GH_TOKEN     = os.environ.get("GH_TOKEN")  # required
# ------------------------------------------


def github_api(path: str, method: str = "GET", json_body: Optional[dict] = None):
    import requests
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{path}"
    headers = {
        "Authorization": f"Bearer {GH_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    resp = requests.request(method, url, headers=headers, json=json_body)
    return resp


def get_existing_sha(path: str) -> Optional[str]:
    resp = github_api(path, "GET")
    if resp.status_code == 200:
        return resp.json().get("sha")
    return None


def put_file(path: str, content_bytes: bytes, message: str, sha: Optional[str]):
    encoded = base64.b64encode(content_bytes).decode("utf-8")
    body = {"message": message, "content": encoded}
    if sha:
        body["sha"] = sha
    resp = github_api(path, "PUT", json_body=body)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"GitHub PUT failed for {path}: {resp.status_code} {resp.text}")


def ensure_folder(prefix: str):
    """Create a .keep file so the folder exists in the repo."""
    if not prefix:
        return
    keep_path = prefix.rstrip("/") + "/.keep"
    sha = get_existing_sha(keep_path)
    if sha is None:
        put_file(keep_path, b"", f"Create {prefix} folder", None)


def playwright_download_xlsx(dest_dir: Path) -> Path:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(accept_downloads=True)
        page.goto(ELECTION_URL, wait_until="domcontentloaded")

        selectors = [
            'text=/Download All Candidates/i',
            'text=/Download|Export|Excel|CSV/i',
            "a:has-text('Download')",
            "a:has-text('Export')",
            "button:has-text('Download')",
            "button:has-text('Export')",
            "input[type=submit]",
            "input[type=button]"
        ]

        handle = None
        for sel in selectors:
            try:
                handle = page.wait_for_selector(sel, timeout=3000)
                if handle:
                    break
            except Exception:
                pass

        with page.expect_download() as dlinfo:
            if handle:
                handle.click()
            else:
                page.evaluate("""
                    () => {
                      const els = [...document.querySelectorAll('a,button,input[type=submit],input[type=button]')];
                      const btn = els.find(el => /download|export|excel|csv/i.test((el.textContent||'')+(el.value||'')));
                      if (btn) btn.click();
                    }
                """)

        download = dlinfo.value
        suggested = download.suggested_filename
        out_path = dest_dir / (suggested or "AllCandidates.xlsx")
        download.save_as(str(out_path))
        browser.close()
        return out_path


def load_dataframe_from_file(path: Path) -> pd.DataFrame:
    """
    Robust loader for KY SOS export:
      - XLSX (zip) -> openpyxl
      - Legacy XLS (OLE/BIFF) -> xlrd==1.2.0 (direct, bypass pandas)
      - Excel-HTML disguised as .xls -> pandas.read_html
      - CSV fallback with encoding detection
    """
    from charset_normalizer import from_path

    def head_bytes(p: Path, n: int = 8192) -> bytes:
        with open(p, "rb") as f:
            return f.read(n)

    def looks_like_zip(b: bytes) -> bool:
        return b.startswith(b"PK\x03\x04")

    def looks_like_ole(b: bytes) -> bool:
        # OLE Compound File header for legacy .xls
        return b.startswith(b"\xD0\xCF\x11\xE0")

    def looks_like_html(b: bytes) -> bool:
        lb = b.lower()
        return (lb.startswith(b"<") and (b"<html" in lb or b"<table" in lb or b"<!doctype" in lb)) \
               or (b"content-type" in lb and b"text/html" in lb)

    def pick_table_with_county(dfs):
        if not dfs:
            return None
        for df in dfs:
            cols = [str(c).strip().lower() for c in df.columns]
            if "county" in cols:
                return df
        return max(dfs, key=lambda d: (len(d.columns), len(d)))

    b = head_bytes(path)
    print(f"[loader] file={path.name} bytes[0:8]={b[:8].hex()} size={path.stat().st_size}")

    # 1) HTML (many 'xls' downloads are HTML)
    if looks_like_html(b):
        print("[loader] Detected HTML; parsing via read_html")
        dfs = pd.read_html(str(path))  # needs lxml/html5lib
        df = pick_table_with_county(dfs) or dfs[0]
        df.columns = [str(c).strip() for c in df.columns]
        return df

    # 2) XLSX-like (zip) -> openpyxl
    if looks_like_zip(b) or path.suffix.lower() in (".xlsx", ".xlsm", ".xltx", ".xltm"):
        print("[loader] Detected XLSX zip or xlsx-like; using openpyxl")
        return pd.read_excel(path, engine="openpyxl")

    # 3) Legacy XLS (OLE/BIFF) -> xlrd DIRECT (bypass pandas)
    if looks_like_ole(b) or path.suffix.lower() == ".xls":
        print("[loader] Detected legacy XLS; using xlrd (direct)")
        try:
            import xlrd  # 1.2.0
            book = xlrd.open_workbook(str(path))
            sheet = book.sheet_by_index(0)

            # Extract rows
            rows = []
            for r in range(sheet.nrows):
                rows.append([sheet.cell_value(r, c) for c in range(sheet.ncols)])

            if not rows:
                raise RuntimeError("XLS appears empty.")

            # Use first non-empty row as header
            header_row_idx = 0
            while header_row_idx < len(rows) and all((str(v).strip() == "" for v in rows[header_row_idx])):
                header_row_idx += 1
            header = [str(h).strip() for h in rows[header_row_idx]]
            data = rows[header_row_idx + 1:]

            # If the header looks binary garbage, try the next row as header
            if len(header) == 1 and len("".join(header)) > 100:
                if header_row_idx + 1 < len(rows):
                    header = [str(h).strip() for h in rows[header_row_idx + 1]]
                    data = rows[header_row_idx + 2:]

            df = pd.DataFrame(data, columns=header)
            # Drop fully-empty columns that happen in BIFF exports
            df = df.dropna(axis=1, how="all")
            # Normalize column names
            df.columns = [str(c).strip() for c in df.columns]
            return df

        except Exception as e:
            print(f"[loader] xlrd direct failed: {type(e).__name__}: {e}")
            # fall through to other attempts

    # 4) Try openpyxl as last Excel attempt (rare mislabeled files)
    try:
        print("[loader] Trying openpyxl as fallback")
        return pd.read_excel(path, engine="openpyxl")
    except Exception as e:
        print(f"[loader] openpyxl fallback failed: {type(e).__name__}: {e}")

    # 5) CSV fallback with encoding detection and retries
    print("[loader] Trying CSV with encoding detection")
    try:
        result = from_path(str(path)).best()
        enc = (result.encoding if result else None) or "utf-8"
        print(f"[loader] Detected encoding={enc}")
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e1:
            print(f"[loader] csv read with {enc} failed: {type(e1).__name__}: {e1}")
            for enc2 in ("cp1252", "latin-1"):
                try:
                    print(f"[loader] Retrying csv with encoding={enc2}")
                    return pd.read_csv(path, encoding=enc2, engine="python", on_bad_lines="skip")
                except Exception as e2:
                    print(f"[loader] csv read with {enc2} failed: {type(e2).__name__}: {e2}")
    except Exception as e:
        print(f"[loader] encoding detection failed: {type(e).__name__}: {e}")

    raise RuntimeError(
        f"Could not parse downloaded file {path.name} as html/xlsx/xls/csv after multiple attempts."
    )

def split_by_county(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    # Look for any column that contains 'county' (case-insensitive)
    candidates = [c for c in df.columns if "county" in str(c).lower()]
    if not candidates:
        # Debug: show columns to logs
        print("[split] Columns:", list(df.columns))
        raise ValueError(f"Couldn't find a County column. Columns: {list(df.columns)}")

    col = candidates[0]
    df[col] = df[col].astype(str).str.strip()
    groups = {}
    for county, sub in df.groupby(col, dropna=True):
        name = str(county).strip()
        if not name:
            continue
        groups[name] = sub.reset_index(drop=True)
    return groups



def dataframe_to_bytes(df: pd.DataFrame) -> bytes:
    if OUTPUT_EXT.lower() == "xlsx":
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        return buf.getvalue()
    return df.to_csv(index=False).encode("utf-8")


def name_to_filename(county: str) -> str:
    # Keep readable/stable names (e.g., "Laurel.csv")
    base = re.sub(r"\s+", " ", county.strip())
    base = re.sub(r"[^A-Za-z0-9 \-']", "", base).strip()
    fname = f"{base}.{OUTPUT_EXT}"
    if REPO_PATH_PREFIX:
        return f"{REPO_PATH_PREFIX.rstrip('/')}/{fname}"
    return fname


def main():
    if not GH_TOKEN:
        print("ERROR: GH_TOKEN is required (repo contents write permissions).", file=sys.stderr)
        sys.exit(1)

    if REPO_PATH_PREFIX:
        ensure_folder(REPO_PATH_PREFIX)

    with tempfile.TemporaryDirectory() as td:
        tmpdir = Path(td)
        print("Downloading master spreadsheet…")
        master_path = playwright_download_xlsx(tmpdir)
        print(f"Downloaded: {master_path.name}")

        print("Reading and splitting by County…")
        df = load_dataframe_from_file(master_path)
        groups = split_by_county(df)
        if not groups:
            print("No county groups found—nothing to upload.")
            return

        for county, subdf in groups.items():
            target_path = name_to_filename(county)
            content = dataframe_to_bytes(subdf)
            sha = get_existing_sha(target_path)
            msg = f"Update {target_path} from latest KY SOS export"
            put_file(target_path, content, msg, sha)
            print(f"Upserted {target_path} ({len(subdf)} rows)")

    print("Done.")


if __name__ == "__main__":
    main()
