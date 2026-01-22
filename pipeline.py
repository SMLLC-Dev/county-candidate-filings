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

# GitHub target (read from workflow env)
GITHUB_OWNER = os.environ["GITHUB_OWNER"]
GITHUB_REPO = os.environ["GITHUB_REPO"]
GH_TOKEN = os.environ.get("GH_TOKEN")  # provided by GITHUB_TOKEN in workflow
# ------------------------------------------

# Canonical county list for validation
EXPECTED_COUNTIES = {
    "Adair","Allen","Anderson","Ballard","Barren","Bath","Bell","Boone","Bourbon","Boyd",
    "Boyle","Bracken","Breathitt","Breckinridge","Bullitt","Butler","Caldwell","Calloway",
    "Campbell","Carlisle","Carroll","Carter","Casey","Christian","Clark","Clay","Clinton",
    "Crittenden","Cumberland","Daviess","Edmonson","Elliott","Estill","Fayette","Fleming",
    "Floyd","Franklin","Fulton","Gallatin","Garrard","Grant","Graves","Grayson","Green",
    "Greenup","Hancock","Hardin","Harlan","Harrison","Hart","Henderson","Henry","Hickman",
    "Hopkins","Jackson","Jefferson","Jessamine","Johnson","Kenton","Knox","Larue","Laurel",
    "Lawrence","Lee","Leslie","Letcher","Lewis","Lincoln","Livingston","Logan","Lyon",
    "Madison","Magoffin","Marion","Marshall","Martin","Mason","McCracken","McCreary",
    "McLean","Meade","Menifee","Mercer","Metcalfe","Monroe","Montgomery","Morgan",
    "Muhlenberg","Nelson","Nicholas","Ohio","Oldham","Owen","Owsley","Pendleton","Perry",
    "Pike","Powell","Pulaski","Robertson","Rockcastle","Rowan","Russell","Scott","Shelby",
    "Simpson","Spencer","Taylor","Todd","Trigg","Trimble","Union","Warren","Washington",
    "Wayne","Webster","Whitley","Wolfe","Woodford"
}

# ---------- Helper functions ----------

def normalize_county_name(raw: str) -> str:
    """Normalize to title-case with Mc/Mac handling."""
    name = raw.strip().title()
    name = re.sub(r"\bMc(\w)", lambda m: "Mc" + m.group(1).upper(), name)
    name = re.sub(r"\bMac(\w)", lambda m: "Mac" + m.group(1).upper(), name)
    return name


def github_api(path: str, method: str = "GET", json_body: Optional[dict] = None):
    import requests
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{path}"
    headers = {
        "Authorization": f"Bearer {GH_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    return requests.request(method, url, headers=headers, json=json_body)


def get_existing_sha(path: str) -> Optional[str]:
    r = github_api(path, "GET")
    return r.json().get("sha") if r.status_code == 200 else None


def put_file(path: str, content_bytes: bytes, message: str, sha: Optional[str]):
    encoded = base64.b64encode(content_bytes).decode("utf-8")
    body = {"message": message, "content": encoded}
    if sha:
        body["sha"] = sha
    r = github_api(path, "PUT", json_body=body)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"GitHub PUT failed for {path}: {r.status_code} {r.text}")


def ensure_folder(prefix: str):
    if not prefix:
        return
    keep_path = prefix.rstrip("/") + "/.keep"
    sha = get_existing_sha(keep_path)
    if sha is None:
        put_file(keep_path, b"", f"Create {prefix} folder", None)


def playwright_download_xlsx(dest_dir: Path) -> Path:
    """
    Navigate to the SOS page, click the export, and persist the download robustly.
    Retries a few times to handle ASP.NET postbacks/races.
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    MAX_TRIES = 4
    DOWNLOAD_TIMEOUT_MS = 240_000  # 4 minutes
    NAV_TIMEOUT_MS = 60_000

    # Candidate selectors (broad → specific). Add known ASP.NET IDs if you find them.
    CANDIDATE_SELECTORS = [
        'text=/Download All Candidates/i',
        'text=/Export All/i',
        'text=/Download|Export|Excel|CSV/i',
        "a:has-text('Download')",
        "button:has-text('Download')",
        "a:has-text('Export')",
        "button:has-text('Export')",
        "input[type=submit]",
        "input[type=button]",
        # Add specific IDs if you can inspect the page:
        "#MainContent_btnExport",
        "input#MainContent_btnExport",
    ]

    def try_click(page):
        # Find the first visible candidate and click it
        for sel in CANDIDATE_SELECTORS:
            try:
                el = page.wait_for_selector(sel, timeout=3000, state="visible")
                if el:
                    el.click()
                    return True
            except Exception:
                continue

        # As a last resort, brute-force click anything that looks like a download/export
        page.evaluate(
            """
            () => {
                const els = [...document.querySelectorAll('a,button,input[type=submit],input[type=button]')];
                const btn = els.find(el =>
                    /download|export|excel|csv/i.test((el.textContent||'')+(el.value||''))
                );
                if (btn) btn.click();
            }
            """
        )
        return True

    with sync_playwright() as p:
        # Keep the browser open until AFTER we fully persist the download.
        browser = p.chromium.launch()
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # Be a little more like a user-agent
        page.set_default_navigation_timeout(NAV_TIMEOUT_MS)
        page.set_default_timeout(NAV_TIMEOUT_MS)

        for attempt in range(1, MAX_TRIES + 1):
            try:
                page.goto(ELECTION_URL, wait_until="domcontentloaded")
                # Give the page a beat to finish postback wiring
                page.wait_for_timeout(800)

                # fire the click and wait for a download event
                with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as dl_info:
                    try_click(page)

                download = dl_info.value

                # If server aborted the stream, surface reason
                fail_reason = download.failure()
                if fail_reason:
                    print(f"[download] attempt {attempt}: server reported failure: {fail_reason}")
                    # Try a fresh attempt (reload)
                    page.reload(wait_until="domcontentloaded")
                    continue

                suggested = download.suggested_filename or "AllCandidates.xls"
                out_path = dest_dir / suggested

                # Primary path: save_as (streams to our desired path)
                try:
                    download.save_as(str(out_path))
                    print(f"[download] saved via save_as → {out_path.name}")
                    # Ensure file is there and non-empty
                    if out_path.exists() and out_path.stat().st_size > 0:
                        context.close()
                        browser.close()
                        return out_path
                except Exception as e:
                    print(
                        f"[download] save_as failed on attempt {attempt}: "
                        f"{type(e).__name__}: {e}"
                    )

                # Fallback: if Playwright cached to a temp file, copy that path
                try:
                    temp_path = download.path()  # may be None on some platforms
                except Exception as e:
                    temp_path = None
                    print(f"[download] download.path() failed: {type(e).__name__}: {e}")

                if temp_path and os.path.exists(temp_path):
                    # Copy to our desired location
                    import shutil

                    shutil.copy2(temp_path, out_path)
                    print(f"[download] copied from temp cache → {out_path.name}")
                    if out_path.exists() and out_path.stat().st_size > 0:
                        context.close()
                        browser.close()
                        return out_path

                # If we get here, the artifact didn’t persist—retry cleanly
                page.reload(wait_until="domcontentloaded")

            except PWTimeout as te:
                print(f"[download] timeout on attempt {attempt}: {type(te).__name__}: {te}")
                # Reload and retry
                try:
                    page.reload(wait_until="domcontentloaded")
                except Exception:
                    # If reload itself failed, re-open page
                    page.close()
                    page = context.new_page()
            except Exception as e:
                print(f"[download] generic error on attempt {attempt}: {type(e).__name__}: {e}")
                # Fresh page for next loop
                try:
                    page.close()
                except Exception:
                    pass
                page = context.new_page()

        # All attempts failed
        try:
            context.close()
        finally:
            browser.close()
        raise RuntimeError("Failed to capture export after multiple attempts. See [download] logs above.")


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
        return (
            (lb.startswith(b"<") and (b"<html" in lb or b"<table" in lb or b"<!doctype" in lb))
            or (b"content-type" in lb and b"text/html" in lb)
        )

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
            while header_row_idx < len(rows) and all(
                (str(v).strip() == "" for v in rows[header_row_idx])
            ):
                header_row_idx += 1

            header = [str(h).strip() for h in rows[header_row_idx]]
            data = rows[header_row_idx + 1 :]

            # If the header looks binary garbage, try the next row as header
            if len(header) == 1 and len("".join(header)) > 100:
                if header_row_idx + 1 < len(rows):
                    header = [str(h).strip() for h in rows[header_row_idx + 1]]
                    data = rows[header_row_idx + 2 :]

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
                    return pd.read_csv(
                        path,
                        encoding=enc2,
                        engine="python",
                        on_bad_lines="skip",
                    )
                except Exception as e2:
                    print(
                        f"[loader] csv read with {enc2} failed: "
                        f"{type(e2).__name__}: {e2}"
                    )
    except Exception as e:
        print(f"[loader] encoding detection failed: {type(e).__name__}: {e}")

    raise RuntimeError(
        f"Could not parse downloaded file {path.name} as html/xlsx/xls/csv after multiple attempts."
    )


def split_by_county(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    # Find county column (same as before)
    county_candidates = [c for c in df.columns if "county" in str(c).lower()]
    if not county_candidates:
        print("[split] Columns:", list(df.columns))
        raise ValueError(
            f"Couldn't find a County column. Columns: {list(df.columns)}"
        )
    county_col = county_candidates[0]

    # Normalize county names
    df = df.copy()
    df[county_col] = df[county_col].astype(str).map(normalize_county_name)

    # Find a "Date Filed" column (exact preferred; fuzzy allowed)
    date_col = None
    exact = [c for c in df.columns if str(c).strip().lower() == "date filed"]
    if exact:
        date_col = exact[0]
    else:
        fuzzy = [
            c
            for c in df.columns
            if "date" in str(c).lower() and "file" in str(c).lower()
        ]
        if fuzzy:
            date_col = fuzzy[0]

    # Compute a sortable datetime if we have the column
    if date_col:
    s = df[date_col]

    # If it's numeric (Excel serial dates), convert from Excel epoch.
    if pd.api.types.is_numeric_dtype(s):
        df["__sort_date"] = pd.to_datetime(s, unit="D", origin="1899-12-30", errors="coerce")
    else:
        # Try normal parse first
        df["__sort_date"] = pd.to_datetime(s, errors="coerce")

        # If many failed, try pandas "mixed" parsing (pandas 2.x)
        if df["__sort_date"].isna().mean() > 0.50:
            try:
                df["__sort_date"] = pd.to_datetime(s, errors="coerce", format="mixed")
            except TypeError:
                # Older pandas doesn't support format="mixed"
                pass
else:
    df["__sort_date"] = pd.NaT

    # Build groups, sorting each group by Date Filed (desc)
    groups: Dict[str, pd.DataFrame] = {}
    for county, sub in df.groupby(county_col, dropna=True):
        county_name = normalize_county_name(str(county))
        if not county_name:
            continue
        sub_sorted = sub.sort_values("__sort_date", ascending=True).drop(
            columns="__sort_date"
        )
        groups[county_name] = sub_sorted.reset_index(drop=True)

    return groups


def dataframe_to_bytes(df: pd.DataFrame) -> bytes:
    if OUTPUT_EXT.lower() == "xlsx":
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        return buf.getvalue()
    return df.to_csv(index=False).encode("utf-8")


def name_to_filename(county: str) -> str:
    safe = normalize_county_name(county)
    fname = f"{safe}.{OUTPUT_EXT}"
    if REPO_PATH_PREFIX:
        return f"{REPO_PATH_PREFIX.rstrip('/')}/{fname}"
    return fname


def main():
    if not GH_TOKEN:
        print(
            "ERROR: GH_TOKEN is required (repo contents write permissions).",
            file=sys.stderr,
        )
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

        total_rows = 0
        for county, subdf in groups.items():
            target_path = name_to_filename(county)
            content = dataframe_to_bytes(subdf)
            sha = get_existing_sha(target_path)
            msg = f"Update {target_path} from latest KY SOS export"
            put_file(target_path, content, msg, sha)
            print(f"Upserted {target_path} ({len(subdf)} rows)")
            total_rows += len(subdf)

        # ---- Summary & Validation ----
        print(f"\nTotal rows across all counties: {total_rows}")
        found = set(groups.keys())
        missing = EXPECTED_COUNTIES - found
        unexpected = found - EXPECTED_COUNTIES

        print("\nValidation summary:")
        if missing:
            print("❌ Missing counties:", sorted(missing))
        if unexpected:
            print("⚠️ Unexpected county names:", sorted(unexpected))
        if not missing and not unexpected:
            print("✅ All expected counties accounted for.")

        print("\n✅ Pipeline complete.")


if __name__ == "__main__":
    main()
