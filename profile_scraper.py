import asyncio
import csv
import logging
import re
import sqlite3
import sys
import time
from typing import List, Dict, Optional

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from tqdm import tqdm

# ---------------- CONFIG ----------------

DB_PATH = "missed_gmc_numbers.csv"
OUT_CSV = "missed_gmc.csv"
OUT_SQLITE = "missed_gmc.sqlite"
HEADLESS = True
RETRY_LIMIT = 5
PAGE_TIMEOUT_MS = 60000
LOG_FILE = "gmc_scraper_log.log"
MAX_CONCURRENT_PAGES = 6

# ----------------------------------------

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ---------- TEXT HELPERS ----------

NOISE_PREFIXES = (
    "The registrant's unique identifier",
    "The organisation responsible for a registrant's revalidation",
    "The senior doctor who oversees a registrant's revalidation",
    "The qualification accepted for registration",
    "The date we first granted the doctor full registration",
    "The type of profession in which the registrant works",
    "A history of the registrant's registration",
    "Registration and licensing history",
    "The date their registration started",
    "The date their registration ended",
    "The registrant's registration status in this period",
)

LABELS = {
    "Profession",
    "Registered qualification",
    "Full registration date",
    "Designated body",
    "Responsible officer",
    "Gender",
    "GP Register",
    "Specialist Register",
}

def normalize_text(s: str) -> str:
    return re.sub(r"[ \t]+", " ", s)

def next_meaningful_line(
    lines: List[str], start_idx: int, stop_labels: Optional[List[str]] = None
) -> Optional[str]:
    stop_set = set(stop_labels or [])
    for j in range(start_idx + 1, len(lines)):
        line = lines[j].strip()
        if not line:
            continue
        if any(line.startswith(p) for p in NOISE_PREFIXES):
            continue
        if line in stop_set or line in LABELS:
            continue
        return line
    return None

def get_label_value(
    full_text: str, label: str, stop_labels: Optional[List[str]] = None
) -> Optional[str]:
    lines = [normalize_text(l) for l in full_text.split("\n")]
    for i, line in enumerate(lines):
        if line.strip() == label:
            val = next_meaningful_line(lines, i, stop_labels)
            if val:
                return val.strip()
    return None

def extract_with_regex(text: str, pattern: str) -> Optional[str]:
    m = re.search(pattern, text, flags=re.S | re.I)
    return m.group(1).strip() if m else None

# ---------------- SPECIALTIES PARSER (BRACKETED FORMAT) ----------------

def parse_specialties(soup: BeautifulSoup) -> List[str]:
    """Extract FULL 'Specialty from DATE' in brackets"""
    specs = []
    
    # Find the exact speciality-list container
    speciality_list = soup.find('ul', class_='speciality-list')
    if speciality_list:
        for li in speciality_list.find_all('li'):
            span = li.find('span')
            if span:
                full_text = span.get_text(strip=True)
                if 'from' in full_text.lower():
                    specs.append(f"[{full_text}]")

    return specs

# ---------------- PARSER (Specialist pages) ----------------

def parse_gp_profile(html: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    full_text = soup.get_text("\n", strip=True)
    data: Dict[str, any] = {}

    # Name: heading immediately above "GMC reference number:"
    name = None
    gmc_label = soup.find(string=lambda t: isinstance(t, str) and "GMC reference number" in t)
    if gmc_label:
        name_tag = gmc_label.find_previous(["h1", "h2"])
        if name_tag:
            candidate = name_tag.get_text(strip=True)
            if candidate and candidate.lower() != "cookies":
                name = candidate
    if not name:
        name = extract_with_regex(full_text, r"\n##\s+([^\n]+)\n\s*Doctor")
    data["Name"] = name

    # GMC number: digits only
    gmc = extract_with_regex(
        full_text,
        r"GMC reference number:\s*(?:The registrant's unique identifier\.\s*)?(\d{6,8})",
    )
    data["GMC_Number"] = gmc

    # Licence status
    data["Licence_Status"] = "Registered with a licence to practise"

    # GP Register (on + since)
    on_gp = "This doctor is on the GP Register" in full_text
    since = extract_with_regex(
        full_text, r"GP Register.*?From\s+([0-9]{2}\s\w{3}\s[0-9]{4})"
    )
    data["GP_Register"] = {
        "On_Register": bool(on_gp),
        "Since": since if on_gp else None,
    }

    # Specialist Register with bracketed specialties
    specialties = parse_specialties(soup)
    on_spec = "This doctor is on the Specialist Register" in full_text or bool(specialties)
    data["Specialist_Register"] = {
        "On_Register": bool(on_spec),
        "Specialties": specialties,
    }

    # Profession
    profession = get_label_value(full_text, "Profession")
    if not profession:
        profession = extract_with_regex(full_text, r"Profession\s+([A-Za-z ]+)")
    data["Profession"] = profession

    # Registered qualification
    reg_qual = get_label_value(
        full_text,
        "Registered qualification",
        stop_labels=["The qualification accepted for registration"],
    )
    if not reg_qual:
        reg_qual = extract_with_regex(
            full_text, r"Registered qualification\s+(.+?)(?:\n|$)"
        )
    data["Registered_Qualification"] = reg_qual

    # Full registration date
    full_reg = get_label_value(full_text, "Full registration date")
    if not full_reg:
        full_reg = extract_with_regex(
            full_text, r"Full registration date\s+([0-9]{2}\s\w{3}\s[0-9]{4})"
        )
    data["Full_Registration_Date"] = full_reg

    # Gender
    gender = get_label_value(full_text, "Gender")
    if not gender:
        gender = extract_with_regex(full_text, r"Gender\s+(\w+)")
    data["Gender"] = gender

    # Designated body
    designated_body = get_label_value(full_text, "Designated body")
    if not designated_body:
        designated_body = extract_with_regex(
            full_text, r"Designated body\s+(.+?)(?:\n|$)"
        )
    data["Designated_Body"] = designated_body

    # Responsible officer
    responsible_officer = get_label_value(full_text, "Responsible officer")
    if not responsible_officer:
        responsible_officer = extract_with_regex(
            full_text, r"Responsible officer\s+(.+?)(?:\n|$)"
        )
    data["Responsible_Officer"] = responsible_officer

    # Annual retention fee due date
    annual_fee = extract_with_regex(
        full_text,
        r"Annual retention fee due date:\s*([0-9]{2}\s\w{3}\s[0-9]{4})",
    )
    data["Annual_Fee_Due"] = annual_fee

    # Registration history (first table with three TDs)
    history = []
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) == 3:
                history.append({"From": cols[0], "To": cols[1], "Status": cols[2]})
        if history:
            break
    data["Registration_History"] = history

    return data

# ---------------- FLATTENERS ----------------

def flatten_history(history: List[Dict[str, str]]) -> str:
    if not history:
        return ""
    entries = []
    for h in history:
        from_v = (h.get("From") or "").replace("\n", " ").strip()
        to_v = (h.get("To") or "").replace("\n", " ").strip()
        status_v = (h.get("Status") or "").replace("\n", " ").strip()
        entries.append(f"From: {from_v} | To: {to_v} | Status: {status_v}")
    return ", ".join(entries)

def flatten_specialties(spec_list: Optional[List[str]]) -> str:
    if not spec_list:
        return ""
    return " ".join(spec_list)  # [A] [B] [C] format

def to_single_row(rec: Dict[str, any]) -> Dict[str, any]:
    gp = rec.get("GP_Register") or {}
    spec = rec.get("Specialist_Register") or {}
    history_flat = flatten_history(rec.get("Registration_History") or [])
    specialties_flat = flatten_specialties(spec.get("Specialties"))

    return {
        "GMC_Number": rec.get("GMC_Number"),
        "Name": rec.get("Name"),
        "Licence_Status": rec.get("Licence_Status"),
        "Profession": rec.get("Profession"),
        "Gender": rec.get("Gender"),
        "Registered_Qualification": rec.get("Registered_Qualification"),
        "Full_Registration_Date": rec.get("Full_Registration_Date"),
        "Annual_Fee_Due": rec.get("Annual_Fee_Due"),
        "Designated_Body": rec.get("Designated_Body"),
        "Responsible_Officer": rec.get("Responsible_Officer"),
        "On_GP_Register": 1 if gp.get("On_Register") else 0,
        "GP_Since": gp.get("Since") or "",
        "On_Specialist_Register": 1 if spec.get("On_Register") else 0,
        "Specialties_Flat": specialties_flat,
        "Registration_History_Flat": history_flat,
        "Profile_URL": rec.get("Profile_URL"),
    }

# ---------------- DB LOADER ----------------
import os
import csv
import sqlite3
from typing import List

def load_target_urls_from_db(db_path: str) -> List[str]:
    """
    Returns a list of URLs. If db_path points to a CSV file, it will read the 'Profile_URL'
    column from that CSV. Otherwise, it will query the SQLite DB as before.

    CSV expectation:
      - Header includes 'Profile_URL'
      - One URL per row, e.g.:
            Profile_URL
            https://www.gmc-uk.org/registrants/4253905
            /registrants/3658437
    """

    def _normalize(url: str) -> str:
        """Normalize relative GMC URLs to absolute; strip whitespace."""
        if not url:
            return ""
        url = url.strip()
        if url.startswith("/"):
            return "https://www.gmc-uk.org" + url
        return url

    def _unique_preserve_order(items: List[str]) -> List[str]:
        """Deduplicate while preserving order."""
        seen = set()
        out = []
        for x in items:
            x = x.strip()
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return out

    # --- Path exists check ---
    if not os.path.isfile(db_path):
        # Graceful empty result on missing file (keeps function contract unchanged)
        return []

    # --- If CSV: read 'Profile_URL' column ---
    _, ext = os.path.splitext(db_path.lower())
    if ext == ".csv":
        urls: List[str] = []
        with open(db_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            # Ensure column exists; if not, return empty to keep behavior predictable
            if "Profile_URL" not in (reader.fieldnames or []):
                return []
            for row in reader:
                url = row.get("Profile_URL")
                if not url:
                    continue
                url = _normalize(url)
                if url:
                    urls.append(url)
        return _unique_preserve_order(urls)

    # --- Fallback: original SQLite DB logic ---
    conn = sqlite3.connect(db_path)
    try:
        query = """
            SELECT DISTINCT Profile_URL
            FROM gmc_data
            WHERE Profile_URL IS NOT NULL
              AND Registration_Status = 'Registered with a licence to practise'
        """
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        urls: List[str] = []
        for r in rows:
            url = r[0]
            if not url:
                continue
            url = _normalize(url)
            if url:
                urls.append(url)
        return _unique_preserve_order(urls)
    finally:
        conn.close()

# ---------------- SAFETY SCRAPER ----------------

async def scrape_profile_page(context, url: str) -> Optional[str]:
    for attempt in range(RETRY_LIMIT):
        page = await context.new_page()
        try:
            await page.goto(url, timeout=PAGE_TIMEOUT_MS)
            await page.wait_for_timeout(2000)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)
            html = await page.content()
            await page.close()

            if any(keyword in html.lower() for keyword in ["verify", "captcha", "blocked", "unusual activity"]):
                print(f"Verification detected on {url}. Retrying ({attempt+1}/{RETRY_LIMIT})...")
                await asyncio.sleep(5 * (attempt + 1))
                continue

            parsed = parse_gp_profile(html)
            if parsed.get("GMC_Number"):
                return html
            else:
                print(f"No data found on {url}. Retrying ({attempt+1}/{RETRY_LIMIT})...")
                await asyncio.sleep(3 * (attempt + 1))
                continue

        except Exception as e:
            print(f"Error loading {url}: {e}. Retrying ({attempt+1}/{RETRY_LIMIT})...")
            try:
                if not page.is_closed():
                    await page.close()
            except Exception:
                pass
            await asyncio.sleep(3 * (attempt + 1))

    print(f"Failed to scrape {url} after {RETRY_LIMIT} attempts.")
    logging.error(f"Failed to scrape {url} after {RETRY_LIMIT} attempts.")
    return None

# ---------------- CONCURRENT SCRAPER ----------------

async def scrape_profiles(urls: List[str]) -> List[Dict]:
    total = len(urls)
    if total == 0:
        return []

    results: List[Dict] = []
    results_lock = asyncio.Lock()
    sem = asyncio.Semaphore(MAX_CONCURRENT_PAGES)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-GB",
            timezone_id="Europe/London",
        )

        await context.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ["image", "font", "media"]
            else route.continue_(),
        )

        progress = tqdm(total=total, desc="Scraping Specialist Profiles", unit="profile")
        start_time = time.time()

        async def worker(url: str):
            async with sem:
                html = await scrape_profile_page(context, url)
                if html:
                    parsed = parse_gp_profile(html)
                    parsed["Profile_URL"] = url
                    async with results_lock:
                        results.append(parsed)
                progress.update(1)
                elapsed = time.time() - start_time
                avg_time = elapsed / progress.n if progress.n > 0 else 0
                remaining = total - progress.n
                eta = remaining * avg_time / 60
                progress.set_postfix({"ETA": f"{eta:.1f}min"})

        tasks = [asyncio.create_task(worker(u)) for u in urls]
        await asyncio.gather(*tasks)

        await browser.close()
        progress.close()

    return results

# ---------------- WRITERS ----------------

def write_csv(rows: List[Dict[str, any]], out_csv: str) -> None:
    if not rows:
        print("No rows to write.")
        return
    fieldnames = list(rows[0].keys())
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"CSV written: {out_csv}")

def write_sqlite(rows: List[Dict[str, any]], out_db: str) -> None:
    conn = sqlite3.connect(out_db)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS doctors_single (
            GMC_Number TEXT PRIMARY KEY,
            Name TEXT, Licence_Status TEXT, Profession TEXT, Gender TEXT,
            Registered_Qualification TEXT, Full_Registration_Date TEXT, Annual_Fee_Due TEXT,
            Designated_Body TEXT, Responsible_Officer TEXT, On_GP_Register INTEGER, GP_Since TEXT,
            On_Specialist_Register INTEGER, Specialties_Flat TEXT, Registration_History_Flat TEXT,
            Profile_URL TEXT
        )
    """)
    for r in rows:
        cur.execute("""
            INSERT INTO doctors_single VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(GMC_Number) DO UPDATE SET
                Name=excluded.Name, Licence_Status=excluded.Licence_Status,
                Profession=excluded.Profession, Gender=excluded.Gender,
                Registered_Qualification=excluded.Registered_Qualification,
                Full_Registration_Date=excluded.Full_Registration_Date, Annual_Fee_Due=excluded.Annual_Fee_Due,
                Designated_Body=excluded.Designated_Body, Responsible_Officer=excluded.Responsible_Officer,
                On_GP_Register=excluded.On_GP_Register, GP_Since=excluded.GP_Since,
                On_Specialist_Register=excluded.On_Specialist_Register, Specialties_Flat=excluded.Specialties_Flat,
                Registration_History_Flat=excluded.Registration_History_Flat, Profile_URL=excluded.Profile_URL
        """, (
            r.get("GMC_Number"), r.get("Name"), r.get("Licence_Status"), r.get("Profession"),
            r.get("Gender"), r.get("Registered_Qualification"), r.get("Full_Registration_Date"),
            r.get("Annual_Fee_Due"), r.get("Designated_Body"), r.get("Responsible_Officer"),
            r.get("On_GP_Register"), r.get("GP_Since"), r.get("On_Specialist_Register"),
            r.get("Specialties_Flat"), r.get("Registration_History_Flat"), r.get("Profile_URL")
        ))
    conn.commit()
    conn.close()
    print(f"SQLite written: {out_db}")

# ---------------- MAIN ----------------

async def main():
    target_urls = load_target_urls_from_db(DB_PATH)
    print(f"Loaded {len(target_urls)} URLs from {DB_PATH}")
    if not target_urls:
        print("No URLs found in database; exiting.")
        return

    parsed = await scrape_profiles(target_urls)
    single_rows = [to_single_row(r) for r in parsed]
    write_csv(single_rows, OUT_CSV)
    write_sqlite(single_rows, OUT_SQLITE)
    print(f"Scraped {len(single_rows)} specialist profiles. Saved to {OUT_CSV} and {OUT_SQLITE}")

if __name__ == "__main__":
    asyncio.run(main())