
import asyncio
import json
import logging
import re
from typing import List, Dict, Optional

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ---------------- CONFIG ----------------
TARGET_URLS = [
    # <<< Replace with your GP profile URLs for manual checks >>>
    "https://www.gmc-uk.org/registrants/5192258",
    "https://www.gmc-uk.org/registrants/7182702",
    "https://www.gmc-uk.org/registrants/7874517",
]
OUT_JSON = "gp_profiles.json"
HEADLESS = True
RETRY_LIMIT = 5
PAGE_TIMEOUT_MS = 60000
LOG_FILE = "gp_scraper.log"
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
    # Keep newlines for line-based parsing; collapse multiple spaces
    return re.sub(r"[ \t]+", " ", s)

def next_meaningful_line(lines: List[str], start_idx: int, stop_labels: Optional[List[str]] = None) -> Optional[str]:
    """Return next non-empty, non-label, non-helper line after a label index."""
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

def get_label_value(full_text: str, label: str, stop_labels: Optional[List[str]] = None) -> Optional[str]:
    """Line-based label parser with helper-text skipping."""
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

# ---------------- PARSER (GP pages) ----------------
def parse_gp_profile(html: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    full_text = soup.get_text("\n", strip=True)

    data: Dict[str, any] = {}

    # Name: anchor to the heading immediately above "GMC reference number:"
    # This avoids picking "Cookies" or "Registrant history".
    name = None
    gmc_label = soup.find(string=lambda t: isinstance(t, str) and "GMC reference number" in t)
    if gmc_label:
        # Find the closest heading above the GMC label
        name_tag = gmc_label.find_previous(["h1", "h2"])
        if name_tag:
            candidate = name_tag.get_text(strip=True)
            if candidate and candidate.lower() != "cookies":
                name = candidate
    if not name:
        # Fallback: heading preceding "Doctor" in raw text
        name = extract_with_regex(full_text, r"\n##\s+([^\n]+)\n\s*Doctor")
    data["Name"] = name

    # GMC number: digits only, skipping helper text
    gmc = extract_with_regex(full_text, r"GMC reference number:\s*(?:The registrant's unique identifier\.\s*)?(\d{6,8})")
    data["GMC_Number"] = gmc

    # Licence status
    data["Licence_Status"] = "Registered with a licence to practise"

    # GP Register (on + since)
    # We search in the text near the "GP Register" block for "From <date>"
    on_gp = "This doctor is on the GP Register" in full_text
    since = extract_with_regex(full_text, r"GP Register.*?From\s+([0-9]{2}\s\w{3}\s[0-9]{4})")
    data["GP_Register"] = {"On_Register": bool(on_gp), "Since": since if on_gp else None}

    # Specialist Register (GP pages usually false)
    data["Specialist_Register"] = {"On_Register": "This doctor is on the Specialist Register" in full_text}

    # Profession
    profession = get_label_value(full_text, "Profession")
    if not profession:
        profession = extract_with_regex(full_text, r"Profession\s+([A-Za-z ]+)")
    data["Profession"] = profession

    # Registered qualification
    reg_qual = get_label_value(full_text, "Registered qualification", stop_labels=["The qualification accepted for registration"])
    if not reg_qual:
        reg_qual = extract_with_regex(full_text, r"Registered qualification\s+(.+?)(?:\n|$)")
    data["Registered_Qualification"] = reg_qual

    # Full registration date
    full_reg = get_label_value(full_text, "Full registration date")
    if not full_reg:
        full_reg = extract_with_regex(full_text, r"Full registration date\s+([0-9]{2}\s\w{3}\s[0-9]{4})")
    data["Full_Registration_Date"] = full_reg

    # Gender
    gender = get_label_value(full_text, "Gender")
    if not gender:
        gender = extract_with_regex(full_text, r"Gender\s+(\w+)")
    data["Gender"] = gender

    # Designated body
    designated_body = get_label_value(full_text, "Designated body")
    if not designated_body:
        designated_body = extract_with_regex(full_text, r"Designated body\s+(.+?)(?:\n|$)")
    data["Designated_Body"] = designated_body

    # Responsible officer
    responsible_officer = get_label_value(full_text, "Responsible officer")
    if not responsible_officer:
        responsible_officer = extract_with_regex(full_text, r"Responsible officer\s+(.+?)(?:\n|$)")
    data["Responsible_Officer"] = responsible_officer

    # Annual retention fee due date
    annual_fee = extract_with_regex(full_text, r"Annual retention fee due date:\s*([0-9]{2}\s\w{3}\s[0-9]{4})")
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

# ---------------- SCRAPER ----------------
async def fetch_page_html(context, url: str) -> Optional[str]:
    for attempt in range(1, RETRY_LIMIT + 1):
        page = await context.new_page()
        try:
            await page.goto(url, timeout=PAGE_TIMEOUT_MS)
            # Gentle settle + small scroll for any lazy content
            await page.wait_for_timeout(1200)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(600)

            html = await page.content()

            # Check for human verification hints
            if any(k in html.lower() for k in ("captcha", "verify", "blocked", "unusual activity")):
                logging.warning(f"Verification hinted on {url}. Attempt {attempt}/{RETRY_LIMIT}")
                await page.close()
                await asyncio.sleep(3 * attempt)
                continue

            await page.close()
            return html

        except Exception as e:
            logging.error(f"Error fetching {url} (attempt {attempt}): {e}")
            try:
                if not page.is_closed():
                    await page.close()
            except Exception:
                pass
            await asyncio.sleep(3 * attempt)
    logging.error(f"Failed to fetch {url} after {RETRY_LIMIT} attempts")
    return None

async def scrape_profiles(urls: List[str]) -> List[Dict]:
    results = []
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

        # Be polite: block heavy resources
        await context.route("**/*", lambda route: (
            route.abort() if route.request.resource_type in {"image", "font", "media"} else route.continue_()
        ))

        for url in urls:
            print(f"Scraping: {url}")
            html = await fetch_page_html(context, url)
            if html:
                parsed = parse_gp_profile(html)
                parsed["Profile_URL"] = url
                results.append(parsed)
            else:
                logging.error(f"No HTML for {url}")

        await browser.close()
    return results

# ---------------- MAIN ----------------
async def main():
    data = await scrape_profiles(TARGET_URLS)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    print(f"✅ Scraped {len(data)} GP profiles. Saved to {OUT_JSON}")

if __name__ == "__main__":
    asyncio.run(main())