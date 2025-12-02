import asyncio
import logging
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import sqlite3
import string
import re
import os
from tqdm import tqdm
import time

# ---------------- CONFIG ----------------
BASE_URL = "https://www.gmc-uk.org/registrants/?page={page}&pagesize=50&isSpecialist=true&givenNameText={letter}"
DB_FILE = "gmc_results.db"
CONCURRENCY = 6
RETRY_LIMIT = 3
LOG_FILE = "gmc_scraping.log"
# ----------------------------------------

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gmc_data (
            Name TEXT,
            GMC_Number TEXT,
            Registration_Status TEXT,
            Profile_URL TEXT
        )
    """)
    conn.commit()
    conn.close()

def save_to_db(data):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO gmc_data VALUES (?, ?, ?, ?)", data)
    conn.commit()
    conn.close()

def get_completed_letters():
    if not os.path.exists(DB_FILE):
        return set()
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT SUBSTR(Name, 1, 1) FROM gmc_data")
    letters = {row[0].upper() for row in cursor.fetchall() if row[0]}
    conn.close()
    return letters

async def scrape_page(context, url):
    for attempt in range(RETRY_LIMIT):
        page = await context.new_page()
        try:
            await page.goto(url, timeout=60000)
            await page.wait_for_timeout(2000)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)
            html = await page.content()
            await page.close()

            # Check for verification or missing data
            if any(keyword in html.lower() for keyword in ["verify", "captcha", "blocked"]):
                print(f"⚠ Human verification detected on {url}. Retrying ({attempt+1}/{RETRY_LIMIT})...")
                await asyncio.sleep(5)
                continue

            if parse_html(html):
                return html
            else:
                print(f"No data found on {url}. Retrying ({attempt+1}/{RETRY_LIMIT})...")
                await asyncio.sleep(3)
        except Exception as e:
            print(f"Error loading {url}: {e}. Retrying...")
            await asyncio.sleep(3)
        finally:
            if not page.is_closed():
                await page.close()
    print(f"Failed to scrape {url} after {RETRY_LIMIT} attempts.")
    return None

def parse_html(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr")
    data = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 3:
            name_tag = cols[0].find("a")
            gmc_tag = cols[0].find("span", class_="faded")
            status_text = cols[2].get_text(strip=True)
            if name_tag and gmc_tag:
                name = name_tag.get_text(strip=True)
                gmc_number = gmc_tag.get_text(strip=True)
                registration_status = status_text.split("Profession")[0].strip()
                profile_url = name_tag["href"]
                data.append((name, gmc_number, registration_status, profile_url))
    return data

def get_total_pages(html):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()
    match = re.search(r"Showing\s+\d+-\d+\s+of\s+(\d+)", text)
    if match:
        total_records = int(match.group(1))
        pages = (total_records // 50) + (1 if total_records % 50 else 0)
        return pages
    return 1

async def scrape_letter(letter, context, progress, start_time):
    first_url = BASE_URL.format(page=1, letter=letter)
    html = await scrape_page(context, first_url)
    if not html:
        return []
    total_pages = get_total_pages(html)

    # Update progress bar total dynamically
    progress.total += total_pages
    progress.refresh()

    buffer = parse_html(html)
    all_data = buffer.copy()

    # Scrape remaining pages in batches
    for start in range(2, total_pages + 1, CONCURRENCY):
        tasks = []
        for page_num in range(start, min(start + CONCURRENCY, total_pages + 1)):
            url = BASE_URL.format(page=page_num, letter=letter)
            tasks.append(scrape_page(context, url))
        results = await asyncio.gather(*tasks)
        for html in results:
            if html:
                buffer.extend(parse_html(html))

        # Save after every batch
        if buffer:
            save_to_db(buffer)
            all_data.extend(buffer)
            buffer.clear()

        # Update progress and ETA dynamically
        pages_scraped = progress.n + min(CONCURRENCY, total_pages - start + 1)
        elapsed = time.time() - start_time
        avg_time_per_page = elapsed / pages_scraped if pages_scraped > 0 else 0
        remaining_pages = progress.total - pages_scraped
        eta = remaining_pages * avg_time_per_page
        progress.set_postfix({"ETA": f"{eta/60:.2f} min"})
        progress.update(min(CONCURRENCY, total_pages - start + 1))

        await asyncio.sleep(1)

    if buffer:
        save_to_db(buffer)
        all_data.extend(buffer)

    return all_data

async def main():
    init_db()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="Europe/London"
        )
        await context.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "font"] else route.continue_())

        completed_letters = get_completed_letters()
        letters = [l for l in string.ascii_uppercase if l not in completed_letters]

        # Start progress bar with dynamic total
        progress = tqdm(total=0, desc="Scraping Progress", unit="pages")
        start_time = time.time()

        for letter in letters:
            print(f"\nStarting letter: {letter}")
            data = await scrape_letter(letter, context, progress, start_time)
            print(f"Finished letter {letter} with {len(data)} records.")

        await browser.close()
        progress.close()

if __name__ == "__main__":
    asyncio.run(main())