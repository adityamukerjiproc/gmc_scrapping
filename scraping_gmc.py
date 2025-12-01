
from playwright.sync_api import sync_playwright
import csv
import os
from bs4 import BeautifulSoup
import re
from tqdm import tqdm
import time

# CSV file name
csv_file = 'bulk_doctor_info.csv'

# Define headers for CSV
headers = [
    'Name', 'GMC Reference Number', 'Registration Status', 'Profession',
    'Registered Qualification', 'Full Registration Date', 'Gender',
    'GP Register', 'Specialist Register', 'Revalidation',
    'history1_from', 'history1_to', 'history1_status',
    'history2_from', 'history2_to', 'history2_status',
    'history3_from', 'history3_to', 'history3_status',
    'history4_from', 'history4_to', 'history4_status'
]

# Create CSV with headers if not exists
if not os.path.exists(csv_file):
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

def extract_doctor_info(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extract Name
    name_tag = soup.select_one('h1#registrantNameId')
    name = name_tag.get_text(strip=True) if name_tag else None

    # Extract GMC Reference Number
    gmc_tag = soup.select_one('span#gmcNumberId')
    gmc_number = gmc_tag.get_text(strip=True) if gmc_tag else None

    # If essential fields missing, return None
    if not name or not gmc_number:
        return None

    # Extract Registration Status
    registration_status_tag = soup.find(string=re.compile(r'Registered|Not Registered'))
    registration_status = registration_status_tag.strip() if registration_status_tag else 'Unknown'

    # Profession
    profession_tag = soup.select_one('.c-rg-details__practitioner-type .u-global-heading-s')
    profession = profession_tag.get_text(strip=True) if profession_tag else 'Unknown'

    # Registered Qualification
    qualification = 'Unknown'
    qualification_tag = soup.find(string=re.compile(r'Registered qualification'))
    if qualification_tag:
        next_tag = qualification_tag.find_next('div', class_='c-rg-details__card-field')
        if next_tag:
            qualification = next_tag.get_text(strip=True)

    # Full Registration Date
    full_registration_date = 'Unknown'
    full_reg_label = soup.find(string=re.compile(r'Full registration date'))
    if full_reg_label:
        next_tag = full_reg_label.find_next('div', class_='c-rg-details__card-field')
        if next_tag:
            full_registration_date = next_tag.get_text(strip=True)

    # Gender
    gender = 'Unknown'
    gender_tag = soup.find(string=re.compile(r'Gender'))
    if gender_tag:
        next_tag = gender_tag.find_next('div', class_='c-rg-details__card-field')
        if next_tag:
            gender = next_tag.get_text(strip=True)

    # Extract Registration History from Table
    history_rows = []
    table = soup.find('table', class_='tableMobileScroll')
    if table:
        rows = table.find('tbody').find_all('tr')
        for row in rows:
            cols = [col.get_text(strip=True) for col in row.find_all('td')]
            if len(cols) == 3:
                history_rows.append((cols[0], cols[1], cols[2]))

    # Pad history rows to max 4
    while len(history_rows) < 4:
        history_rows.append(('', '', ''))

    # Flatten history rows for CSV
    history_flat = []
    for row in history_rows[:4]:
        history_flat.extend(row)

    return [
        name, gmc_number, registration_status, profession,
        qualification, full_registration_date, gender,
        'Not on GP Register', 'Not on Specialist Register',
        'Not subject to revalidation'
    ] + history_flat

def scrape_gmc_pages(start_id, max_ids=1000, max_consecutive_invalid=100000):
    start_time = time.time()
    valid_count = 0
    invalid_count = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()

        print(f"Starting scrape from GMC ID {start_id} for {max_ids} IDs...")
        for i in tqdm(range(start_id, start_id + max_ids), desc="Scraping Progress", unit="ID"):
            url = f'https://www.gmc-uk.org/registrants/{i}'
            page.goto(url, wait_until='networkidle')
            html = page.content()

            doctor_info = extract_doctor_info(html)
            if doctor_info:
                with open(csv_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(doctor_info)
                valid_count += 1
                invalid_count = 0  # reset invalid counter
            else:
                invalid_count += 1

            if invalid_count >= max_consecutive_invalid:
                print("\nStopping due to consecutive invalid IDs.")
                break

        browser.close()

    elapsed_time = time.time() - start_time
    print("\nScraping Completed!")
    print(f"Total Valid Entries: {valid_count}")
    print(f"Total Invalid Entries: {invalid_count}")
    print(f"Elapsed Time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")

scrape_gmc_pages(start_id=1457081, max_ids=100, max_consecutive_invalid=100000)

'''
Starting scrape from GMC ID 1457081 for 100 IDs...
Scraping Progress: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 100/100 [02:45<00:00,  1.66s/ID]

Scraping Completed!
Total Valid Entries: 2
Total Invalid Entries: 82
Elapsed Time: 168.34 seconds (2.81 minutes)
'''