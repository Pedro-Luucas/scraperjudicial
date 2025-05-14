import json
import os
import re
import requests
import sqlite3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import unquote, urljoin, urlparse, parse_qs
import time
import uuid
from datetime import datetime

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.page_load_strategy = 'eager'
    
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

def sanitize_process_number(process_number):
    """Remove special characters from the process number and format for DB name"""
    clean_number = re.sub(r'[^0-9]', '', process_number)
    # Format as XX.XXXX.X.XX.XXXX (common Brazilian process number format)
    if len(clean_number) >= 20:  # Full process number with verification digits
        return f"{clean_number[:7]}.{clean_number[7:9]}.{clean_number[9:13]}.{clean_number[13:14]}.{clean_number[14:16]}.{clean_number[16:20]}.{clean_number[20:22]}"
    elif len(clean_number) >= 13:  # Minimum viable process number
        return f"{clean_number[:7]}.{clean_number[7:9]}.{clean_number[9:13]}.{clean_number[13:14]}.{clean_number[14:16]}.{clean_number[16:20]}"
    else:
        return clean_number  # Fallback if number is too short

def get_db_connection(process_number):
    """Get SQLite connection for the specific process number"""
    db_name = f"processos/{sanitize_process_number(process_number)}.db"
    os.makedirs(os.path.dirname(db_name), exist_ok=True)
    
    conn = sqlite3.connect(db_name)
    conn.execute('''
    CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        doc_uuid TEXT UNIQUE,
        doc_type TEXT,
        doc_id TEXT,
        original_url TEXT,
        download_date TEXT,
        content BLOB
    )''')
    return conn

def get_pdf_url(driver):
    """Extracts the PDF URL from the process page"""
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "iframe, embed"))
        )
        time.sleep(1)

        elements = driver.find_elements(By.CSS_SELECTOR, 'iframe[src*="viewer.html?file="], embed[src*="viewer.html?file="]')
        for el in elements:
            if viewer_src := el.get_attribute("src"):
                if file_param := parse_qs(urlparse(viewer_src).query).get("file", [None])[0]:
                    return unquote(file_param)

        if match := re.search(r'file=([^"&]+getPDF\.do[^"&]+)', driver.page_source):
            return unquote(match.group(1))

        return None
    except Exception:
        return None

def save_to_database(process_number, pdf_url, pdf_content, doc_type, doc_id):
    """Saves the PDF to the appropriate SQLite database"""
    conn = None
    try:
        conn = get_db_connection(process_number)
        cursor = conn.cursor()
        
        # Generate metadata
        doc_uuid = str(uuid.uuid4())
        download_date = datetime.now().isoformat()
        
        cursor.execute('''
        INSERT INTO documents (doc_uuid, doc_type, doc_id, original_url, download_date, content)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (doc_uuid, doc_type, doc_id, pdf_url, download_date, sqlite3.Binary(pdf_content)))
        
        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"Database error: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()

def download_pdf(pdf_url, process_number, session, headers):
    """Downloads the PDF and saves it to the database"""
    try:
        response = session.get(pdf_url, headers=headers, timeout=15)
        response.raise_for_status()

        if not response.content.startswith(b'%PDF'):
            return False

        query = parse_qs(urlparse(pdf_url).query)
        doc_type = query.get("deTipoDocDigital", ["document"])[0]
        doc_id = query.get("idDocumento", ["no_id"])[0]

        # Clean up doc_type
        substitutions = {" ": "", "ç": "c", "ã": "a", "é": "e", "í": "i", "ó": "o", "ô": "o", "�":""}
        for old, new in substitutions.items():
            doc_type = doc_type.replace(old, new)

        if save_to_database(process_number, pdf_url, response.content, doc_type, doc_id):
            return True
        return False
    except Exception as e:
        print(f"Error downloading PDF: {str(e)}")
        return False

def process_case(process_data, driver, session, headers):
    """Processes an individual case"""
    clean_number = sanitize_process_number(process_data["numero_processo"])
    print(f"\nProcessing: {process_data['numero_processo']}")
    print(f"Formatted DB name: {clean_number}.db")
    print(f"URL: {process_data['link_processo']}")

    try:
        driver.get(process_data["link_processo"])
        
        if not driver.find_elements(By.CLASS_NAME, "linkMovVincProc"):
            print("No linked documents found")
            return

        doc_links = driver.find_elements(By.CLASS_NAME, "linkMovVincProc")
        unique_hrefs = {link.get_attribute("href") for link in doc_links if link.get_attribute("href")}

        print(f"Found {len(unique_hrefs)} documents to download")

        for href in unique_hrefs:
            print(f"\nAccessing document: {href}")
            driver.get(href)
            
            if pdf_url := get_pdf_url(driver):
                if pdf_url.startswith("/"):
                    pdf_url = urljoin("https://esaj.tjsp.jus.br", pdf_url)
                
                print(f"PDF URL found: {pdf_url}")
                
                session.cookies.clear()
                for cookie in driver.get_cookies():
                    session.cookies.set(cookie['name'], cookie['value'])
                
                if download_pdf(pdf_url, process_data["numero_processo"], session, headers):
                    print("✓ PDF saved to database successfully")
                else:
                    print("✕ Failed to save PDF to database")
            else:
                print("No PDF found on this page")

    except Exception as e:
        print(f"Error processing case: {str(e)}")

def find_json_files(folder_path):
    """Finds all JSON files in the specified folder"""
    json_files = []
    for file in os.listdir(folder_path):
        if file.endswith(".json"):
            json_files.append(os.path.join(folder_path, file))
    return sorted(json_files)

def main():
    # Configurations
    driver = setup_driver()
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    }

    try:
        # Creates processos folder if it doesn't exist
        os.makedirs("processos", exist_ok=True)

        # Finds all JSON files in the processes folder
        json_files = find_json_files("./processos")
        
        if not json_files:
            print("No JSON files found in the 'processos' folder")
            return

        print(f"Found {len(json_files)} JSON files to process")

        for json_file in json_files:
            print(f"\n=== Processing file: {json_file} ===")
            
            with open(json_file, "r", encoding="utf-8") as f:
                processes = json.load(f)

            for i, process in enumerate(processes, 1):
                print(f"\n[{i}/{len(processes)}] Starting processing...")
                process_case(process, driver, session, headers)
                
            print(f"\nCompleted processing file: {json_file}")
            
    finally:
        driver.quit()
        print("\nProcessing of all files completed!")

if __name__ == "__main__":
    main()