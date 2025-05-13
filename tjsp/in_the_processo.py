import json
import os
import re
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import unquote, urljoin, urlparse, parse_qs
import time

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
    """Remove special characters from the process number"""
    return re.sub(r'[^0-9]', '', process_number)

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

def download_pdf(pdf_url, process_folder, session, headers):
    """Downloads and saves the PDF"""
    try:
        response = session.get(pdf_url, headers=headers, timeout=15)
        response.raise_for_status()

        if not response.content.startswith(b'%PDF'):
            return False

        query = parse_qs(urlparse(pdf_url).query)
        doc_type = query.get("deTipoDocDigital", ["document"])[0]
        doc_id = query.get("idDocumento", ["no_id"])[0]

        substitutions = {" ": "", "ç": "c", "ã": "a", "é": "e", "í": "i", "ó": "o", "ô": "o", "�":""}
        for old, new in substitutions.items():
            doc_type = doc_type.replace(old, new)

        filename = f"{doc_type}_{doc_id}.pdf"
        filepath = os.path.join(process_folder, filename)

        with open(filepath, "wb") as f:
            f.write(response.content)

        return True
    except Exception as e:
        print(f"Error downloading PDF: {str(e)}")
        return False

def process_case(process_data, driver, session, headers):
    """Processes an individual case"""
    clean_number = sanitize_process_number(process_data["numero_processo"])
    process_folder = os.path.join("process_documents", clean_number)
    os.makedirs(process_folder, exist_ok=True)

    print(f"\nProcessing: {process_data['numero_processo']}")
    print(f"Folder: {process_folder}")
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
                
                if download_pdf(pdf_url, process_folder, session, headers):
                    print("✓ PDF downloaded successfully")
                else:
                    print("✕ Failed to download PDF")
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
        # Creates main folder if it doesn't exist
        os.makedirs("process_documents", exist_ok=True)

        # Finds all JSON files in the processes folder
        json_files = find_json_files("./processos")
        
        if not json_files:
            print("No JSON files found in the 'processes' folder")
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