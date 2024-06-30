from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import zipfile

def download_tender_files(tender_id, download_dir):
    chrome_options = Options()
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=chrome_options)

    try:
        url = f'https://vergabe.autobahn.de/NetServer/TenderingProcedureDetails?function=_Details&TenderOID=54321-NetTender-{tender_id}&thContext=publications'
        driver.get(url)
        wait = WebDriverWait(driver, 10)
        download_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.btn-modal.zipFileContents")))
        download_button.click()
        modal = wait.until(EC.visibility_of_element_located((By.ID, 'detailModal')))
        select_all_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Alles auswählen']")))
        select_all_button.click()
        confirm_download_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Auswahl herunterladen']")))
        confirm_download_button.click()
        time.sleep(10)  # Adjust time as necessary for your connection speed
    finally:
        driver.quit()

def unzip_files(download_dir, tender_id):
    # Directory specific to this tender ID
    tender_dir = os.path.join(download_dir, f"{tender_id}_files")
    if not os.path.exists(tender_dir):
        os.makedirs(tender_dir)

    for filename in os.listdir(download_dir):
        file_path = os.path.join(download_dir, filename)
        if filename.endswith(".zip"):
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(tender_dir)
            os.remove(file_path)
            print(f"Extracted and removed ZIP file: {filename}")

    # Now recursively unzip any nested ZIP files
    unzip_recursively(tender_dir)

def unzip_recursively(directory):
    for foldername, subfolders, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith('.zip'):
                file_path = os.path.join(foldername, filename)
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(foldername)
                os.remove(file_path)
                print(f"Extracted and removed nested ZIP file: {filename}")
                # Recursively unzip newly extracted folders if they contain any ZIP files
                unzip_recursively(foldername)

def process_multiple_tenders(tender_ids, download_dir):
    for tender_id in tender_ids:
        print(f"Processing tender ID: {tender_id}")
        download_tender_files(tender_id, download_dir)
        unzip_files(download_dir, tender_id)

download_directory = r"C:\Users\Mert As\Desktop\scrapper"
tender_ids = ['190540c37e6-7065f4480bd645ac', '19054b53176-7d861dcb50055eb4', '19059a3fd19-630886ad86d4f3e6']    
process_multiple_tenders(tender_ids, download_directory)
