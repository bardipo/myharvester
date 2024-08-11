from .helpers import *
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from .databaseConnection import get_tender_ids_vergabemarktplatz_brandenburg

import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  

def download_tender_files_vergabe_brandenburg(tender_id, download_dir):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument('start-maximized')
        chrome_options.add_argument('disable-infobars')
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
        })
        driver = webdriver.Chrome(options=chrome_options)
    
        file_path = None
        try:
            logger.info("Scrapping website for " + tender_id)
            url = f"https://vergabemarktplatz.brandenburg.de/VMPSatellite/public/company/project/{tender_id}/de/documents"
            driver.get(url)
            wait = WebDriverWait(driver,10)
            download_button = driver.find_element(By.XPATH, "//a[contains(@title, 'Alle Dokumente als ZIP-Datei herunterladen')]")
            download_button.click()
            wait_until_download_finishes(download_dir)
            file_path = give_latest_file(download_dir)
            unzip_file(file_path,download_dir)
            logger.info("Files downloaded for " + tender_id) 
            return True
        except NoSuchElementException:
              logger.info("Nothing to download for " + tender_id)
              return False
        finally:
            driver.quit()
    
def gather_stage_vergabeBrandenburg(harvest_job,justImport = False):
        tender_ids = get_tender_ids_vergabemarktplatz_brandenburg()
        if justImport:
            return process_multiple_tenders_without_download(tender_ids,harvest_job,"vergabe_brandenburg")
        else:
            return process_multiple_tenders_giving_publisher(tender_ids,harvest_job,download_tender_files_vergabe_brandenburg,"vergabe_brandenburg")