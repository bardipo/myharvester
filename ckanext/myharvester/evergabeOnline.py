from .helpers import *
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from .databaseConnection import get_tender_ids_evergabe_online
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
def download_tender_files_evergabeOnline(tender_id, download_dir):
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
            base_url = 'https://www.evergabe-online.de'
            url = f'{base_url}/tenderdocuments.html?6&id={tender_id}'
            logger.info("Scrapping the website for " + tender_id)
            driver.get(url)
            wait = WebDriverWait(driver,10)
            if "cookieCheck" in driver.current_url:
                driver.get(driver.current_url)
                wait = WebDriverWait(driver,10)
            zip_button = driver.find_element(By.CSS_SELECTOR, 'a[title="Als ZIP-Datei herunterladen"]')
            zip_button.click()
            wait_until_download_finishes(download_dir)
            file_path = give_latest_file(download_dir)
            unzip_file(file_path,download_dir)
            logger.info("Files downloaded for " + tender_id)
            return True
        except NoSuchElementException:
              logger.error("Nothing to Download for " + tender_id)
              return False
        finally:
            driver.quit()
    
def gather_stage_evergabeOnline(harvest_job):
        tender_ids = get_tender_ids_evergabe_online()
        return process_multiple_tenders_giving_publisher(tender_ids,harvest_job,download_tender_files_evergabeOnline,"evergabe_online")