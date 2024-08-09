from .helpers import *
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from .databaseConnection import get_tender_ids_dtvp

logging.basicConfig(level=logging.INFO)
    
def download_tender_files_dtvp(tender_id, download_dir):
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
            url = f"https://www.dtvp.de/Satellite/public/company/project/{tender_id}/de/documents"
            logging.info("Scrapping the website for " + tender_id)
            driver.get(url)
            wait = WebDriverWait(driver,10)
            download_button = driver.find_element(By.XPATH, "//a[contains(@title, 'Alle Dokumente als ZIP-Datei herunterladen')]")
            download_button.click()
            wait_until_download_finishes(download_dir)
            file_path = give_latest_file(download_dir)
            unzip_file(file_path,download_dir)
            logging.info("Files downloaded for " + tender_id) 
            return True
        except NoSuchElementException:
              logging.error("Nothing to download for " + tender_id)
              return False
        finally:
            driver.quit()
    
def gather_stage_dtvp(harvest_job):
        tender_ids = get_tender_ids_dtvp()
        return process_multiple_tenders_giving_publisher(tender_ids,harvest_job,download_tender_files_dtvp,"dtvp")