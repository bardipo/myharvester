from .helpers import *
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from .databaseConnection import get_tender_ids_aumass

logging.basicConfig(level=logging.INFO)
 
def download_tender_files_aumass(url, download_dir):
    # Set up Chrome options
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
        logging.info("Scrapping the website for " + url)
        driver.get(url)
        try:
            cookies_accept_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//a[@class='cc-btn cc-dismiss' and text()='Ich stimme zu.']"))
            )
            cookies_accept_button.click()
        except TimeoutException:
            logging.info("No Cookies")
        
        wait = WebDriverWait(driver, 10)
        download_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@class, 'btn-download') and contains(text(), 'DOWNLOAD')]")))
        
        download_button.click()
        
        try:
            modal = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'modal-dialog'))
            )
            ohne_registrierung_button = WebDriverWait(driver, 1).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@id='freierDownloadLinkArea']/a[contains(text(), 'Ohne Registrierung herunterladen')]"))
            )
            ohne_registrierung_button.click()

        except TimeoutException:
            logging.info("No Modal Check for " + url)

        wait_until_download_finishes(download_dir)
        file_path = give_latest_file(download_dir)
        unzip_file(file_path, download_dir)
        logging.info("Downloaded files for " + url)
        return True

    except NoSuchElementException:
        logging.error("Nothing to download for " + url)
        return False
    except Exception as e:
        logging.error(str(e))
        return False
    finally:
        driver.quit()
    
def gather_stage_aumass(harvest_job):
        tender_ids = get_tender_ids_aumass()
        return process_multiple_tenders_giving_publisher(tender_ids,harvest_job,download_tender_files_aumass,"aumass")

