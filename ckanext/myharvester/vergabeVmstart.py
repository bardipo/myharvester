from .helpers import *
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from .databaseConnection import get_tender_ids_vergabe_vmstart
 
def download_tender_files_vergabe_vmstart(url, download_dir):
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
        logging.info("Scrapping website for " + url)
        driver.get(url)
        wait = WebDriverWait(driver, 10)
        
        if "PublicationControllerServlet" in url:
            link = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.hiddenForLoggedInUser.noPrint.color-main")))
            link.click()
            wait.until(EC.url_contains("TenderingProcedureDetails"))

        download_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.btn-modal.zipFileContents")))
        download_button.click()
        modal = wait.until(EC.visibility_of_element_located((By.ID, 'detailModal')))
        select_all_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Alles auswählen']")))
        select_all_button.click()
        confirm_download_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Auswahl herunterladen']")))
        confirm_download_button.click()
        wait_until_download_finishes(download_dir)
        file_path = give_latest_file(download_dir)
        unzip_file(file_path, download_dir)
        logging.info("Files downloaded for " + url)
        return True
    except TimeoutException:
        logging.info("Nothing to download for " + url)
        return False
    finally:
        driver.quit()
    
def gather_stage_vergabe_vmstart(harvest_job):
        tender_ids = get_tender_ids_vergabe_vmstart()
        return process_multiple_tenders_giving_publisher(tender_ids,harvest_job,download_tender_files_vergabe_vmstart,"vergabe_vmstart")

