from hashlib import sha1
import time
from .helpers import *
from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from hashlib import sha1
import os
import logging
from ckan import model
from ckan.model import Session, Package
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError

    
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
        prefs = {
            "download.default_directory": download_dir,
            "savefile.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(options=chrome_options)
    
        file_path = None
        try:
            base_url = 'https://www.evergabe-online.de'
            url = f'{base_url}/tenderdocuments.html?6&id={tender_id}'
            driver.get(url)
            print(download_dir)
            time.sleep(2)

            if "cookieCheck" in driver.current_url:
                driver.get(driver.current_url)
                time.sleep(2)
            contract_name_element = driver.find_element(By.CSS_SELECTOR, 'div.procedure-infos h4')
            contract_name = contract_name_element.text
            zip_button = driver.find_element(By.CSS_SELECTOR, 'a[title="Als ZIP-Datei herunterladen"]')
            zip_button.click()
            time.sleep(10)

            file_path = move_zip_file_to_public(download_dir)
        finally:
            driver.quit()
        return [file_path,contract_name]
    
def gather_stage_evergabeOnline(harvest_job):
        tender_ids = ['697632', '701009']
        return process_multiple_tenders_giving_publisher(tender_ids,harvest_job,download_tender_files_evergabeOnline,"evergabe_online")