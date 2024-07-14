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

def process_multiple_tenders_bieter(tender_ids, download_dir,harvest_job):
        bieter_portal_path = ensure_directory_exists(os.path.join(download_dir, 'bieter_portal'))
        harvest_object_ids = []
        for tender_id in tender_ids:
            print(f"Processing tender ID: {tender_id}")
            tender_download_path = ensure_directory_exists(os.path.join(bieter_portal_path, tender_id))
            print(tender_download_path)
            zip_file_path, contract_name = download_tender_files_bieter(tender_id, f"{tender_download_path}/")
            if zip_file_path:
                unzip_file(zip_file_path, tender_download_path)
            files = os.listdir(tender_download_path)
            for file in files:
                file_path = os.path.join(tender_download_path,file)
                if os.path.isfile(file_path):
                    file_hash = sha1(os.path.basename(file_path).encode('utf-8')).hexdigest()
                    guid = f"{tender_id}-{file_hash}"
                    obj = Session.query(HarvestObject).filter_by(guid=guid).first()
                    if not obj:
                        content = json.dumps({'file_path': file_path, 'contract_name': contract_name})
                        obj = HarvestObject(guid=guid, job=harvest_job, content=content)
                        Session.add(obj)
                        Session.commit()
                    harvest_object_ids.append(obj.id)
        return harvest_object_ids


def ensure_directory_exists(path):
        if not os.path.exists(path):
            os.makedirs(path)
        return path
    
def download_tender_files_bieter(tender_id, download_dir):
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
        print(chrome_options.to_capabilities())

        file_path = None
        try:
            url = f'https://bieterportal.noncd.db.de/evergabe.bieter/eva/supplierportal/portal/subproject/{tender_id}/details'
            driver.get(url)
            wait = WebDriverWait(driver, 10)
            contract_name = ""
            title_div = wait.until(EC.visibility_of_element_located((By.XPATH, "//div[text()='Titel']")))
            if title_div:
                value_div = title_div.find_element(By.XPATH, "./following-sibling::div")
                contract_name = value_div.text.strip()
            download_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Herunterladen']]")))
            download_button.click()
            time.sleep(10)
            file_path = max([os.path.join(download_dir, f) for f in os.listdir(download_dir)], key=os.path.getctime)
        finally:
            driver.quit()
        return [file_path,contract_name]
    
def gather_stage_bieter(harvest_job):
        download_directory = "/srv/app/src_extensions/ckanext-myharvester/ckanext/myharvester/public"
        tender_ids = ['ceeb1cbd-e356-4249-b583-f3f8ccf044f2','eebba3cb-c144-4d07-9f87-31b1e3de0cce']
        return process_multiple_tenders_bieter(tender_ids, download_directory,harvest_job)

