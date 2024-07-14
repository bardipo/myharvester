from hashlib import sha1
import requests
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
from urllib.parse import urlparse, unquote
import re
from .helpers import *


def process_tenders_vergabe_autobahn(tender_ids):
        all_tender_data = {}
        for tender_id in tender_ids:
            contract_name, download_urls = fetch_download_urls_vergabe_autobahn(tender_id)
            all_tender_data[tender_id] = {
                'contract_name': contract_name,
                'download_urls': download_urls
            }
        return all_tender_data


def fetch_download_urls_vergabe_autobahn(tender_id):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--no-sandbox") 
        chrome_options.add_argument("--disable-gpu") 
        chrome_options.add_argument('start-maximized') 
        chrome_options.add_argument('disable-infobars')
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument('--disable-dev-shm-usage')

        driver = webdriver.Chrome(options=chrome_options)

        try:
            print("ITS WORKING")
            url = f'https://vergabe.autobahn.de/NetServer/TenderingProcedureDetails?function=_Details&TenderOID=54321-NetTender-{tender_id}&thContext=publications'
            driver.get(url)
            wait = WebDriverWait(driver, 10)
            contract_name = driver.find_element(By.XPATH, '//h1[@class="color-main"]/small').text
            download_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.btn-modal.zipFileContents")))
            download_button.click()
            wait.until(EC.visibility_of_element_located((By.ID, 'detailModal')))
            links = [link.get_attribute('href') for link in driver.find_elements(By.CSS_SELECTOR, '#detailModal a')]
            return contract_name, links
        finally:
            driver.quit()


def gather_stage_vergabe_autobahn(harvest_job):

        tender_ids = ['19059a3fd19-630886ad86d4f3e6','190540c37e6-7065f4480bd645ac','19054b53176-7d861dcb50055eb4','1902b9c9902-346425e0d9f324ae']
        all_tender_data = process_tenders_vergabe_autobahn(tender_ids)

        harvest_object_ids = []

        for tender_id, data in all_tender_data.items():
            contract_name = data['contract_name']
            urls = data['download_urls']
            for url in urls:
                url_hash = sha1(url.encode('utf-8')).hexdigest()
                guid = f"{tender_id}-{url_hash}"
                obj = Session.query(HarvestObject).filter_by(guid=guid).first()
                if not obj:
                    content = json.dumps({'url': url, 'contract_name': contract_name})
                    obj = HarvestObject(guid=guid, job=harvest_job, content=content)
                    Session.add(obj)
                    Session.commit()
                harvest_object_ids.append(obj.id)

        return harvest_object_ids
    

def fetch_stage_vergabe_autobahn(harvest_object):
        content = json.loads(harvest_object.content)
        url = content['url']
        contract_name = content['contract_name']
        guid = harvest_object.guid
        tender_id, url_hash = guid.split('-', 1)

        # Create 'vergabe_autobahn' directory if it doesn't exist
        base_directory = '/srv/app/src_extensions/ckanext-myharvester/ckanext/myharvester/public/vergabe_autobahn'
        if not os.path.exists(base_directory):
            os.makedirs(base_directory)

        try:
            response = requests.get(url)
            response.raise_for_status()
            cd = response.headers.get('Content-Disposition')
            if cd:
                filename = re.findall('filename="?([^";]+)"?', cd)[0]
            else:
                filename = unquote(os.path.basename(urlparse(url).path))

            content_type = response.headers.get('Content-Type', '')
            if '.' not in filename:
                if 'application/pdf' in content_type:
                    filename += '.pdf'
                elif 'application/zip' in content_type:
                    filename += '.zip'

            filename = filename.strip().replace('', '')

            # Set the directory path to include 'vergabe_autobahn'
            directory_path = os.path.join(base_directory, tender_id)
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)
            file_path = os.path.join(directory_path, filename)

            with open(file_path, 'wb') as f:
                f.write(response.content)

            if file_path.endswith('.zip'):
                unzip_file(file_path, directory_path)

            content = json.dumps({'file_path': file_path, 'contract_name': contract_name})
            harvest_object.content = content
            Session.commit()
            return True

        except requests.HTTPError as e:
            logging.error('HTTP error fetching %s: %s' % (url, str(e)))
            return False
        except requests.RequestException as e:
            logging.error('Error fetching %s: %s' % (url, str(e)))
            return False
        







