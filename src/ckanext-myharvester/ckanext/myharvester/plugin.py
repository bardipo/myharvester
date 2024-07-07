from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
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
import zipfile
from bs4 import BeautifulSoup
import time

class MyharvesterPlugin(SingletonPlugin):
    implements(IHarvester)

    # Ensure logger is set up
    logging.basicConfig(level=logging.DEBUG)
    log = logging.getLogger(__name__)

  
    def info(self):
        return {
            'name': 'myharvester',
            'title': 'My Harvester',
            'description': 'A test harvester for CKAN that handles PDF and ZIP resources'
        }
    
    def get_original_url(self, harvest_object_id):
        obj = HarvestObject.get(harvest_object_id)
        if not obj:
            return None
        original_url = obj.source.url
        return original_url
    
    # VERGABE AUTOBAHN --------------------------------
    def fetch_download_urls_vergabe_autobahn(self, tender_id):
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

    def process_tenders_vergabe_autobahn(self, tender_ids):
        all_tender_data = {}
        for tender_id in tender_ids:
            contract_name, download_urls = self.fetch_download_urls_vergabe_autobahn(tender_id)
            all_tender_data[tender_id] = {
                'contract_name': contract_name,
                'download_urls': download_urls
            }
        return all_tender_data
    
    def gather_stage_vergabe_autobahn(self,harvest_job):

        tender_ids = ['19059a3fd19-630886ad86d4f3e6','190540c37e6-7065f4480bd645ac','19054b53176-7d861dcb50055eb4','1902b9c9902-346425e0d9f324ae']
        all_tender_data = self.process_tenders_vergabe_autobahn(tender_ids)

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
    

    def fetch_stage_vergabe_autobahn(self,harvest_object):
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
                self.unzip_file(file_path, directory_path)

            content = json.dumps({'file_path': file_path, 'contract_name': contract_name})
            harvest_object.content = content
            Session.commit()
            return True

        except requests.HTTPError as e:
            self.log.error('HTTP error fetching %s: %s' % (url, str(e)))
            return False
        except requests.RequestException as e:
            self.log.error('Error fetching %s: %s' % (url, str(e)))
            return False

    
    # VERGABE AUTOBAHN --------------------------------


    # BieterPortal DB --------------------------------

    def process_multiple_tenders_bieter(self,tender_ids, download_dir):
        bieter_portal_path = self.ensure_directory_exists(os.path.join(download_dir, 'bieter_portal'))
        for tender_id in tender_ids:
            print(f"Processing tender ID: {tender_id}")
            tender_download_path = self.ensure_directory_exists(os.path.join(bieter_portal_path, tender_id))
            print(tender_download_path)
            zip_file_path = self.download_tender_files_bieter(tender_id, tender_download_path)
            if zip_file_path:
                self.unzip_file(zip_file_path, tender_download_path)

    def ensure_directory_exists(self,path):
        if not os.path.exists(path):
            os.makedirs(path)
        return path
    
    def download_tender_files_bieter(self,tender_id, download_dir):
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
            url = f'https://bieterportal.noncd.db.de/evergabe.bieter/eva/supplierportal/portal/subproject/{tender_id}/details'
            driver.get(url)
            wait = WebDriverWait(driver, 10)
            ok_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.mat-focus-indicator.mat-tooltip-trigger.dialog-button-same-size.mat-stroked-button.mat-button-base.mat-primary[mat-dialog-close]")))
            ok_button.click()
            download_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.mat-focus-indicator.mat-tooltip-trigger.download-button-style.mat-stroked-button.mat-button-base.mat-primary")))
            download_button.click()
            time.sleep(15)  # Allow time for the download to complete
            file_list = [os.path.join(download_dir, f) for f in os.listdir(download_dir)]
            print(file_list)
            file_path = max([os.path.join(download_dir, f) for f in os.listdir(download_dir)], key=os.path.getctime)
        finally:
            driver.quit()
        return file_path
    
    def gather_stage_bieter(self,harvest_job):
        download_directory = "/srv/app/src_extensions/ckanext-myharvester/ckanext/myharvester/public"
        tender_ids = ['ceeb1cbd-e356-4249-b583-f3f8ccf044f2','eebba3cb-c144-4d07-9f87-31b1e3de0cce']
        self.process_multiple_tenders_bieter(tender_ids, download_directory)
        raise HarvestGatherError()




    # BieterPortal DB --------------------------------

    def gather_stage(self, harvest_job):
        self.log.debug('Gather stage for: %s' % harvest_job.source.url)
        
        if "vergabe.autobahn.de" in harvest_job.source.url:
            return self.gather_stage_vergabe_autobahn(harvest_job)
        elif "https://bieterportal.noncd.db.de/" in harvest_job.source.url:
            return self.gather_stage_bieter(harvest_job)

        raise HarvestGatherError()

    def fetch_stage(self, harvest_object):
        self.log.debug('Fetch stage for object: %s' % harvest_object.id)

        if "vergabe.autobahn.de" in self.get_original_url(harvest_object.id):
            return self.fetch_stage_vergabe_autobahn(harvest_object)
        
        return False
    def import_stage(self, harvest_object):
        self.log.debug('Import stage for object: %s' % harvest_object.id)
        self.log.debug('Harvesting object: %s' % harvest_object)

        if "vergabe.autobahn.de" in self.get_original_url(harvest_object.id):
            return self.import_stage_giving_publisher(harvest_object,"vergabe-autobahn")
            
        return False


    # GENERAL FUNCTIONS --------------------------------

    def import_stage_giving_publisher(self, harvest_object,publisher):
        try:
            base_api_url = 'http://localhost:5000/api/3/action'
            resource_create_url = f'{base_api_url}/resource_create'
            package_show_url = f'{base_api_url}/package_show'
            package_create_url = f'{base_api_url}/package_create'
            api_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJlMDNpQVFNSzE0QXZfbGV6NXhneW4yeWczZDVabDRNSVpVQnFXV0pkclJnIiwiaWF0IjoxNzIwMzY0MDI1fQ._tMt8Lrid-kuzZIzX4BFIeUiGaG8FZO7sDkDQJpQpLM'
            owner_org = publisher

            content = json.loads(harvest_object.content)
            file_path = content['file_path']
            contract_name = content['contract_name']
            guid = harvest_object.guid
            tender_id, url_hash = guid.split('-', 1)

            response = requests.get(package_show_url, params={'id': tender_id}, headers={'Authorization': api_token})
            if response.status_code == 404:
                self.log.debug('Dataset %s does not exist. Creating new dataset.' % tender_id)
                try:
                    self.create_dataset(package_create_url, api_token, tender_id, owner_org, contract_name)
                except Exception as e:
                    self.log.error('Failed to create package %s: %s' % (tender_id, str(e)))
                    return False
            elif response.status_code != 200:
                self.log.error('Failed to check if package exists %s: %s' % (tender_id, response.text))
                return False

            filename = os.path.basename(file_path)
            self.log.debug('Uploading file %s to package %s' % (file_path, tender_id))
            if not self.upload_file(resource_create_url, api_token, file_path, tender_id, filename):
                return False

            return True

        except Exception as e:
            self.log.error('Could not import dataset for object %s: %s' % (harvest_object.id, str(e)))
            return False

    def unzip_file(self, file_path, extract_to):
        self.log.debug('Unzipping file: %s' % file_path)
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            os.remove(file_path)
            
            # Recursively unzip any nested zip files and move files to extract_to
            for root, dirs, files in os.walk(extract_to):
                for file in files:
                    file_path = os.path.join(root, file)
                    if file.endswith('.zip'):
                        self.unzip_file(file_path, extract_to)
                    else:
                        # Move file to extract_to directory if it's not already there
                        if root != extract_to:
                            new_path = os.path.join(extract_to, file)
                            if not os.path.exists(new_path):
                                os.rename(file_path, new_path)
                            else:
                                # If file already exists, handle conflict (e.g., rename or skip)
                                self.log.warning('File %s already exists in %s. Skipping.' % (file, extract_to))
                                os.remove(file_path)

            # Remove empty directories
            for root, dirs, files in os.walk(extract_to, topdown=False):
                for dir in dirs:
                    dir_path = os.path.join(root, dir)
                    if not os.listdir(dir_path):
                        os.rmdir(dir_path)
        except FileNotFoundError as e:
            self.log.error('File not found during unzipping: %s' % str(e))
        except OSError as e:
            self.log.error('Error while processing files during unzipping: %s' % str(e))

    def create_dataset(self, api_url, api_token, package_id, owner_org, contract_name):
        response = requests.post(
            api_url,
            headers={'Authorization': api_token},
            json={
                'name': package_id,
                'title': contract_name,
                'owner_org': owner_org
            }
        )
        if response.status_code != 200:
            raise Exception('Failed to create package %s: %s' % (package_id, response.text))

    def upload_file(self, resource_create_url, api_token, file_path, package_id, filename):
        try:
            with open(file_path, 'rb') as f:
                response = requests.post(
                    resource_create_url,
                    headers={'Authorization': api_token},
                    files={'upload': (filename, f)},
                    data={'package_id': package_id, 'name': filename}
                )
            if response.status_code != 200:
                self.log.error('Failed to upload file %s to package %s: %s' % (file_path, package_id, response.text))
                return False
            return True
        except requests.exceptions.RequestException as e:
            self.log.error('Request failed: %s' % e)
            return False
        except Exception as e:
            self.log.error('Error uploading file %s to package %s: %s' % (file_path, package_id, str(e)))
            return False








    def _create_or_update_package(self, data_dict, harvest_object):
        pass

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}
