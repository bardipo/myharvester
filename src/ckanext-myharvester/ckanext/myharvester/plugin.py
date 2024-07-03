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

class MyharvesterPlugin(SingletonPlugin):
    implements(IHarvester)

    # Ensure logger is set up
    logging.basicConfig(level=logging.DEBUG)
    log = logging.getLogger(__name__)

    def fetch_download_urls(self,tender_id):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920,1080")  # Standard desktop window size
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
            print("I got URL")
            wait = WebDriverWait(driver, 10)
            print("I waited")
            download_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.btn-modal.zipFileContents")))
            print("I got download button")
            driver.execute_script("arguments[0].scrollIntoView();", download_button)
            print("I am doing something")
            ActionChains(driver).move_to_element(download_button).click().perform()
            print("I clicked")
            wait.until(EC.visibility_of_element_located((By.ID, 'detailModal')))
            print("I got download screen")
            links = [link.get_attribute('href') for link in driver.find_elements(By.CSS_SELECTOR, '#detailModal a')]
            return links
        finally:
            driver.quit()

    def process_tenders(self,tender_ids):
        all_tender_urls = {}
        for tender_id in tender_ids:
            print(f"Fetching URLs for tender ID: {tender_id}")
            download_urls = self.fetch_download_urls(tender_id)
            all_tender_urls[tender_id] = download_urls
            print(f"Found {len(download_urls)} URLs for Tender ID {tender_id}")
        return all_tender_urls


    def info(self):
        return {
            'name': 'myharvester',
            'title': 'My Harvester',
            'description': 'A test harvester for CKAN that handles PDF and ZIP resources'
        }

    def gather_stage(self, harvest_job):
        self.log.debug('Gather stage for: %s' % harvest_job.source.url)
        
        # Example tender IDs; replace with dynamic fetching logic if required
        tender_ids = ['190540c37e6-7065f4480bd645ac', '19054b53176-7d861dcb50055eb4', '19059a3fd19-630886ad86d4f3e6']
        
        # Fetch all URLs for the provided tender IDs
        all_tender_urls = self.process_tenders(tender_ids)

        harvest_object_ids = []

        for tender_id, urls in all_tender_urls.items():
            for url in urls:
                # Generate a SHA1 hash for the URL to use as the GUID
                guid = sha1(url.encode('utf-8')).hexdigest()
                
                # Check if the HarvestObject already exists
                obj = Session.query(HarvestObject).filter_by(guid=guid).first()
                if not obj:
                    # Create a new HarvestObject if it doesn't exist
                    obj = HarvestObject(guid=guid, job=harvest_job, content=url)
                    obj.save()
                
                harvest_object_ids.append(obj.id)

        # Set configuration from the harvest job source
        self._set_config(harvest_job.source.config)

        # Check if this source has been harvested before
        previous_job = Session.query(HarvestJob) \
                        .filter(HarvestJob.source == harvest_job.source) \
                        .filter(HarvestJob.gather_finished != None) \
                        .filter(HarvestJob.id != harvest_job.id) \
                        .order_by(HarvestJob.gather_finished.desc()) \
                        .limit(1).first()

        return harvest_object_ids

    def fetch_stage(self, harvest_object):
        self.log.debug('Fetch stage for object: %s' % harvest_object.id)
        url = harvest_object.content
        try:
            response = requests.get(url)
            response.raise_for_status()

            content_type = response.headers.get('Content-Type', '')
            if 'application/pdf' in content_type or url.lower().endswith('.pdf'):
                file_extension = 'pdf'
            elif 'application/zip' in content_type or url.lower().endswith('.zip'):
                file_extension = 'zip'
            else:
                self.log.error('Unsupported file type for URL %s: %s' % (url, content_type))
                return False

            filename = sha1(url.encode('utf-/8')).hexdigest() + '.' + file_extension
            file_path = os.path.join(r'/srv/app/src_extensions/ckanext-myharvester/ckanext/myharvester/public/', filename)

            with open(file_path, 'wb') as f:
                f.write(response.content)

            harvest_object.content = file_path
            harvest_object.save()
            return True

        except requests.HTTPError as e:
            self.log.error('HTTP error fetching %s: %s' % (url, str(e)))
            return False
        except requests.RequestException as e:
            self.log.error('Error fetching %s: %s' % (url, str(e)))
            return False

    def import_stage(self, harvest_object):
        self.log.debug('Import stage for object: %s' % harvest_object.id)
        self.log.debug('Harvesting object: %s' % harvest_object)
        try:
            dataset_dict = {
                'name': sha1(harvest_object.content.encode('utf-8')).hexdigest(),
                'title': 'Dataset from PDF ' + harvest_object.content,
                'resources': [{
                    'url': harvest_object.content,
                    'format': 'PDF'
                }]
            }

            package_id = self._create_or_update_package(dataset_dict, harvest_object)
            
            if package_id:
                harvest_object.current = True
                harvest_object.package_id = package_id
                harvest_object.save()
                return True
            else:
                return False
        except Exception as e:
            self.log.error('Could not import dataset for object %s: %s' % (harvest_object.id, str(e)))
            return False

    def _create_or_update_package(self, data_dict, harvest_object):
        pass

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}
