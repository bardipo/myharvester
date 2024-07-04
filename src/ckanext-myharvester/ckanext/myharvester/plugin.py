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

  
    def info(self):
        return {
            'name': 'myharvester',
            'title': 'My Harvester',
            'description': 'A test harvester for CKAN that handles PDF and ZIP resources'
        }

    def gather_stage(self, harvest_job):
        self.log.debug('Gather stage for: %s' % harvest_job.source.url)
        
      
        
        all_tender_urls = ["https://raw.githubusercontent.com/bardipo/testscrapper/main/1.pdf","https://raw.githubusercontent.com/bardipo/testscrapper/main/2.pdf","https://raw.githubusercontent.com/bardipo/testscrapper/main/3.pdf"]

        harvest_object_ids = []

        for url in all_tender_urls:
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
