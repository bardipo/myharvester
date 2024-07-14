from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
import logging
from ckan import model
from ckan.model import Session, Package
import json
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError
from .helpers import *
from .vergabeAutobahn import gather_stage_vergabe_autobahn, fetch_stage_vergabe_autobahn
from .bieterPortal import gather_stage_bieter
from .evergabe import gather_stage_evergabe, fetch_stage_evergabe
from .evergabeOnline import gather_stage_evergabeOnline

class MyharvesterPlugin(SingletonPlugin):
    implements(IHarvester)

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
    


    def gather_stage(self, harvest_job):
        self.log.debug('Gather stage for: %s' % harvest_job.source.url)
        
        if "vergabe.autobahn.de" in harvest_job.source.url:
            return gather_stage_vergabe_autobahn(harvest_job)
        elif "https://bieterportal.noncd.db.de/" in harvest_job.source.url:
            return gather_stage_bieter(harvest_job)
        elif "https://www.evergabe.de/" in harvest_job.source.url:
            return gather_stage_evergabe(harvest_job)
        elif "https://www.evergabe-online.de/" in harvest_job.source.url:
            return gather_stage_evergabeOnline(harvest_job)

        raise HarvestGatherError()

    def fetch_stage(self, harvest_object):
        self.log.debug('Fetch stage for object: %s' % harvest_object.id)

        if "vergabe.autobahn.de" in self.get_original_url(harvest_object.id):
            return fetch_stage_vergabe_autobahn(harvest_object)
        elif "https://bieterportal.noncd.db.de/" in self.get_original_url(harvest_object.id):
            return True
        elif "https://www.evergabe.de/" in self.get_original_url(harvest_object.id):
            return fetch_stage_evergabe(harvest_object)
        elif "https://www.evergabe-online.de/" in self.get_original_url(harvest_object.id):
            return True
        
        return False
    def import_stage(self, harvest_object):
        self.log.debug('Import stage for object: %s' % harvest_object.id)
        self.log.debug('Harvesting object: %s' % harvest_object)

        if "vergabe.autobahn.de" in self.get_original_url(harvest_object.id):
            return import_stage_giving_publisher(harvest_object,"vergabe-autobahn")
        elif "https://bieterportal.noncd.db.de/" in self.get_original_url(harvest_object.id):
            return import_stage_giving_publisher(harvest_object,"bieter-portal-db")
        elif "https://www.evergabe.de/" in self.get_original_url(harvest_object.id):
            return import_stage_giving_publisher(harvest_object,"evergabe")
        elif "https://www.evergabe-online.de/" in self.get_original_url(harvest_object.id):
            return import_stage_giving_publisher(harvest_object,"evergabe-online")
            
        return False


    def _create_or_update_package(self, data_dict, harvest_object):
        pass

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}
