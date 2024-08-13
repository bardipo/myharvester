from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
import logging
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action
import json
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError
from .helpers import *
from .vergabeAutobahn import gather_stage_vergabe_autobahn
from .bieterPortal import gather_stage_bieter
from .evergabe import gather_stage_evergabe
from .evergabeOnline import gather_stage_evergabeOnline
from .vergabemarktplatz_brandenburg import gather_stage_vergabeBrandenburg
from .dtvp import gather_stage_dtvp
from .vergabeNiedersachsen import gather_stage_vergabe_niedersachsen
from .vergabeBremen import gather_stage_vergabe_bremen
from .meinauftrag import gather_stage_meinauftrag
from .ausmass import gather_stage_aumass
from .staatsanzeiger import gather_stage_staatsanzeiger
from .vergabeVmstart import gather_stage_vergabe_vmstart
from .vergabeNrw import gather_stage_vergabe_nrw
from .vmpRheinland import gather_stage_vmp_rheinland
from .base import HarvesterBase
from .importHelpers import import_stage_giving_publisher

class MyharvesterPlugin(HarvesterBase):
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
            if "IMPORT" in harvest_job.source.url:
                return gather_stage_vergabe_autobahn(harvest_job,True)
            else:
                return gather_stage_vergabe_autobahn(harvest_job)
        elif "https://bieterportal.noncd.db.de/" in harvest_job.source.url:
            if "IMPORT" in harvest_job.source.url:
                return gather_stage_bieter(harvest_job,True)
            else:
                return gather_stage_bieter(harvest_job)
        elif "https://www.evergabe.de/" in harvest_job.source.url:
            if "IMPORT" in harvest_job.source.url:
                return gather_stage_evergabe(harvest_job,True)
            else:
                return gather_stage_evergabe(harvest_job)
        elif "https://www.evergabe-online.de/" in harvest_job.source.url:
            if "IMPORT" in harvest_job.source.url:
                return gather_stage_evergabeOnline(harvest_job,True)
            else:
                return gather_stage_evergabeOnline(harvest_job)
        elif "https://vergabemarktplatz.brandenburg.de/" in harvest_job.source.url:
            if "IMPORT" in harvest_job.source.url:
                return gather_stage_vergabeBrandenburg(harvest_job,True)
            else:
                return gather_stage_vergabeBrandenburg(harvest_job)
        elif "https://www.dtvp.de/" in harvest_job.source.url:
            if "IMPORT" in harvest_job.source.url:
                return gather_stage_dtvp(harvest_job,True)
            else:
                return gather_stage_dtvp(harvest_job)
        elif "https://vergabe.niedersachsen.de/" in harvest_job.source.url:
            if "IMPORT" in harvest_job.source.url:
                return gather_stage_vergabe_niedersachsen(harvest_job,True)
            else:
                return gather_stage_vergabe_niedersachsen(harvest_job)
        elif "https://vergabe.bremen.de/" in harvest_job.source.url:
            if "IMPORT" in harvest_job.source.url:
                return gather_stage_vergabe_bremen(harvest_job,True)
            else:
                return gather_stage_vergabe_bremen(harvest_job)
        elif "https://www.meinauftrag.rib.de/" in harvest_job.source.url:
            if "IMPORT" in harvest_job.source.url:
                return gather_stage_meinauftrag(harvest_job,True)
            else:
                return gather_stage_meinauftrag(harvest_job)
        elif "https://plattform.aumass.de/" in harvest_job.source.url:
            if "IMPORT" in harvest_job.source.url:
                return gather_stage_aumass(harvest_job,True)
            else:
                return gather_stage_aumass(harvest_job)
        elif "https://www.staatsanzeiger-eservices.de/" in harvest_job.source.url:
            if "IMPORT" in harvest_job.source.url:
                return gather_stage_staatsanzeiger(harvest_job,True)
            else:
                return gather_stage_staatsanzeiger(harvest_job)
        elif "https://vergabe.vmstart.de/" in harvest_job.source.url:
            if "IMPORT" in harvest_job.source.url:
                return gather_stage_vergabe_vmstart(harvest_job,True)
            else:
                return gather_stage_vergabe_vmstart(harvest_job)
        elif "https://www.evergabe.nrw.de/" in harvest_job.source.url:
            if "IMPORT" in harvest_job.source.url:
                return gather_stage_vergabe_nrw(harvest_job,True)
            else:
                return gather_stage_vergabe_nrw(harvest_job)
        elif "https://www.vmp-rheinland.de/" in harvest_job.source.url:
            if "IMPORT" in harvest_job.source.url:
                return gather_stage_vmp_rheinland(harvest_job,True)
            else:
                return gather_stage_vmp_rheinland(harvest_job)

        raise HarvestGatherError()

    def fetch_stage(self, harvest_object):
        self.log.debug('Fetch stage for object: %s' % harvest_object.id)
        return True
    
    def import_stage(self, harvest_object):
        self.log.debug('Import stage for object: %s' % harvest_object.id)
        self.log.debug('Harvesting object: %s' % harvest_object)

        base_context = {'model': model, 'session': model.Session, 'user': self._get_user_name()}

        if not harvest_object:
            self.log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id, harvest_object, 'Import')
            return False

        try:
            package_dict = json.loads(harvest_object.content)
            tender_id = package_dict.get('tender_id')
            contract_name = package_dict.get('contract_name')
            resources = package_dict.get('resources', [])
            publisher_name = package_dict.get('publisher_name')

            if not resources:
                self._save_object_error('No resources found for object %s' % harvest_object.id, harvest_object, 'Import')
                return False

            package_dict = {
                'id': tender_id,
                'name': tender_id,
                'title': contract_name,
            }

            source_dataset = get_action('package_show')(base_context.copy(), {'id': harvest_object.job.source.id})
            local_org = source_dataset.get('owner_org')
            package_dict['owner_org'] = local_org

            ckan_resources = []
            for resource in resources:
                file_path = resource.get('url')
                if os.path.isfile(file_path):
                    ckan_resources.append({
                        'name': os.path.basename(file_path),
                        'url': f'https://procurdatstorage.file.core.windows.net/procurdat/public/{publisher_name}/{tender_id}/{os.path.basename(file_path)}/?sv=2022-11-02&ss=f&srt=o&sp=r&se=2100-08-14T03:45:24Z&st=2024-08-13T19:45:24Z&spr=https&sig=Ozmr5xEsjYVj%2BvrDO6qXmrBGuLjjeJpIHM1yK28jxDc%3D'
                    })

            package_dict = {
                'id': tender_id,
                'name': tender_id,
                'title': contract_name,
                'resources': ckan_resources,
                'owner_org' : local_org,
            }

            result = self._create_or_update_package(package_dict, harvest_object, package_dict_form='package_show')
            return result

        except ValidationError as e:
            self._save_object_error('Invalid package with GUID %s: %r' % (harvest_object.guid, e.error_dict), harvest_object, 'Import')
        except Exception as e:
            self._save_object_error('%s' % e, harvest_object, 'Import')

        return False
    