from hashlib import sha1
import json
import os
from bs4 import BeautifulSoup
import requests
from ckan import model
from ckan.model import Session, Package
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError
from urllib.parse import unquote
from .helpers import *


def fetch_download_urls_evergabe(tender_id):

        url = f'https://www.evergabe.de/unterlagen/{tender_id}'
        response = requests.get(url)
        file_paths = []
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', {'data-turbo': 'false', 'class': 'btn btn-primary'})
            final_links = []
            contract_name = soup.find('h2').text
            for link in links:
                final_links.append(f"https://www.evergabe.de/{link['href']}")
            base_directory = '/srv/app/src_extensions/ckanext-myharvester/ckanext/myharvester/public/evergabe'
            if not os.path.exists(base_directory):
                os.makedirs(base_directory)
            try:
                for link in final_links:
                    response = requests.get(link)
                    response.raise_for_status()
                    cd = response.headers.get('Content-Disposition')
                    if cd:
                        filename = cd.split('filename=')[-1].strip('"')
                    else:
                        filename = unquote(url.split('/')[-1])

                    directory_path = os.path.join(base_directory, tender_id)
                    if not os.path.exists(directory_path):
                        os.makedirs(directory_path)
                    file_path = os.path.join(directory_path, filename)

                    with open(file_path, 'wb') as f:
                        f.write(response.content)

                    if file_path.endswith('.zip'):
                        unzip_file(file_path, directory_path)
                files = os.listdir(directory_path)
                for file in files:
                     print(file)
                     file_paths.append(os.path.join(directory_path,file))
                print(file_paths)
            except requests.HTTPError as e:
                logging.error('HTTP error fetching %s: %s' % (url, str(e)))
                return False
            except requests.RequestException as e:
                logging.error('Error fetching %s: %s' % (url, str(e)))
                return False
            
            return contract_name, file_paths
        else:
            print("Failed to retrieve the webpage. Status code:", response.status_code)


def process_tenders_evergabe(tender_ids):
        all_tender_data = {}
        for tender_id in tender_ids:
            contract_name, file_paths = fetch_download_urls_evergabe(tender_id)
            all_tender_data[tender_id] = {
                'contract_name': contract_name,
                'file_paths': file_paths
            }
        return all_tender_data



def gather_stage_evergabe(harvest_job):

        tender_ids = ['019082de-42d6-4667-a75f-77c6388af649','019095fa-7ca4-40b8-9968-4a8d991b0768']
        all_tender_data = process_tenders_evergabe(tender_ids)

        harvest_object_ids = []

        for tender_id, data in all_tender_data.items():
            contract_name = data['contract_name']
            file_paths = data['file_paths']
            for file_path in file_paths:
                file_hash = sha1(file_path.encode('utf-8')).hexdigest()
                guid = f"{tender_id}-{file_hash}"
                obj = Session.query(HarvestObject).filter_by(guid=guid).first()
                if not obj:
                    content = json.dumps({'file_path': file_path, 'contract_name': contract_name})
                    obj = HarvestObject(guid=guid, job=harvest_job, content=content)
                    Session.add(obj)
                    Session.commit()
                harvest_object_ids.append(obj.id)

        return harvest_object_ids




def fetch_stage_evergabe(harvest_object):
        return True