from datetime import datetime, timezone
from hashlib import sha1
import json
import os
import re
import shutil
import time
import zipfile
import requests
from ckan.model import Session, Package
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError
import logging
from pathlib import Path

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def extract_password_from_filename(filename):
    match = re.search(r'Kennwort (\S+)\.zip', filename)
    if match:
        return match.group(1)
    return None

def unzip_file(file_path, extract_to, password=None):
    file_path = Path(file_path)
    extract_to = Path(extract_to)

    logger.debug(f'Unzipping file: {file_path}')
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            if password:
                zip_ref.extractall(extract_to, pwd=password.encode())
            else:
                zip_ref.extractall(extract_to)
        os.remove(file_path)
        
        for root, dirs, files in os.walk(extract_to):
            for file in files:
                nested_file_path = Path(root) / file
                if file.endswith('.zip'):
                    password = extract_password_from_filename(file)
                    unzip_file(nested_file_path, extract_to, password)
                else:
                    if Path(root) != extract_to:
                        new_path = extract_to / file
                        if not new_path.exists():
                            shutil.move(str(nested_file_path), str(new_path))
                        else:
                            logger.warning(f'File {file} already exists in {extract_to}. Skipping.')
                            nested_file_path.unlink()

        for root, dirs, files in os.walk(extract_to, topdown=False):
            for dir in dirs:
                dir_path = Path(root) / dir
                if not any(dir_path.iterdir()):
                    dir_path.rmdir()

    except zipfile.BadZipFile as e:
        logger.error(f'BadZipFile error while unzipping: {str(e)}')
        corrupted_dir = Path('/code/log')
        if not corrupted_dir.exists():
            corrupted_dir.mkdir(parents=True, exist_ok=True)
        with open(corrupted_dir / 'corruptedlogs.txt', 'a') as log_file:
            log_file.write(f'Corrupted file moved from: {file_path} | Date: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}\n')
        corrupted_path = corrupted_dir / file_path.name
        shutil.move(str(file_path), str(corrupted_path))

    except FileNotFoundError as e:
        logger.error(f'File not found during unzipping: {str(e)}')

    except OSError as e:
        logger.error(f'Error while processing files during unzipping: {str(e)}')

    except RuntimeError as e:
        logger.error(f'Error while processing files during unzipping: {str(e)}')
        logger.info(f'Skipping file due to RuntimeError: {file_path}')
        


def ensure_directory_exists(path):
        if not os.path.exists(path):
            os.makedirs(path)
        return path


def process_multiple_tenders_giving_publisher(tenders, harvest_job, download_function, publisher_name):
    download_dir = ensure_directory_exists(os.path.join("/storage","public"))
    publisher_path = ensure_directory_exists(os.path.join(download_dir, publisher_name))
    harvest_object_ids = []
    total_tenders = len(tenders)
    for index, tender in enumerate(tenders, start=1):
        tender_id = tender["tender_id"]
        contract_name = tender["title"]
        doc = tender["document"]
        logger.info(f"Processing {tender_id} ({index}/{total_tenders})")

        if has_offline_tag(tender_id.lower()):
            continue

        tender_download_path = ensure_directory_exists(os.path.join(publisher_path, tender_id))
        if publisher_name in ["vergabe_autobahn", "vergabe_bremen", "meinauftrag", "aumass", "staatsanzeiger", "vergabe_vmstart"]:
            download_result = download_function(tender["url"], tender_download_path)
        else:
            download_result = download_function(tender_id, tender_download_path)

        if not os.listdir(tender_download_path):
            shutil.rmtree(tender_download_path)
            logger.info("No Files for " + tender_id + " deleting folder and skipping...")
            continue

        if not download_result:
            logger.info("No more Files for " + tender_id + " adding offline tag and skipping...")
            add_offline_tag(tender_id.lower())
            continue

        # Save the document metadata as Meta.json
        meta_json_path = os.path.join(tender_download_path, 'Meta.json')
        try:
            with open(meta_json_path, 'w', encoding='utf-8') as file:
                json.dump(doc, file, ensure_ascii=False, indent=4)
        except Exception as e:
            logger.error(f"Error saving document {tender_id} to meta.json: {e}")
        resources = []
        for file in os.listdir(tender_download_path):
            file_path = os.path.join(tender_download_path, file)
            if os.path.isfile(file_path):
                resources.append({'url': file_path, 'name': file})
        
        guid = sha1(f"{publisher_name}-{tender_id}".encode('utf-8')).hexdigest()
        obj = Session.query(HarvestObject).filter_by(guid=guid).first()
        
        if not obj:
            content = json.dumps({'resources': resources, 'contract_name': contract_name, 'tender_id': tender_id, 'publisher_name': publisher_name})
            obj = HarvestObject(guid=guid, job=harvest_job, content=content)
            Session.add(obj)
        else:
            obj.content = json.dumps({'resources': resources, 'contract_name': contract_name, 'tender_id': tender_id, 'publisher_name': publisher_name})
            obj.job = harvest_job
        Session.commit()
        harvest_object_ids.append(obj.id)
    return harvest_object_ids

def process_multiple_tenders_without_download(tenders, harvest_job, publisher_name):
    download_dir = "/storage/public"
    publisher_path = os.path.join(download_dir, publisher_name)
    harvest_object_ids = []
    
    for tender in tenders:
        tender_id = tender["tender_id"]
        contract_name = tender["title"]
        logger.info(f"Processing tender ID: {tender_id}")
        
        tender_download_path = os.path.join(publisher_path, tender_id)
        if not os.path.exists(tender_download_path):
            logger.info(f"No folder found for tender ID: {tender_id}. Skipping.")
            continue
        
        resources = []
        for file in os.listdir(tender_download_path):
            file_path = os.path.join(tender_download_path, file)
            if os.path.isfile(file_path):
                resources.append({'url': file_path, 'name': file})
        
        guid = sha1(f"{publisher_name}-{tender_id}".encode('utf-8')).hexdigest()
        obj = Session.query(HarvestObject).filter_by(guid=guid).first()
        
        if not obj:
            content = json.dumps({'resources': resources, 'contract_name': contract_name, 'tender_id': tender_id,'publisher_name': publisher_name})
            obj = HarvestObject(guid=guid, job=harvest_job, content=content)
            Session.add(obj)
        else:
            obj.content = json.dumps({'resources': resources, 'contract_name': contract_name, 'tender_id': tender_id,'publisher_name': publisher_name})
            obj.job = harvest_job
        Session.commit()
        harvest_object_ids.append(obj.id)
    
    return harvest_object_ids



def wait_until_download_finishes(download_dir):
    dl_wait = True
    while dl_wait:
        time.sleep(5)
        dl_wait = False
        for file_name in os.listdir(download_dir):
            if file_name.endswith('.crdownload'):
                dl_wait = True



def has_offline_tag(dataset_id):
    ckan_url = 'https://procurdat.azurewebsites.net'
    api_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiItdTl5NEJmNklNWHl1RHQ5M1Z5dFdaOURWS19LMGoxSmQ3aGJmUlUyT0pRIiwiaWF0IjoxNzIzMTYwNTAwfQ.pvJ0AV29WKHSaOCjW1uF3Ga-4sucHhS6sov0p2lOTzA'

    package_show_url = f'{ckan_url}/api/3/action/package_show'
    payload = {'id': dataset_id}
    headers = {'Authorization': api_key, 'Content-Type': 'application/json'}
    try:
        response = requests.post(package_show_url, data=json.dumps(payload), headers=headers)
        
        if response.status_code == 200:
            dataset_info = response.json()
            tags = dataset_info['result']['tags']
            for tag in tags:
                if tag['name'].lower() == 'offline':
                    return True
            return False
        elif response.status_code == 404:
            print(f"Dataset with ID '{dataset_id}' does not exist.")
            return False
        else:
            print("Failed to get dataset info:", response.text)
            return False
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return False
    

def add_offline_tag(dataset_id):
    ckan_url = 'https://procurdat.azurewebsites.net'
    api_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiItdTl5NEJmNklNWHl1RHQ5M1Z5dFdaOURWS19LMGoxSmQ3aGJmUlUyT0pRIiwiaWF0IjoxNzIzMTYwNTAwfQ.pvJ0AV29WKHSaOCjW1uF3Ga-4sucHhS6sov0p2lOTzA'

    package_show_url = f'{ckan_url}/api/3/action/package_show'
    package_patch_url = f'{ckan_url}/api/3/action/package_patch'

    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }

    response = requests.post(package_show_url, data=json.dumps({'id': dataset_id}), headers=headers)

    if response.status_code != 200:
        print(f"Failed to get dataset info: {response.text}")
        return False

    dataset_info = response.json()
    tags = dataset_info['result']['tags']
    tags.append({'name': 'Offline'})

    payload = {
        'id': dataset_id,
        'tags': tags
    }

    response = requests.post(package_patch_url, data=json.dumps(payload), headers=headers)

    if response.status_code == 200:
        print(f"The 'Offline' tag has been added to the dataset {dataset_id}.")
        return True
    else:
        print(f"Failed to add 'Offline' tag: {response.text}")
        return False
    

def give_latest_file(download_dir):

    return max([os.path.join(download_dir, f) for f in os.listdir(download_dir)], key=os.path.getctime)