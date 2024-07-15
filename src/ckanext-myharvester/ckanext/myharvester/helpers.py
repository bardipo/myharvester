from hashlib import sha1
import json
import logging
import os
import shutil
import zipfile
import requests
from ckan.model import Session, Package
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError


def unzip_file(file_path, extract_to):
        logging.debug('Unzipping file: %s' % file_path)
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            os.remove(file_path)
            
            # Recursively unzip any nested zip files and move files to extract_to
            for root, dirs, files in os.walk(extract_to):
                for file in files:
                    file_path = os.path.join(root, file)
                    if file.endswith('.zip'):
                        unzip_file(file_path, extract_to)
                    else:
                        # Move file to extract_to directory if it's not already there
                        if root != extract_to:
                            new_path = os.path.join(extract_to, file)
                            if not os.path.exists(new_path):
                                os.rename(file_path, new_path)
                            else:
                                # If file already exists, handle conflict (e.g., rename or skip)
                                logging.warning('File %s already exists in %s. Skipping.' % (file, extract_to))
                                os.remove(file_path)

            # Remove empty directories
            for root, dirs, files in os.walk(extract_to, topdown=False):
                for dir in dirs:
                    dir_path = os.path.join(root, dir)
                    if not os.listdir(dir_path):
                        os.rmdir(dir_path)
        except FileNotFoundError as e:
            logging.error('File not found during unzipping: %s' % str(e))
        except OSError as e:
            logging.error('Error while processing files during unzipping: %s' % str(e))


def import_stage_giving_publisher(harvest_object,publisher):
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

            response = requests.get(package_show_url, params={'id': tender_id.lower()}, headers={'Authorization': api_token})
            if response.status_code == 404:
                logging.debug('Dataset %s does not exist. Creating new dataset.' % tender_id)
                try:
                    create_dataset(package_create_url, api_token, tender_id.lower(), owner_org, contract_name)
                except Exception as e:
                    logging.error('Failed to create package %s: %s' % (tender_id, str(e)))
                    return False
            elif response.status_code != 200:
                logging.error('Failed to check if package exists %s: %s' % (tender_id, response.text))
                return False

            filename = os.path.basename(file_path)
            logging.debug('Uploading file %s to package %s' % (file_path, tender_id))
            if not upload_file(resource_create_url, api_token, file_path, tender_id.lower(), filename):
                return False

            return True

        except Exception as e:
            logging.error('Could not import dataset for object %s: %s' % (harvest_object.id, str(e)))
            return False

    
def create_dataset(api_url, api_token, package_id, owner_org, contract_name):
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

def upload_file(resource_create_url, api_token, file_path, package_id, filename):
        try:
            with open(file_path, 'rb') as f:
                response = requests.post(
                    resource_create_url,
                    headers={'Authorization': api_token},
                    files={'upload': (filename, f)},
                    data={'package_id': package_id, 'name': filename}
                )
            if response.status_code != 200:
                logging.error('Failed to upload file %s to package %s: %s' % (file_path, package_id, response.text))
                return False
            return True
        except requests.exceptions.RequestException as e:
            logging.error('Request failed: %s' % e)
            return False
        except Exception as e:
            logging.error('Error uploading file %s to package %s: %s' % (file_path, package_id, str(e)))
            return False


def ensure_directory_exists(path):
        if not os.path.exists(path):
            os.makedirs(path)
        return path


def process_multiple_tenders_giving_publisher(tender_ids,harvest_job,download_function,publisher_name):
        download_dir = "/srv/app/src_extensions/ckanext-myharvester/ckanext/myharvester/public"
        bieter_portal_path = ensure_directory_exists(os.path.join(download_dir, publisher_name))
        harvest_object_ids = []
        for tender_id in tender_ids:
            print(f"Processing tender ID: {tender_id}")
            tender_download_path = ensure_directory_exists(os.path.join(bieter_portal_path, tender_id))
            print(tender_download_path)
            zip_file_path, contract_name = download_function(tender_id, tender_download_path)
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



def move_zip_file_to_public(download_dir):
     
    file_path = max([os.path.join("/srv/app", f) for f in os.listdir("/srv/app")], key=os.path.getctime)
    shutil.move(file_path,download_dir)
    return os.path.join(download_dir, os.path.basename(file_path))