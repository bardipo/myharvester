from datetime import datetime, timezone
import json
import logging
import os
import requests
import time


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def import_stage_giving_publisher(harvest_object, publisher):
    try:
        base_api_url = 'https://procurdat.azurewebsites.net/api/3/action'
        resource_create_url = f'{base_api_url}/resource_create'
        package_show_url = f'{base_api_url}/package_show'
        package_create_url = f'{base_api_url}/package_create'
        api_token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiItdTl5NEJmNklNWHl1RHQ5M1Z5dFdaOURWS19LMGoxSmQ3aGJmUlUyT0pRIiwiaWF0IjoxNzIzMTYwNTAwfQ.pvJ0AV29WKHSaOCjW1uF3Ga-4sucHhS6sov0p2lOTzA'
        owner_org = publisher

        content = json.loads(harvest_object.content)
        file_path = content['file_path']
        contract_name = content['contract_name']
        guid = harvest_object.guid
        tender_id, url_hash = guid.split('-', 1)

        response = get_with_retries(package_show_url, params={'id': tender_id.lower()}, headers={'Authorization': api_token})
        if response.status_code == 404:
            logger.debug('Dataset %s does not exist. Creating new dataset.' % tender_id)
            try:
                create_dataset(package_create_url, api_token, tender_id.lower(), owner_org, contract_name)
            except Exception as e:
                logger.error('Failed to create package %s: %s' % (tender_id, str(e)))
                return False
        elif response.status_code != 200:
            logger.error('Failed to check if package exists %s: %s' % (tender_id, response.text))
            return False

        filename = os.path.basename(file_path)
        logger.debug('Uploading file %s to package %s' % (file_path, tender_id))
        if not upload_file_with_retries(resource_create_url, api_token, file_path, tender_id.lower(), filename):
            return False

        return True

    except Exception as e:
        logger.error('Could not import dataset for object %s: %s' % (harvest_object.id, str(e)))
        return False


def create_dataset(api_url, api_token, package_id, owner_org, contract_name):
    try:
        response = requests.post(
            api_url,
            headers={'Authorization': api_token},
            json={
                'name': package_id,
                'title': contract_name,
                'owner_org': owner_org
            },
            timeout=30
        )
        if response.status_code != 200:
            raise Exception('Failed to create package %s: %s' % (package_id, response.text))
        logger.debug('Created package %s successfully.' % package_id)
    except requests.exceptions.RequestException as e:
        logger.error('Request failed: %s' % e)
        raise
    except Exception as e:
        logger.error('Error creating package %s: %s' % (package_id, str(e)))
        logs_dir = '/storage/public/importlogs'
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)
        with open(os.path.join(logs_dir, 'logs.txt'), 'a') as log_file:
            log_file.write(f'[{datetime.now(timezone.utc)}] Error creating package %s: %s' % (package_id, str(e)))
        raise


def upload_file_with_retries(resource_create_url, api_token, file_path, package_id, filename, retries=3, delay=5):
    for attempt in range(retries):
        try:
            with open(file_path, 'rb') as f:
                response = requests.post(
                    resource_create_url,
                    headers={'Authorization': api_token},
                    files={'upload': (filename, f)},
                    data={'package_id': package_id, 'name': filename},
                    timeout=30
                )
            if response.status_code != 200:
                logger.error('Failed to upload file %s to package %s: %s' % (file_path, package_id, response.text))
                continue
            logger.debug('Uploaded file %s to package %s successfully.' % (file_path, package_id))
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f'Request failed on attempt {attempt + 1}/{retries}: {e}')
            time.sleep(delay)
        except Exception as e:
            logger.error(f'Error uploading file {file_path} to package {package_id} on attempt {attempt + 1}/{retries}: {str(e)}')
            time.sleep(delay)
    logger.error(f'{[{datetime.now(timezone.utc)}]} All retry attempts to upload file %s to package %s failed.' % (file_path, package_id))
    logs_dir = '/storage/public/importlogs'
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    with open(os.path.join(logs_dir, 'logs.txt'), 'a') as log_file:
        log_file.write('All retry attempts to upload file %s to package %s failed.' % (file_path, package_id))
    return False


def get_with_retries(url, params, headers, retries=3, delay=5):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f'Request failed on attempt {attempt + 1}/{retries}: {e}')
            time.sleep(delay)
    logs_dir = '/storage/public/importlogs'
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    with open(os.path.join(logs_dir, 'logs.txt'), 'a') as log_file:
        log_file.write(
        f'[{datetime.now(timezone.utc)}] All retry attempts to fetch data from URL: {url} with params: {params} failed. '
        f'Additional details: Headers: {headers}\n'
    )
    raise Exception('All retry attempts failed')
