import os
from bs4 import BeautifulSoup
import requests
from urllib.parse import unquote
from .helpers import *
from .databaseConnection import get_tender_ids_evergabe
import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def fetch_download_urls_evergabe(tender_id,tender_download_path):
        logger.info("Scrapping the website for " + tender_id)
        url = f'https://www.evergabe.de/unterlagen/{tender_id}'
        response = requests.get(url)
        file_paths = []
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', {'data-turbo': 'false', 'class': 'btn btn-primary'})
            if links == []:
                logger.info("Nothing to download for" + tender_id)
                return False
            final_links = []
            for link in links:
                final_links.append(f"https://www.evergabe.de/{link['href']}")
            try:
                for link in final_links:
                    response = requests.get(link)
                    response.raise_for_status()
                    cd = response.headers.get('Content-Disposition')
                    if cd:
                        filename = cd.split('filename=')[-1].strip('"').split('"')[0]
                    else:
                        filename = unquote(url.split('/')[-1])

                    file_path = os.path.join(tender_download_path, filename)

                    with open(file_path, 'wb') as f:
                        f.write(response.content)

                    if file_path.endswith('.zip'):
                        password = extract_password_from_filename(filename)
                        unzip_file(file_path, tender_download_path, password)
                files = os.listdir(tender_download_path)
                for file in files:
                     file_paths.append(os.path.join(tender_download_path,file))
                logger.info("Files downloaded for" + tender_id)
                return True
            except requests.HTTPError as e:
                logger.error('HTTP error fetching %s: %s' % (url, str(e)))
                return False
            except requests.RequestException as e:
                logger.error('Error fetching %s: %s' % (url, str(e)))
                return False
        else:
            logger.error(f"Failed to retrieve the webpage. Status code: {response.status_code}")
            return False


def gather_stage_evergabe(harvest_job,justImpport = False):
        tenders = get_tender_ids_evergabe()
        if justImpport:
             return process_multiple_tenders_without_download(tenders,harvest_job,"evergabe")
        else:
             return process_multiple_tenders_giving_publisher(tenders,harvest_job,fetch_download_urls_evergabe,"evergabe")
