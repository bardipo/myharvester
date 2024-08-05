from .helpers import *
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from .databaseConnection import get_tender_ids_vergabe_nrw

    
def download_tender_files_vergabe_nrw(tender_id, download_dir):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument('start-maximized')
        chrome_options.add_argument('disable-infobars')
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
        })
        driver = webdriver.Chrome(options=chrome_options)
    
        file_path = None
        try:
            url = f"https://www.evergabe.nrw.de/VMPSatellite/public/company/project/{tender_id}/de/documents"
            driver.get(url)
            wait = WebDriverWait(driver,10)
            download_button = driver.find_element(By.XPATH, "//a[contains(@title, 'Alle Dokumente als ZIP-Datei herunterladen')]")
            download_button.click()
            wait_until_download_finishes(download_dir)
            file_path = give_latest_file(download_dir)
            unzip_file(file_path,download_dir) 
            return True
        except NoSuchElementException:
              print("Nothing to download")
              return False
        finally:
            driver.quit()
    
def gather_stage_vergabe_nrw(harvest_job):
        tender_data = {
    'tender_id': 'CXPNYHNDUU0',
    'title': 'Redaktion und Online-Publikation der BASS und des Amtsblatts MSB',
    'url': 'https://www.evergabe.nrw.de/VMPSatellite/notice/CXPNYHNDUU0/documents',
    'document': {
        '_id': '66a99d2f89b3af54f8ec7afe',
        'ocid': 'ocds-mnwr74-8352229d-bece-4a17-bd94-c630bac0ad12',
        '__v': 0,
        'extensions': [
            'https://raw.githubusercontent.com/open-contracting-extensions/ocds_lots_extension/bd6a5a0617abcdcf9da19db0e70a899e6b10c91c/extension.json',
            'https://raw.githubusercontent.com/open-contracting-extensions/ocds_location_extension/v1.1.5/extension.json',
            'https://raw.githubusercontent.com/open-contracting-extensions/ocds_enquiry_extension/v1.1.5/extension.json',
            'https://raw.githubusercontent.com/open-contracting-extensions/ocds_coveredBy_extension/9b617f9c3a84f8eb727f273e1bb55a1c5bc7a925/extension.json'
        ],
        'license': 'https://opendefinition.org/licenses/cc-zero/',
        'publicationPolicy': 'http://bkmk/ui/de/publicationPolicy',
        'publishedDate': '2024-07-28T22:00:00.000Z',
        'publisher': {'name': 'Bekanntmachungsservice'},
        'releases': [
            {
                'ocid': 'ocds-mnwr74-8352229d-bece-4a17-bd94-c630bac0ad12',
                'id': 'f6d2c659-0e90-4ab2-8757-e2cb90c28ac5',
                'date': '2024-07-28T22:00:00Z',
                'tag': ['tender'],
                'initiationType': 'tender',
                'parties': [
                    {
                        'name': 'Ministerium für Schule und Bildung NRW',
                        'id': 'ORG-0001',
                        'identifier': {'id': '05111-05001-82', 'legalName': 'Ministerium für Schule und Bildung NRW'},
                        'address': {
                            'streetAddress': 'Völklinger Str. 49',
                            'locality': 'Düsseldorf',
                            'region': 'DEA11',
                            'postalCode': '40221',
                            'countryName': 'DEU'
                        },
                        'contactPoint': {'email': 'fp-referat126@msb.nrw.de', 'telephone': '+49 21158673252'},
                        'roles': ['procuringEntity']
                    },
                    {
                        'name': 'Vergabekammer Rheinland',
                        'id': 'ORG-0002',
                        'identifier': {'id': '05315-03002-81', 'legalName': 'Vergabekammer Rheinland'},
                        'address': {
                            'streetAddress': 'Zeughausstraße 2-10',
                            'locality': 'Köln',
                            'postalCode': '50667',
                            'countryName': 'DEU'
                        },
                        'contactPoint': {
                            'email': 'VKRheinland@bezreg-koeln.nrw.de',
                            'telephone': '+49 2211473055',
                            'faxNumber': '+49 2211472889'
                        },
                        'roles': ['reviewBody']
                    }
                ],
                'buyer': {
                    'name': 'Ministerium für Schule und Bildung NRW',
                    'id': '05111-05001-82',
                    'identifier': {'id': 'ORG-0001', 'legalName': 'Ministerium für Schule und Bildung NRW'},
                    'address': {
                        'streetAddress': 'Völklinger Str. 49',
                        'locality': 'Düsseldorf',
                        'region': 'DEA11',
                        'postalCode': '40221',
                        'countryName': 'DEU'
                    },
                    'contactPoint': {'email': 'fp-referat126@msb.nrw.de', 'telephone': '+49 21158673252'}
                },
                'tender': {
                    'id': '8352229d-bece-4a17-bd94-c630bac0ad12',
                    'title': 'Redaktion und Online-Publikation der BASS und des Amtsblatts MSB',
                    'description': 'Redaktions-und Online-Publikationsdienstleistungen für die Herstellung und Bereitstellung der BASS und des Amtsblatts MSB.',
                    'procuringEntity': {'name': 'Ministerium für Schule und Bildung NRW', 'id': '05111-05001-82'},
                    'items': [
                        {
                            'id': 'LOT-0001',
                            'classification': {'scheme': 'CPV', 'id': '72320000', 'description': 'Database services'},
                            'relatedLot': 'LOT-0001',
                            'deliveryAddress': {'region': 'DEA11', 'countryName': 'DEU'}
                        }
                    ],
                    'procurementMethodDetails': 'Negotiated with prior publication of a call for competition / competitive with negotiation',
                    'numberOfTenderers': 0,
                    'documents': [
                        {
                            'id': 'DOC-0001',
                            'url': 'https://www.evergabe.nrw.de/VMPSatellite/notice/CXPNYHNDUU0/documents',
                            'language': 'DEU',
                            'relatedLots': ['LOT-0001']
                        }
                    ],
                    'lots': [
                        {
                            'id': 'LOT-0001',
                            'title': 'Redaktion und Online-Publikation der BASS und des Amtsblatts MSB',
                            'description': 'Gegenstand dieser Ausschreibung sind die in diesem Zusammenhang durchzuführenden Redaktions- und Publikationsdienstleistungen für die Herstellung und Bereitstellung der BASS und des Amtsblatts MSB im Internet sowie auf mobilen Endgeräten für einen Leistungszeitraum frühestens ab 1. Januar 2025 bis zum 31. Dezember 2027, optional bis längstens 31. Dezember 2029.',
                            'contractPeriod': {'startDate': '2025-01-01T00:00:00+01:00', 'endDate': '2027-12-31T00:00:00+01:00'}
                        }
                    ]
                },
                'language': 'DEU'
            }
        ],
        'uri': 'http://bkmk/api/notices/f6d2c659-0e90-4ab2-8757-e2cb90c28ac5?format=ocds&noticeVersion=01',
        'version': '1.1'
    }
}
        tender_ids = [tender_data]
        return process_multiple_tenders_giving_publisher(tender_ids,harvest_job,download_tender_files_vergabe_nrw,"vergabe_nrw")



