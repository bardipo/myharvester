import logging
import time
from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException


class MyharvesterPlugin(SingletonPlugin):
    """
    A Test Harvester
    """
  
    logging.basicConfig(level=logging.INFO)

    implements(IHarvester)

    def info(self):
        """
        Harvesting implementations must provide this method, which will return
        a dictionary containing different descriptors of the harvester. The
        returned dictionary should contain:

        * name: machine-readable name. This will be the value stored in the
          database, and the one used by ckanext-harvest to call the appropiate
          harvester.
        * title: human-readable name. This will appear in the form's select box
          in the WUI.
        * description: a small description of what the harvester does. This
          will appear on the form as a guidance to the user.

        A complete example may be::

            {
                'name': 'csw',
                'title': 'CSW Server',
                'description': 'A server that implements OGC's Catalog Service
                                for the Web (CSW) standard'
            }

        :returns: A dictionary with the harvester descriptors
        """
        return {
            'name': 'myharvester',
            'title': 'My Harvester',
            'description': 'A test harvester for CKAN'
        }

    def validate_config(self, config):
        """

        [optional]

        Harvesters can provide this method to validate the configuration
        entered in the form. It should return a single string, which will be
        stored in the database.  Exceptions raised will be shown in the form's
        error messages.

        :param harvest_object_id: Config string coming from the form
        :returns: A string with the validated configuration options
        """

    def get_original_url(self, harvest_object_id):
        """

        [optional]

        This optional but very recommended method allows harvesters to return
        the URL to the original remote document, given a Harvest Object id.
        Note that getting the harvest object you have access to its guid as
        well as the object source, which has the URL.
        This URL will be used on error reports to help publishers link to the
        original document that has the errors. If this method is not provided
        or no URL is returned, only a link to the local copy of the remote
        document will be shown.

        Examples:
            * For a CKAN record: http://{ckan-instance}/api/rest/{guid}
            * For a WAF record: http://{waf-root}/{file-name}
            * For a CSW record: http://{csw-server}/?Request=GetElementById&Id={guid}&...

        :param harvest_object_id: HarvestObject id
        :returns: A string with the URL to the original document
        """

    def gather_stage(self, harvest_job):

      download_dir = "/storage"
        # Set up Chrome options
      chrome_options = Options()
      chrome_options.add_argument("--headless")
      chrome_options.add_argument("--window-size=1920,1080")
      chrome_options.add_argument("--no-sandbox")
      chrome_options.add_argument("--disable-gpu")
      chrome_options.add_argument('start-maximized')
      chrome_options.add_argument('disable-infobars')
      chrome_options.add_argument("--disable-extensions")
      chrome_options.add_argument('--disable-dev-shm-usage')
      driver = webdriver.Chrome(options=chrome_options)
      chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,  # İndirmeleri /storage dizinine yap
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
      })

      try:
          logging.info("Now im trying to reach link")
          driver.get("https://vergabe.autobahn.de/NetServer/TenderingProcedureDetails?function=_Details&TenderOID=54321-NetTender-19101f44104-7ac7217fb59bc4dd&thContext=publications")
          wait = WebDriverWait(driver, 10)
          logging.info("I got link")
          download_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.btn-modal.zipFileContents")))
          download_button.click()
          modal = wait.until(EC.visibility_of_element_located((By.ID, 'detailModal')))
          select_all_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Alles auswählen']")))
          select_all_button.click()
          confirm_download_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Auswahl herunterladen']")))
          confirm_download_button.click()
          logging.info("I clicked link")
          time.sleep(20)
          logging.info("I waited 20 sec now im done")
          return []
      except TimeoutException:
          logging.info("Nothing to download")  
          return []
      finally:
          driver.quit()

    def fetch_stage(self, harvest_object):
        """
        The fetch stage will receive a HarvestObject object and will be
        responsible for:
            - getting the contents of the remote object (e.g. for a CSW server,
              perform a GetRecordById request).
            - saving the content in the provided HarvestObject.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything is ok (ie the object should now be
              imported), "unchanged" if the object didn't need harvesting after
              all (ie no error, but don't continue to import stage) or False if
              there were errors.

        :param harvest_object: HarvestObject object
        :returns: True if successful, 'unchanged' if nothing to import after
                  all, False if not successful
        """

        return False

    def import_stage(self, harvest_object):
        """
        The import stage will receive a HarvestObject object and will be
        responsible for:
            - performing any necessary action with the fetched object (e.g.
              create, update or delete a CKAN package).
              Note: if this stage creates or updates a package, a reference
              to the package should be added to the HarvestObject.
            - setting the HarvestObject.package (if there is one)
            - setting the HarvestObject.current for this harvest:
              - True if successfully created/updated
              - False if successfully deleted
            - setting HarvestObject.current to False for previous harvest
              objects of this harvest source if the action was successful.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - creating the HarvestObject - Package relation (if necessary)
            - returning True if the action was done, "unchanged" if the object
              didn't need harvesting after all or False if there were errors.

        NB You can run this stage repeatedly using 'paster harvest import'.

        :param harvest_object: HarvestObject object
        :returns: True if the action was done, "unchanged" if the object didn't
                  need harvesting after all or False if there were errors.
        """

        return False
