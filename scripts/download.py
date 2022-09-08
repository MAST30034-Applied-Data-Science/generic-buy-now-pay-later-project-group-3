from urllib.request import urlretrieve
from zipfile import ZipFile
import os

def download_all():
    """
    Function to call all the download functions required for the program to run
    """
    pass

def download_sa2():
    """
    Function to download SA2 boundary maps
    """

    url = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SA2_2021_AUST_SHP_GDA2020.zip"

    # Safety check
    output_dir = "../data/tables/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_location = output_dir + "sa2.zip"

    if not os.path.exists(file_location[:-4]):
        urlretrieve(url, file_location)

        # Unzip
        with ZipFile(file_location, 'r') as f:
            f.extractall(output_dir)
            os.remove(file_location)

def download_sa2_meta():
    """
    Function to download SA2 meta data and other relevant statistics
    """

download_sa2()
    