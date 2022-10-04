import os
from zipfile import ZipFile
from urllib.request import urlretrieve

import utils as u

class Download():
    """
    Call all download functions and delete variables when done
    """

    def __init__(self):
        print("Download started")
        
    def __del__(self):
        print("Download Completed!")

    def download_external(self):
        """
        Function to download all the external datasets
        """
        self.download_tax()
        self.download_postcodes()
        # self.industry_data()
    
    def download_tax(self):
        """
        Function to download tax/income data from ABS SA2 website
        """
        source = "https://drive.google.com/uc?export=download&id=10VZ0auZjhD_eXboBDBo-8LRlxRi_RpU5"
    
        file_location = u.safety_check() + "/tax_income.csv"
        urlretrieve(source, file_location)

        print("Download Tax/Income data complete")

    def download_postcodes(self):
        """
        Function to download a list of postcodes of Aus
        """
        source = "https://drive.google.com/uc?export=download&id=1ihZ1aHSu3mqIHzii5UD2VEZNldIUkonn"
    
        file_location = u.safety_check() + "/postcode_verification.csv"
        urlretrieve(source, file_location)

        print("Download Post code verification data complete")

    def industry_data(self):
        """
        Function to download industry specific related data
        """
        pass
