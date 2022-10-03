import os
from zipfile import ZipFile
from urllib.request import urlretrieve

class Download():
    """
    Call all download functions and delete variables when done
    """

    def __init__(self) -> None:
        print("Download started")
        
    def __del__(self):
        print("Download Completed!")

    def download_external(self):
        """
        Function to download all the external datasets
        """
        self.download_tax()
    
    def download_tax(self):
        """
        Function to download tax/income data from ABS SA2 website
        """
        pass

