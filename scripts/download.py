import os
from zipfile import ZipFile
from urllib.request import urlretrieve

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
<<<<<<< HEAD
=======
        # self.download_postcodes()
>>>>>>> download
    
    def download_tax(self):
        """
        Function to download tax/income data from ABS SA2 website
        """
        source = "https://drive.google.com/uc?export=download&id=10VZ0auZjhD_eXboBDBo-8LRlxRi_RpU5"
    
        file_location = self.safety_check() + "/tax_income.csv"
        urlretrieve(source, file_location)

        print("Download Tax/Income data complete")

    def download_postcodes(self):
        """
        Function to download a list of postcodes of Aus
        """
        pass

    def safety_check(self, dir_name = None):
        """
        Function to perform checks of directory folders
        """
        # Safety check
        output_dir = "../data/tables/"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        if not (dir_name is None):
            # Create a directory for source group
            target_dir = output_dir + dir_name
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)

            return target_dir
        return output_dir
