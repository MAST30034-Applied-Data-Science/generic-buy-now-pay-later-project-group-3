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
        self.download_description_avg()
        self.download_sector_average()
        self.download_sector_information()
        self.download_description_lookup()
        self.download_merchants_tbl_processed()
        self.download_entropy()
        self.download_holidays()
        self.download_shape_files()
    
    def download_tax(self):
        """
        Function to download tax/income data from ABS SA2 website
        """
        source = "https://drive.google.com/uc?export=download&id=1uVWoiIqU2gam_JKr13-wr-7gbJ7E9bGF"
    
        file_location = u.safety_check("tables") + "/tax_income.csv"
        urlretrieve(source, file_location)

        print("Download Tax/Income data complete")

    def download_postcodes(self):
        """
        Function to download a list of postcodes of Aus
        """
        source = "https://drive.google.com/uc?export=download&id=1GmWZlGW0l6DEc3lW09t5O6rbBDAZ5e--"
    
        file_location = u.safety_check("tables") + "/australian_postcodes.csv"
        urlretrieve(source, file_location)

        print("Download Australian postcodes data complete")

    def download_description_avg(self):
        """
        Function to download user defined file of description averages
        """
        source = "https://drive.google.com/uc?export=download&id=1xBNnZBAXd8nQDIRjK1VCp2_WVwN4xYTB"
    
        file_location = u.safety_check("tables") + "/description_average.parquet"
        urlretrieve(source, file_location)

        print("Download Description Average complete")

    def download_sector_average(self):
        """
        Function to download sector averages
        """
        source = "https://drive.google.com/uc?export=download&id=1c5qS6SaDlrYnaNLFMc-ZyCZ2jck5SOZS"
    
        file_location = u.safety_check("tables") + "/sector_average.parquet"
        urlretrieve(source, file_location)

        print("Download Sector Average complete")

    def download_sector_information(self):
        """
        Function to download sector based information
        """
        source = "https://drive.google.com/uc?export=download&id=1-Lug_07B5J4F_8sEYFfFMzJGHuzSyXKg"
    
        file_location = u.safety_check("tables") + "/sector_information.parquet"
        urlretrieve(source, file_location)

        print("Download Sector Information complete")
        

    def download_description_lookup(self):
        """
        Function to lookup table for descriptions
        """
        source = "https://drive.google.com/uc?export=download&id=1y6Zp7p20W1O2uCgqL36MVh1WlXTxjaZI"

        file_location = u.safety_check("tables") + "/description_lookup.csv"
        urlretrieve(source, file_location)

        print("Download Lookup table for descriptions complete")

    def download_merchants_tbl_processed(self):
        """
        Function to download a processed version of the merchant table
        """
        source = "https://drive.google.com/uc?export=download&id=16nj7YdMzserc-e4XcH0TCoV85FI3Ub2Z"

        file_location = u.safety_check("tables") + "/merchants_tbl_processed.csv"
        urlretrieve(source, file_location)

        print("Download Processed Merchant table complete")

    def download_entropy(self):
        """
        Function to download entropy values
        """
        source = "https://drive.google.com/uc?export=download&id=1MPo06FczbrkH-yORTvLZE3tQl6D8t4Ry"

        file_location = u.safety_check("tables") + "/entropy.parquet"
        urlretrieve(source, file_location)

        print("Download Processed Entropy complete")


    def download_holidays(self):
        """
        Function to download holiday data
        """
        source = "https://drive.google.com/uc?export=download&id=1znhkqMRiyoyKGU37SD7CKshetdc5DfYt"

        file_location = u.safety_check("tables") + "/holiday.parquet"
        urlretrieve(source, file_location)

        print("Download holidays complete")


    def download_shape_files(self):
        """
        Function to download shape files
        """
        source = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SA2_2021_AUST_SHP_GDA2020.zip"

        file_location = u.safety_check("tables") + "geo_data.zip"
        urlretrieve(source, file_location)

        with ZipFile(file_location, 'r') as f:
            f.extractall(u.safety_check("tables/geo_data"))
            os.remove(file_location)

        print("Download shape files complete")
