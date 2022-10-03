'''This module contains the functions to download the external data.'''
from utils import createTaregetDir 
def download_external(raw_dir):
    '''This function downloads all the external datasets(if there is any)'''

    download_process = True
    if (download_process):
        print("external dataset downloading finished")
        return True
    else:
        return False
    

