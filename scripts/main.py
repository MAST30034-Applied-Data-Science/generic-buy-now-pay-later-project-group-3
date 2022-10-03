import utils

from download import Download
from clean import Clean
from process import Process

def BNPL_ranker():
    """
    Main funtion that calls and runs all scripts. Please view terminal for output on progress.
    """
    # Download
    _download()

    # c = Clean()
    # p = Process()
    pass

def _download():
    """
    Function to deal with all download based code
    """
    d = Download()
    d.download_external()
    del d

# BNPL_ranker()