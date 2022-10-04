import utils

from download import Download
from clean import Clean
from process import Process

def BNPL_ranker():
    """
    Main funtion that calls and runs all scripts. Please view terminal for output on progress.
    """
    # Download
    # _download()

    # c = Clean()
    _clean()

    # p = Process()
    # _process()

def _download():
    """
    Function to call with all download based code
    """
    d = Download()
    d.download_external()
    del d

def _clean():
    """
    Function to call all cleaning based code
    """
    c = Clean()
    c.clean_all()
    del c

BNPL_ranker()