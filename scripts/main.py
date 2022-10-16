import utils as u
import sys

from download import Download
from clean import Clean
from process import Process

ALL = 0
DOWNLOAD = 1
CLEAN = 2
PROCESS = 3

def BNPL_ranker():
    """
    Main funtion that calls and runs all scripts. Please view terminal for output on progress.
    """
    flag = u.read_command_line()
    
    if flag == DOWNLOAD:
        _download()
        print("Downloading task finished!")
        return
    
    elif flag == CLEAN:
        _clean()
        print("Cleaning task finished!")
        return

    elif flag == PROCESS:
        _process()
        print("Processing task finished!")
        return
    
    elif flag == ALL:
        _download()
        print("Downloading task finished!")
        _clean()
        print("Cleaning task finished!")
        _process()
        print("Processing task finished!")


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

def _process():
    """
    Function to call all processed based code
    """
    p = Process()
    p.transform()
    del p

BNPL_ranker()