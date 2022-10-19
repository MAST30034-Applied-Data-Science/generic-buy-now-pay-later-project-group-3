import utils as u
import sys

from download import Download
from clean import Clean
from process import Process
from pre_model import PreModel
from model import Model

ALL = 0
DOWNLOAD = 1
CLEAN = 2
PROCESS = 3
MODEL = 4

def BNPL_ranker():
    """
    Main funtion that calls and runs all scripts. Please view terminal for output on progress.
    """
    flag = u.read_command_line()
    
    if flag == DOWNLOAD:
        _download()
        return
    
    elif flag == CLEAN:
        _clean()
        return

    elif flag == PROCESS:
        _process()
        return

    elif flag == MODEL:
        _model()
        return
    
    elif flag == ALL:
        _download()
        _clean()
        _process()
        _model()


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

def _model():
    """
    Function to call all model based code
    """
    pre = PreModel()
    pre.run_model()
    del pre

    m = Model()
    m.run_model()
    del m

BNPL_ranker()