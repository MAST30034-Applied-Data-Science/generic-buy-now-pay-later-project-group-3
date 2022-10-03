'''This module contains all the utilty functions'''
import os


def createTaregetDir(curated_dir, relative_dir):
    if  not os.path.exits(curated_dir + relative_dir):
        os.makedirs(curated_dir + relative_dir)
    return curated_dir + relative_dir

def show_intermediate(spark,target_dir = None, target_file = None):
    if target_dir == None and target_file == None:
        raise ValueError("No intemediate results to show")
    if target_dir != None:
        print("show dir")

