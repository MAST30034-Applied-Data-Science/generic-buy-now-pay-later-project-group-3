from distutils.command.clean import clean
import os
from functools import reduce

import numpy as np
import pandas as pd
import sys
from pyspark.sql import SparkSession, DataFrame

ALL = 0
DOWNLOAD = 1
CLEAN = 2
PROCESS = 3
MODEL = 4

def read_tables(sp: SparkSession, file: str, ftype = "p", sample=False):
    """
    Helper function to read data from the desginated folder

    sp : Current SparkSession
    file : Type of data/Name of file name to be read
            (if file is "transactions" no ftype needed)
    ftype : File type (Parquet(p) or CSV(c))

    returns DataFrame
    """
    # Root directory
    #dir = "../data/tables/"

    if(sys.argv[1] != "--path"):
        print >> sys.stderr, "Incorrect format."
        sys.exit(1)
    dir = sys.argv[2] + "/"     #folder path should be at position 2.

    # Transaction folders
    if file == "transactions":
        # Read all transactions together
        groups = [
            "transactions_20210228_20210827_snapshot/",
            "transactions_20210828_20220227_snapshot/",
            "transactions_20220228_20220828_snapshot/"
            ]

        # Read the different transaction folders
        final_list = []
        for g in groups:
            final_list.append(sp.read.option("inferSchema", True).parquet(dir + g))

        if not sample:
            return reduce(DataFrame.unionAll, final_list)
        return reduce(DataFrame.unionAll, final_list).sample(0.01)

    # Special file
    elif file == "tbl_consumer":
        return sp.read.option("inferSchema", True).option("header", True).option("delimiter", "|").csv(dir + file + ".csv")

    # Parquet files
    if ftype == "p":
        return sp.read.option("inferSchema", True).parquet(dir + file + ".parquet")
    elif ftype == "c":
        return sp.read.option("inferSchema", True).option("header", True).csv(dir + file + ".csv")

def read_curated(sp:SparkSession, fname: str):
    """
    Function to read data from the curated folder

    sp : Current sparkSession
    fname : Name of file to be read
    """
    # Root directory
    dir = sys.argv[4] + "/curated/" + fname
    return sp.read.option("inferSchema", True).parquet(dir)

def read_processed(sp: SparkSession, fname: str):
    """
    Function to read data from the processed folder

    sp : Current sparkSession
    fname : Name of file to be read
    """
    # Root directory
    dir = sys.argv[4] + "/processed/" + fname
    return sp.read.option("inferSchema", True).parquet(dir)

def read_models(sp: SparkSession, fname: str):
    """
    Function to read data from the models folder

    sp : Current sparkSession
    fname : Name of file to be read
    """
    # Root directory
    dir = sys.argv[4] + "/models/" + fname
    return sp.read.option("inferSchema", True).parquet(dir)

def write_data(data: DataFrame, folder: str, fname: str):
    """
    Function to write spark data into the output folder specified by command line input
    """
    dir = safety_check(folder) + "/" + fname
    data.write.parquet(dir, mode="overwrite")

    print("Wrote " + fname + " to memory")

def safety_check(parent_dir: str, dir_name = None):
    """
    Function to perform checks of directory folders
    """
    # Safety check
    if (sys.argv[3] != "--output"):
        print >> sys.stderr, "Incorrect format."
        sys.exit(1)
    output_dir = sys.argv[4] + "/" + parent_dir
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if not (dir_name is None):
        # Create a directory for source group
        target_dir = output_dir + dir_name
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        return target_dir
    return output_dir


def read_command_line():
    """
    Function to read and parse the flag from command line that specifies which scripts to run.
    """
    if len(sys.argv) == 5:
        return ALL
    if sys.argv[5] == "-d":
        return DOWNLOAD
    elif sys.argv[5] == "-c":
        return CLEAN
    elif sys.argv[5] == "-p":
        return PROCESS
    elif sys.argv[5] == "-m":
        return MODEL
    else:
        print >> sys.stderr, "Incorrect flag. Please select one of the options provided in the README.md file"
        sys.exit(1)


##########################
