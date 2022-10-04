import os
from functools import reduce

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, DataFrame

def read_tables(sp: SparkSession, file: str, ftype = "p"):
    """
    Helper function to read data from the desginated folder

    sp : Current SparkSession
    file : Type of data/Name of file name to be read
    ftype : File type (Parquet(p) or CSV(c))

    returns DataFrame read
    """
    # Root directory
    dir = "../data/tables/"
    
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
            final_list.append(sp.read.parquet(dir + g))

        return reduce(DataFrame.unionAll, final_list)

    # Special file
    elif file == "tbl_consumer":
        return sp.read.option("header", True).option("delimiter", "|").csv("../data/tables/tbl_consumer.csv")

    # Parquet files
    if ftype == "p":
        return sp.read.parquet(dir + file + ".parquet")
    elif ftype == "c":
        return sp.read.option("header", True).csv(dir + file + ".csv")

def write_data(data: DataFrame, folder: str, fname: str):
    """
    Function to write spark data into the specified folder
    """
    dir = "../data/" + folder + "/" + fname
    data.write.parquet(dir, mode="overwrite")

    print("Wrote " + fname + " to memory")


##########################


def createTaregetDir(curated_dir, relative_dir):
    if  not os.path.exits(curated_dir + relative_dir):
        os.makedirs(curated_dir + relative_dir)
    return curated_dir + relative_dir

def show_intermediate(spark,target_dir = None, target_file = None):
    if target_dir == None and target_file == None:
        raise ValueError("No intemediate results to show")
    if target_dir != None:
        print("show dir")

