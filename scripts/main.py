'''This is the main session to run all the other preprocess modules, as well as presenting a cmd interface for the 
    client'''
#steps:
# 1, download the external dataset(if any) 
# 2, outliers  
# 3, feature engineering 
# 4, join datasets as the final input to modeling stage

#Questions:
#1, code for downloading packages in requirements.txt?
#2, 

from symbol import continue_stmt
import download as d
import feature  as f
import outlier as o


print("start preparing raw datasets, type \"Y\" to continue.")
continue_indicator = input()
if continue_indicator == "Y":
    print("hello")

#download external dataset
if d.download_external() != True:
    raise ValueError("can't download external dataset")
else:
    print("all datasets ready")





    



    