import os
import glob
os.chdir("/mnt/d/University/Applied Datascience/generic-buy-now-pay-later-project-group-3/scripts/")

for file in glob.glob(os.path.join('../data/tables/', 'transactions_*')):
    print(file)