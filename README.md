# Generic Buy Now, Pay Later Project

### Introduction
---
A generic Buy Now, Pay Later (BNPL) firm has begun offering a new “Pay in 5 Installments” feature. Merchants are looking to boost their customer base by forming a partnership with this firm and in return, the BNPL firm gets a small percentage of revenue (take rate) to cover operating costs. The BNPL firm is looking for the top 100 merchants to onboard.

With this as the motivation, we have been able to formulate a Ranking Model, to score a Merchant based on their Financial capabilities, Customer Base, Fraud risk and Sustainability in the Market.

### How to use
---
#### Pipeline
To ensure all required libraries are installed, run (preferably in Python 3.9)
`pip install -r requirements.txt`

To run the pipeline
`python3 main.py --path "some/path/data" --output "some/output/dir" -option`
- \- -path: input folder path where all the tables (customer, merchant, transactions) are stored
- \- -output: folder where the processed/cleaned tables are to be stored. Should have the same parent directory as '- -path', see example below
- \-option (optional): 
        -d: _download_ files only
        -c: _clean_ files only
        -p: _process_ files only
        -m: _model_ files only
        none: _download, clean, process, and model_

E.g.,
`python3 main.py --path "/home/generic-buy-now-pay-later-project-group-3/data/tables" --output "/home/generic-buy-now-pay-later-project-group-3/data" -p`


The following scripts get executed in order:
```
scripts
├── Main.py 
├── Download.py
├── Clean.py
├── Process.py
├── Pre_model.py
├── Model.py
```

To run the fraud detection and Ranking models, execute the following notebooks in order:
```
notebooks
├── Model_results.ipynb
├── Ranking_System_P1.ipynb
└── Ranking_System_P2_Functions.ipynb
```
Note: 
- Notebooks with the prefix 'DO_NOT_RUN' are just helper notebooks used to understand the data and analyse trends, need not be run.
- In all the above notebooks, user needs to specify the path variable in the top cell. A note is included, just need to set the 'dir' variable to point to the data folder.

#### Visualisations and graphs
The graphs for trends and model evaluations are included in the *Model_results.ipynb* and *Ranking_System_P2_Functions.ipynb* notebooks.

#### Contributors

- Prathyush P Rao (1102225)
- Harshita Soni (1138784)
- Chaitanya Raghuvanshi (117645)
- James Barro (1082092)
- Ruitong Wang (1118966)
