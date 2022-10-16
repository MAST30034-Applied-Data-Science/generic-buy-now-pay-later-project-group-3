# Generic Buy Now, Pay Later Project

### Introduction

---

A generic Buy Now, Pay Later (BNPL) firm has begun offering a new “Pay in 5 Installments” feature. Merchants (also known as retailers) are looking to boost their customer base by forming a partnership with this firm and in return, the BNPL firm gets a small percentage of revenue (take rate) to cover operating costs. Since this is a great Win-Win opportunity, there are X number of merchants who wish to partner up! However, the BNPL firm can only onboard at most 100 < X number of merchants every year due to limited resources. 

With this as the modivation/task, we have been able to formulate a Ranking Model, to determine score a Merchant base on their Finantial capabilities, Customer Base, and Sustainability in the Market. 




### How to use

---

#### Download required files

To ensure all required libraries are installed, run `pip install -r requirements.txt`

Firstly, before running the main.py in the scripts section, please ensure that all libraries in the 'requirements.txt' file are installed 

To utilise the Model, all that is required is for main.py to be run (found in the scripts folder), which will activate a ETL script, putting in place all steps in Ranking each Merchant. 

Furthermore, the Notebook 'Summary.ipynb' (found in notebooks), has been formulated to collate the interesting findings derived along the way of achieving the overall goal. Please have a look to better understand the infuences of the Ranking Process.




### Key objective files

---

The objective of the project, to rank the top 100 merchants, and find the top 10 ranked merchants by sector, has been saved as paquet files under the names Top_100_Merchants and Top_10_by_Segment respectively, which are found in '../data/curated/'. These can be acessed after sucessfully running the main.py script, and include a list of 'high quality' merchants to target in introducing the BNPL scheme to, where the lower Merchant_Score indicated a better merchant. The full Merchant Rankings can be found in the same folder under the name 'Merchant Rankings'.
