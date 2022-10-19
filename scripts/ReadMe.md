# Scripts

The scripts are divided as seen in the folder.

### `clean.py`

The clean py script contains functions that are required to clean external and internal datasets

### `download.py`

Download py script downloads all external datsets such as

- Tax/Income aggregate data for every postcode
- Post code verified list data

### `process.py`

The process py script takes the cleaned data to process and transform such that it is ready for the model

### `utils.py`

The utils py script contains several helper functions that are accessed by all files

### `pre_model.py`

The pre model script contains several functions that help ready the day for the model

### `model.py`

The model script runs the Random Forest algorithm against all transactions
