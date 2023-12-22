# Crawlers - Group 6
A repository to demonstrate how to use crawlers for crypto-users. 

# Setup

Kindly utilize either pyenv or Anaconda to set up the Python version specified in the .python-version file.

Install all the libraries inside requirements.txt file to establish the environment.
```
pip install -r requirements.txt
```

# Crawling steps
1. Retrieve complete user information, including addresses and Twitter accounts, through the QuestN platform:
```
python3 QuestN/crawl.py
```
2. Preprocess Twitter username from QuestN/data/users folder
```
python3 Twitter/preprocess.py
```
3. Scrape Twitter data, keeping in mind that data retrieval is subject to a sleep timeframe.
```
python3 Twitter/run.py
```

# Data dictionary

Please refer to the metadata description [here](https://docs.google.com/spreadsheets/d/1SCYqC_8tjeUHj7Cz52ZUMEbfyNyNPIPwRATcJNJ8S0A/edit?usp=sharing)