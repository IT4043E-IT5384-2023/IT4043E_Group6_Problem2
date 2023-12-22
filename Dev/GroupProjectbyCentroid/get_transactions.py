from pymongo import MongoClient
import pandas as pd

def get_transaction_data() -> pd.DataFrame:
    try:
        transactions = pd.read_csv("transactions.csv")
        print("abc")
    except:
        client = MongoClient("mongodb://etlReaderAnalysis:etl_reader_analysis__Gr2rEVBXyPWzIrP@34.126.84.83:27017,34.142.204.61:27017,34.142.219.60:27017")

        transactions = []
        for collection in client.list_database_names():
            result = client[collection]['transactions'].find({})
            for i, document in enumerate(result):
                transactions.append(document)
                if i >= 140000:
                    break
                
        transactions = pd.DataFrame(transactions)
    
    return transactions