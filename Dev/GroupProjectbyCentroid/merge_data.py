import pandas as pd
import numpy as np

def merge_data(transactions_df, project_smc_df):
    transaction_project_df = pd.merge(transactions_df,
                                        project_smc_df,
                                        left_on='to_address', 
                                        right_on='address', 
                                        how='inner')

    address_categories = transaction_project_df.groupby(["mapped_categories", "from_address"]).count().reset_index()[['mapped_categories', 'from_address', '_id_x']]
    address_categories.columns = ["categories", "user_address", "count"]


    for cate in address_categories.categories.unique():
        address_categories[f'{cate}'] = len(address_categories[address_categories.categories == cate])
    
    address_categories['Indexes'] = np.random.randint(1, 10, len(address_categories))
    address_categories['Memberships'] = np.random.randint(10, 20,len(address_categories))
    address_categories['PFPs'] = np.random.randint(20, 30,len(address_categories))
    address_categories['Dexes'] = np.random.randint(30, 40,len(address_categories))
    address_categories['Yield'] = np.random.randint(40, 50,len(address_categories))

    address_categories.drop("count", inplace = True, axis=1)
    address_categories.to_csv("address_categories.csv", index=False)