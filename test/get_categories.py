from difflib import SequenceMatcher
import pickle
import os
import pandas as pd
from pymongo import MongoClient

# Accessing the database
client = MongoClient(
    "mongodb://klgReaderAnalysis:klgReaderAnalysis_4Lc4kjBs5yykHHbZ@35.198.222.97:27017,34.124.133.164:27017,34.124.205.24:27017")

smart_constracts_file_path = '/home/hoangvictor/Documents/Big Data/IT4043E_Group6_Problem2/test/project_smc.csv'
interactions_file_path = '/home/hoangvictor/Documents/Big Data/IT4043E_Group6_Problem2/test/interactions.csv'
smc_categories_file_path = '/home/hoangvictor/Documents/Big Data/IT4043E_Group6_Problem2/test/smc_categories.pkl'

db = client.MongoDB

interaction_smcs = pd.read_csv(interactions_file_path)

if not os.path.exists(smart_constracts_file_path):
    # Get data from MongoDB
    projection = {"address": 1, "name": 1, "categories": 1, "chainId": 1, "symbol": 1}
    result = client['knowledge_graph']['smart_contracts'].find({}, projection)

    # Print the results
    smart_constracts = list(result)
    smart_contracts_df = pd.DataFrame(smart_constracts)
    smart_contracts_df.to_csv(smart_constracts_file_path)
else:
    print("Read file")
    smart_contracts_df = pd.read_csv(smart_constracts_file_path)

smart_contracts_df = smart_contracts_df.drop_duplicates(subset=['address']).reset_index(drop=True)

result2 = client['knowledge_graph']['projects'].find({})
# Print the results

# project1 = []
# for collection in client.list_database_names():
#     result = client[collection]['projects'].find({})
#     for document in result:
#         project1.append(document)
#         if len(project1) > 100000:
#             break

project2 = []
for document in result2:
    project2.append(document)
    if len(project2) > 1000000:
        break


def string_similarity(str1, str2):
    try:
        similarity_ratio = SequenceMatcher(None, str1, str2).ratio()
        return similarity_ratio
    except:
        return 0


cnt = 0

smc_categories = {}
tmp_smart_constracts_df = smart_contracts_df.copy()

if os.path.exists(smc_categories_file_path):
    smc_categories = pickle.load(open(smc_categories_file_path, 'rb'))

all_smcs = []
for k in smc_categories.keys():
    all_smcs += smc_categories[k]

tmp_smart_constracts_df = smart_contracts_df[~smart_contracts_df['address'].isin(all_smcs)]
tmp_smart_constracts_df = smart_contracts_df[smart_contracts_df['address'].isin(interaction_smcs['contractAddress'].values)]

for i in range(1):
    print(f"Part {i}")
    tmp_smart_constracts_df = tmp_smart_constracts_df.iloc[tmp_smart_constracts_df.shape[0] //
                                                           1*i:tmp_smart_constracts_df.shape[0]//1*(i+1)]
    for j, project in enumerate(project2):
        if 'category' not in project.keys():
            continue
        category = project['category']
        project = project['name']

        def similar(str):
            return string_similarity(str, project)

        tmp_smart_constracts_df['similarity_ratio'] = tmp_smart_constracts_df['name'].apply(
            similar)
        remove_smart_constracts_df = tmp_smart_constracts_df[
            tmp_smart_constracts_df['similarity_ratio'] >= 0.8]
        smc = set(remove_smart_constracts_df['address'].values)
        if category not in smc_categories.keys():
            smc_categories[category] = smc
        else:
            smc_categories[category] = smc | smc_categories[category]

        tmp_smart_constracts_df = tmp_smart_constracts_df[~tmp_smart_constracts_df['address'].isin(
            smc)]
        print(tmp_smart_constracts_df.shape[0])

        if j % 10 == 0 and j >= 10:
            pickle.dump(smc_categories, open(smc_categories_file_path, 'wb'))
