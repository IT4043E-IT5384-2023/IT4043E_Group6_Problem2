import json
import uuid
import sys
import os
import time
import requests
import warnings
import pandas as pd
from datetime import datetime
from multiprocessing import Pool
warnings.filterwarnings("ignore")

def get_latest_date(folder):
    json_files = [f for f in os.listdir(folder) if f.endswith('.json')]
    file_dates = [datetime.strptime(file_name.split(".")[0][11:], "%Y-%m-%d").date() for file_name in json_files]
    latest_file_date = max(file_dates) if file_dates else None
    return latest_file_date

def get_the_endtime(sample):
    timestamp = sample['end_time']
    # print(type(timestamp))
    dt_object = datetime.utcfromtimestamp(int(timestamp))
    date_part = dt_object.date()
    return date_part

def check_valid_quest(item):
    if item['community_info']['introduction'] == "":
        return False
    elif get_latest_date("QuestN/data/") is not None and get_latest_date("QuestN/data/") > get_the_endtime(item):
        return False
    else:
        return True

def task_crawl_quests_per_page(params):
    page_number, count_per_page, num_pages, category_code, status_code, community_code, reward_type_code, chain_code = params
    url = f"https://api.questn.com/consumer/explore/list/?count={count_per_page}&page={page_number}&search=&category={category_code}&status_filter={status_code}&community_filter={community_code}&rewards_filter={reward_type_code}&chain_filter={chain_code}&user_id=836558801530990796"
    print(f"Crawl {count_per_page} quests at page {page_number}/{num_pages}")

    count = 1
    while count <= 3:
        try:
            r = requests.get(url)
            time.sleep(5*count)
            break
        
        except:
            time.sleep(5*count)
            count += 1
    if count > 3:
        return []
    
    return r.json()["result"]['data']

def get_all_quests(
        status = "all",
        reward_type: str = 'all',
        chain: str = 'all',
        category: str = 'trending',
        community: str = 'all',
        pool: Pool = Pool(1)
    ):
    categories = {
          "trending": 100,  # Treding quests, provided by QuestN
          "newest": 200,    # Newest quests posted to QuestN
          "top": 300        # Top interest quests, provided by QuestN
      }
    
    communities = {
          "all": 0,           # Query quests from all communities
          "verified": 100,    # Verified QuestN communities
          "followed": 200     # Communities followed by the user
      }
    
    status_codes = {
          "all": 0,           # All quests
          "available": 100,   # Available quests
          "missed": 400       # Missed quests
      }
    
    reward_codes = {
          "all": 0,           # All quests
          "nft": 100,         # Quests with NFt rewards
          "token": 200,       # Quests with token rewards
          "whitelist": 400    # Quests with whitelist rewards
      }
    
    chain_codes = {
          "all": 0,           # All chains
          "eth": 1,           # Ethereum
          "bnb": 56,          # Binance Smart Chain
          "polygon": 137,     # Polygon
          "arbitrum": 42161,  # Arbitrum
          "optimism": 10,     # Optimism
          "zk": 324,          # ZK
          "avalanche": 43114  # Avalanche
      }
    
    count_per_page = 200
    url = f"https://api.questn.com/consumer/explore/list/?count={count_per_page}&page=1&search=&category={categories[category]}&status_filter={status_codes[status]}&community_filter={communities[community]}&rewards_filter={reward_codes[reward_type]}&chain_filter={chain_codes[chain]}&user_id=836558801530990796"
    r = requests.get(url)
    print(r.status_code)
    num_pages = r.json()["result"]["num_pages"]

    data = pool.map(task_crawl_quests_per_page, 
                    [(p, count_per_page, num_pages, category, status, community, reward_type, chain) for p in list(range(1, num_pages+1))])
    pool.close()
    pool.join()
    
    data = [item for sublist in data for item in sublist if (check_valid_quest(item))]
    
    return data


def task_crawl_users_per_quests(params):
    global DATE
    quest_item, users_per_quest_root_path = params
    if  "event_token_summary" not in quest_item['id']:
        name = f"{users_per_quest_root_path}/" + quest_item["id"] + ".csv"
        title = quest_item["title"]
        id = quest_item["id"]
        run = True
        page = 0
        df = pd.DataFrame()
        
        if not os.path.exists(name):
            while run:
                page += 1
                quest_query_url = "https://api.questn.com/consumer/quest/user_participants/?quest_id={}&page={}&count=24".format(id, page)
                
                sleep_count = 1
                while sleep_count < 4:
                    try:
                        r = requests.get(quest_query_url, headers={
                                      "Access-Token": "6bde4819f1c61ed0ee81b7134d9577e224a0cf6b6f5677bacc2fecb2c1fdf396"})
                        break
                    except:
                        time.sleep(5 * sleep_count)
                        
                    sleep_count += 1
                
                if len(r.json()['result']['data']) == 1:
                    run = False
                else:
                    df = pd.concat([df, pd.DataFrame(r.json()['result']['data'])], ignore_index=True)
                
                if page > r.json()["result"]['num_pages']:
                    run = False
                    df = df.drop_duplicates().reset_index(drop = True)
                    
            df.to_csv(name)
            time.sleep(1)
                        
def get_all_users_per_quest(
        all_quests:list,
        pool: Pool = Pool(1),
        users_per_quest_root_path: str=''
    ):
    
    pool.map(task_crawl_users_per_quests, [(q, users_per_quest_root_path) for q in all_quests])
    pool.close()
    pool.join()
    

def crawl():
    DATE = datetime.now().strftime("%Y-%m-%d") # date object
    rerun = False
    json_filepath = f"QuestN/data/all_quests-{DATE}.json"
    # print(json_filepath)
    users_per_quest_root_path = f"QuestN/data/users/{DATE}"
    if not os.path.exists(users_per_quest_root_path):
        os.makedirs(users_per_quest_root_path)
    
    if rerun is True or not os.path.exists(json_filepath):
        all_quests = get_all_quests(pool = Pool(os.cpu_count()))
        # print(len(all_quests))
        with open(json_filepath, 'w') as f:
            json.dump(all_quests, f)
    else:
        print("Loading file")
        with open(json_filepath, 'r') as f:
            all_quests = json.load(f)

    get_all_users_per_quest(all_quests=all_quests, pool=Pool(
        os.cpu_count()), users_per_quest_root_path=users_per_quest_root_path)
if __name__ == "__main__":
    crawl()
    # DATE = datetime.now().strftime("%Y-%m-%d") # date object
    # rerun = False
    # json_filepath = f"QuestN/data/all_quests-{DATE}.json"
    
    # users_per_quest_root_path = f"QuestN/data/users/{DATE}"
    
    # if not os.path.exists(users_per_quest_root_path):
    #     os.makedirs(users_per_quest_root_path)
        
    # # Get all quests available
    # print(json_filepath)
    # if rerun is True or not os.path.exists(json_filepath):
    #     all_quests = get_all_quests(pool = Pool(os.cpu_count()))
    #     print(len(all_quests))
        
    #     with open(json_filepath, 'w') as f:
    #         json.dump(all_quests, f)
    # else:
    #     print("Load file")
    #     with open(json_filepath, 'r') as f:
    #         all_quests = json.load(f)
            
    # get_all_users_per_quest(all_quests=all_quests[0:50], pool=Pool(
    #     os.cpu_count()), users_per_quest_root_path=users_per_quest_root_path)