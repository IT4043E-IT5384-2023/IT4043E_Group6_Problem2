import json
import uuid
import sys
import os
import time
import requests
import warnings
import pandas as pd
from multiprocessing import Pool
warnings.filterwarnings("ignore")


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
  return r.json()["result"]["data"]


def get_all_quests(
  status: str = 'all',
  reward_type: str = 'all',
  chain: str = 'all',
  category: str = 'trending',
  community: str = 'all',
  pool: Pool = Pool(1)
):
  categories = {
      "trending": 100,
      "newest": 200,
      "top": 300
  }
  communities = {
      "all": 0,
      "verified": 100,
      "followed": 200
  }
  status_codes = {
      "all": 0,
      "available": 100,
      "missed": 400
  }
  reward_codes = {
      "all": 0,
      "nft": 100,
      "token": 200,
      "whitelist": 400
  }
  chain_codes = {
      "all": 0,
      "eth": 1,
      "bnb": 56,
      "polygon": 137,
      "arbitrum": 42161,
      "optimism": 10,
      "zk": 324,
      "avalanche": 43114
  }
  count_per_page = 200
  
  url = f"https://api.questn.com/consumer/explore/list/?count={count_per_page}&page=1&search=&category={categories[category]}&status_filter={status_codes[status]}&community_filter={communities[community]}&rewards_filter={reward_codes[reward_type]}&chain_filter={chain_codes[chain]}&user_id=836558801530990796"
  r = requests.get(url)
  num_pages = r.json()["result"]["num_pages"]
  
  data = pool.map(task_crawl_quests_per_page, [
                  (p, count_per_page, num_pages, category, status, community, reward_type, chain) for p in list(range(1, num_pages+1))])
  pool.close()
  pool.join()
  data = [item for sublist in data for item in sublist]

  return data


def task_crawl_users_per_quest(params):
    quest_item, users_per_quest_root_path = params
    # Remove all events from the data, take quests only
    if "event_token_summary" not in quest_item["id"]:
      name = f"{users_per_quest_root_path}/" + quest_item["id"] + ".csv"
      title = quest_item["title"]
      id = quest_item["id"]
      run = True
      page = 0
      df = pd.DataFrame()

      print(f"Crawl event {title} with ID = {id}")

      if not os.path.exists(name):
          while run:
              page += 1
              quest_query_url = "https://api.questn.com/consumer/quest/user_participants/?quest_id={}&page={}&count=24".format(
                  id, page)

              # Sleep if request limit is exceeded
              sleep_count = 1
              while sleep_count < 4:
                  try:
                      r = requests.get(quest_query_url, headers={
                                      "Access-Token": "6bde4819f1c61ed0ee81b7134d9577e224a0cf6b6f5677bacc2fecb2c1fdf396"})
                      break
                  except:
                      time.sleep(5*sleep_count)
                  sleep_count += 1
              if len(r.json()['result']['data']) == 0:
                  run = False
              else:
                  df = pd.concat([df, pd.DataFrame(r.json()['result']['data'])], ignore_index=True)
              
              if page > r.json()['result']['num_pages']:
                  run = False
                  df = df.drop_duplicates().reset_index(drop=True)

          df.to_csv(name, index=False)
          time.sleep(1)


def get_all_users_per_quest(
  all_quests: list,
  pool: Pool = Pool(1),
  users_per_quest_root_path: str=''
):
    pool.map(task_crawl_users_per_quest, [(q, users_per_quest_root_path) for q in all_quests])
    pool.close()
    pool.join()

if __name__ == "__main__":
    rerun = False
    json_filepath = "QuestN/data/all_quests.json"
    users_per_quest_root_path = "QuestN/data/users"

    if not os.path.exists(users_per_quest_root_path):
       os.makedirs(users_per_quest_root_path)

    if rerun is True or not os.path.exists(json_filepath):
      # Get all quests available
      all_quests = get_all_quests(pool=Pool(os.cpu_count()))
      print(len(all_quests))

      with open(json_filepath, 'w') as f:
        json.dump(all_quests, f)
    else:
      with open(json_filepath, 'r') as f:
        all_quests = json.load(f)

    get_all_users_per_quest(all_quests=all_quests, pool=Pool(
        os.cpu_count()), users_per_quest_root_path=users_per_quest_root_path)
