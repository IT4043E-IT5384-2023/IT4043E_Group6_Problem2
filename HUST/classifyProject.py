import json
import pandas as pd
import numpy
from datetime import datetime
from infer import infer
from tqdm import tqdm
from utils import load_config
import warnings
warnings.filterwarnings("ignore")

def get_project_infor(sample):
    project_id = sample['id']
    description = sample['community_info']['introduction']
    return project_id, description

def create_dataframe_project(data, cfg):
    df = pd.DataFrame(columns = ["id", "description", "category"])
    for sample in tqdm(data):
        project_id, description = get_project_infor(sample)
        # print(description)
        # try:
        category = infer(description, cfg)[0]
            # print(category)
        df.loc[len(df.index)] = [project_id, description, category]
        # except:
            # continue
    return df

def classifyProject():
    cfg = load_config("config/model.yaml")
    quests_path = "QuestN/data/all_quests-{}.json".format(datetime.utcnow().date())
    with open(quests_path, 'r') as f:
        data = json.load(f)
    df = create_dataframe_project(data, cfg)
    df.to_csv(cfg["save_file"].format(datetime.now().strftime("%Y-%m-%d")), index = False)
    
if __name__ == "__main__":
    classifyProject()

