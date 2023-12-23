from crawlQuestN import crawl
from concateUser import user_data
from classifyProject import classifyProject
from utils.load_config import load_config
from utils.gcs_push import push
from utils.postgresql import append_df_to_db
from utils.basic_args import obtain_args
import pandas as pd
from datetime import datetime

def run(args):
    cfg = load_config(args.config)
    try:
        crawl()
    except:
        pass
    classifyProject()
    user_data()
    push()
    append_df_to_db(
        db_name = cfg["db_name"],
        df = pd.read_csv(cfg["result_paths"].format(datetime.now().strftime("%Y-%m-%d"))),
        table = cfg['table']
    )
    # db_name='users_clusters', df=pd.read_csv("/home/ubuntu/system/HUST/results.csv"), table="questn_results"
if __name__ == "__main__":
    args = obtain_args()
    run(args)