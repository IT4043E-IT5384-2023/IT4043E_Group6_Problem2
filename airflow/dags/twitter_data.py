import os
import sys

print(os.path.join(os.path.dirname(__file__), '..', '..', 'HUST'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'HUST'))

# The DAG object; we'll need this to instantiate a DAG
from datetime import timedelta
from airflow import DAG
from airflow.decorators import task

# Operators; we need this to operate!
from airflow.utils.dates import days_ago

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['nguyenhoangtien21102002gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
# [END default_args]

# [START instantiate_dag]
with DAG(
    'twitter_data',
    default_args=default_args,
    description='Process QuestN data',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['twitter'],
) as dag:
    
    from crawlQuestN import crawl
    from concateUser import user_data
    from classifyProject import classifyProject
    from utils.load_config import load_config
    from utils.gcs_push import push
    from utils.postgresql import append_df_to_db
    import pandas as pd
    from datetime import datetime

    cfg = {
        "db_name": "users_clusters" ,
        "result_paths": "results_{}.csv",
        "table": "questn_results"
    }

    @task(task_id = 'run')
    def run():
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
    
    run()