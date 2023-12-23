import yaml

# Connection parameters, modify as needed
USER_CLUSTERS_DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432',
}

BLOCKCHAIN_DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'student_token_transfer',
    'password': 'svbk_2023',
    'host': '34.126.75.56',
    'port': '5432'
}

QUESTN_RESULTS_TABLE_NAME = 'questn_results'
TABLE_SCHEMAS = {
    "questn_results": ['user_address','user_name','twitter_username','twitter_name','Art','Cexes','Derivatives','Dexes','Gaming','Indexes','Infrastructure','Lending','Memberships','PFPs','Services','Stablecoins','Virtual Worlds','Yield']
}

def get_db_config(db_name):
    if db_name == 'users_clusters':
        return USER_CLUSTERS_DB_CONFIG
    elif db_name == 'blockchain':
        return BLOCKCHAIN_DB_CONFIG
    else:
        raise Exception(f"Database {db_name} is not defined")

def load_config(config_path):
    with open(config_path) as file:
        config = yaml.safe_load(file)
    return config