import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from .load_config import get_db_config, TABLE_SCHEMAS


def query(db_name: str, query_str: str):
    """
    Function to execute a query

    Parameters
    ----------
        db_name: str
            users_clusterss (our DB) or blockchain (DB from Mr Bang)
        query_str: str,
            the content of the query
    
    Returns
    ----------
        records: list,
            query result in list format
    """
    db_config = get_db_config(db_name)
    records = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)

        # Create a cursor object
        cur = conn.cursor()

        # Execute a query
        cur.execute(query_str)

        # Fetch the results
        records = cur.fetchall()

    except psycopg2.DatabaseError as e:
        print(f'Error {e}')
    finally:
        if conn is not None:
            conn.close()

    return records


def query_by_address(db_name: str, address: str, table: str, return_type: str = 'dataframe'):
    """
    Query data based on user's address

    Parameters
    ----------
        db_name: str
            users_clusterss (our DB) or blockchain (DB from Mr Bang)
        address: str,
            user's address
        table: str,
            table name
        return_type: str,
            type of returned value
    
    Returns
    ----------
        Query result in expected format
    """
    res = query(db_name, f"SELECT * FROM {table} WHERE user_address = '{address}'")
    if return_type == 'dataframe':
        if table in TABLE_SCHEMAS.keys():
            res = pd.DataFrame(res, columns=TABLE_SCHEMAS[table])
        else:
            res = pd.DataFrame(res)
    return res


def append_df_to_db(db_name: str, df: pd.DataFrame, table: str):
    """
    Append dataframe to database

    Parameters
    ----------
        db_name: str
            users_clusterss (our DB) or blockchain (DB from Mr Bang)
        df: pd.DataFrame,
            the dataframe to be saved to the database
        table: str,
            the table name
    
    Returns
    ---------
        True if successfully
    """
    db_config = get_db_config(db_name)

    # Create the connection URL
    conn_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

    # Create the engine
    engine = create_engine(conn_url)

    # Append the DataFrame to the database in the most efficient way
    df.to_sql(table, engine, if_exists='append', index=False, method='multi')

    return True

if __name__ == "__main__":
    # append_df_to_db(db_name='users_clusters', df=pd.read_csv("/home/ubuntu/system/HUST/results.csv"), table="questn_results")
    l = query_by_address(db_name='users_clusters', address="0x0F0450aD79A0339f95E920fD157F113C8CFbfD9f", table="questn_results")
    print(l)