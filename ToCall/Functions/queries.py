import psycopg2
import pandas as pd
import json


def get_query(dwh_credentials, query):
    # Get DB connection
    con = psycopg2.connect(dbname=dwh_credentials['dbname'],
                           host=dwh_credentials['host'],
                           port=dwh_credentials['port'],
                           user=dwh_credentials['user'],
                           password=dwh_credentials['password'])

    # Get a cursor from DB connection
    cur = con.cursor()

    # Get query to df
    df = pd.read_sql_query(query, con)

    # Close cursor and connection
    cur.close()
    con.close()

    return df













