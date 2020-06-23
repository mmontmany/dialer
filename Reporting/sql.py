import pandas as pd
import mysql.connector as connector
import time
import datetime
import sqlalchemy
import psycopg2

# set up mysql
user = 'USERNAME'
password = 'PASS'
host = 'SG-pacodb-1980-master.servers.mongodirector.com'
port = '3306'
database = 'paco_db'

# set up postgres
POSTGRES_ADDRESS = 'glovo-dwh-prod.cmsc5llor91g.eu-west-1.redshift.amazonaws.com'
POSTGRES_PORT = '5439'
POSTGRES_USERNAME = 'USERNAME'
POSTGRES_PASSWORD = 'PASS'
POSTGRES_DBNAME = 'glovodwh'
postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=POSTGRES_USERNAME,
                                                                                        password=POSTGRES_PASSWORD,
                                                                                        ipaddress=POSTGRES_ADDRESS,
                                                                                        port=POSTGRES_PORT,
                                                                                        dbname=POSTGRES_DBNAME))


def get_mysql(query):
    try:
        cnx = connector.connect(user=user, password=password,
                                host=host, port=port,
                                database=database)
        df = pd.read_sql(query, cnx)
        df = df.fillna("")
        cnx.close()
        return df
    except Exception:
        cnx.close()
        time.sleep(1)
        try:
            cnx = connector.connect(user=user, password=password,
                                    host=host, port=port,
                                    database=database)
            df = pd.read_sql(query, cnx)
            cnx.close()
            return df
        except Exception:
            cnx.close()
            return pd.DataFrame()


def get_postgres(query):
    try:
        cnx = cnx = sqlalchemy.create_engine(postgres_str)
        df = pd.read_sql(query, cnx)
        df = df.fillna("")
        return df
    except Exception:
        time.sleep(1)
        try:
            cnx = sqlalchemy.create_engine(postgres_str)
            df = pd.read_sql(query, cnx)
            return df
        except Exception:
            return pd.DataFrame()


def get_calls(segment, days=14):
    try:
        query = "SELECT call_id, right(campaign_name,length(campaign_name)-7) segment, courier_id, call_time FROM cloudtalk_calls WHERE courier_id is not null AND agent_action is not null AND agent_action not like UPPER('REJECTED') AND days_to_convert is null And campaign_name like '%" + segment + "%' AND call_time >SUBDATE(current_date, INTERVAL " + str(
            days) + " DAY);"

        cnx = connector.connect(user=user, password=password,
                                host=host, port=port,
                                database=database)
        df = pd.read_sql(query, cnx)
        df = df.fillna("")
        cnx.close()
        return df
    except Exception:
        cnx.close()
        time.sleep(1)
        try:
            query = "SELECT call_id, right(campaign_name,length(campaign_name)-7) segment, courier_id, call_time FROM cloudtalk_calls WHERE courier_id is not null AND agent_action is not null AND agent_action not like UPPER('REJECTED') AND days_to_convert is null And campaign_name like '%" + segment + "%' AND call_time >SUBDATE(current_date, INTERVAL " + str(
                days) + " DAY);"
            cnx = connector.connect(user=user, password=password,
                                    host=host, port=port,
                                    database=database)
            df = pd.read_sql(query, cnx)
            df = df.fillna("")
            cnx.close()
            return df
        except Exception:
            cnx.close()
            return pd.DataFrame()


def get_conversions_queries():
    try:
        query = "SELECT campaign_name, data_base, query, time_frame FROM conversion_queries_brain;"
        cnx = connector.connect(user=user, password=password,
                                host=host, port=port,
                                database=database)
        df = pd.read_sql(query, cnx)
        df = df.fillna("")
        cnx.close()
        return df
    except Exception:
        cnx.close()
        time.sleep(1)
        try:
            query = "SELECT campaign_name, data_base, query, time_frame FROM conversion_queries_brain;"
            cnx = connector.connect(user=user, password=password,
                                    host=host, port=port,
                                    database=database)
            df = pd.read_sql(query, cnx)
            df = df.fillna("")
            cnx.close()
            return df
        except Exception:
            cnx.close()
            return pd.DataFrame()


def post_conversion(call_id, converted, days_to_convert, table_name='cloudtalk_calls'):
    try:
        cnx = connector.connect(user=user, password=password, host=host, port=port, database=database)
        cursor = cnx.cursor()
        sql_add = 'UPDATE ' + table_name + ' SET  converted= ' + str(converted) + ', days_to_convert = "' + str(
            days_to_convert) + '" WHERE call_id = "' + str(call_id) + '";'
        cursor.execute(sql_add)
        cnx.commit()
        cursor.close()
        cnx.close()
        return True
    except Exception:
        cursor.close()
        cnx.close()
