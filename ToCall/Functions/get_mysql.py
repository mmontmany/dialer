import pandas as pd
import mysql.connector as connector
import time

# set up connection
user = 'pdb-flaviodib'
password = 'pacoaLwkoepdw9831!'
host = 'SG-pacodb-1980-master.servers.mongodirector.com'
port = '3306'
database = 'paco_db'

def get_query(query):
    try:
        cnx = connector.connect(user=user, password=password,
                                      host=host, port=port,
                                      database=database)
        df = pd.read_sql(query,cnx)
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

def get_agents_by_hub(hub):
    try:
        query = "SELECT agent_id, email, 'Glovo Agent' as first_name, agent_id as last_name, capacity, cloudtalk_agent_id FROM agents_brain WHERE STRCMP(UPPER(hub),UPPER('"+hub+"'))=0 AND enabled = TRUE;"
        cnx = connector.connect(user=user, password=password,
                                      host=host, port=port,
                                      database=database)
        df = pd.read_sql(query,cnx)
        cnx.close()
        return df
    except Exception:
        cnx.close()
        time.sleep(1)
        try:
            query = "SELECT agent_id, email, 'Glovo Agent' as first_name, agent_id as last_name, capacity, cloudtalk_agent_id FROM agents_brain WHERE STRCMP(UPPER(hub),UPPER('" + hub + "'))=0 AND enabled = TRUE;"
            cnx = connector.connect(user=user, password=password,
                                    host=host, port=port,
                                    database=database)
            df = pd.read_sql(query, cnx)
            cnx.close()
            return df
        except Exception:
            cnx.close()
            return pd.DataFrame()

def get_groups_by_hub(hub):
    try:
        query = "SELECT DISTINCT LEFT(dialer_campaigns.campaign_name,10) AS group_name,dialer_campaigns.country_code,dialer_campaigns.city_code, dialer_campaigns.hub, sum(dialer_campaigns.agents_real) as agents_real, ct_campaign_id FROM dialer_campaigns LEFT JOIN campaigns_brain ON campaigns_brain.campaign_name=dialer_campaigns.campaign_name WHERE day=CURRENT_DATE AND STRCMP(UPPER(dialer_campaigns.hub),UPPER('"+hub+"'))=0 GROUP BY 1,2,3,4,6 ORDER BY agents_real DESC;"
        cnx = connector.connect(user=user, password=password,
                                      host=host, port=port,
                                      database=database)
        df = pd.read_sql(query,cnx)
        cnx.close()
        return df
    except Exception:
        cnx.close()
        time.sleep(1)
        try:
            query = "SELECT DISTINCT LEFT(dialer_campaigns.campaign_name,10) AS group_name,dialer_campaigns.country_code,dialer_campaigns.city_code, dialer_campaigns.hub, sum(dialer_campaigns.agents_real) as agents_real, ct_campaign_id FROM dialer_campaigns LEFT JOIN campaigns_brain ON campaigns_brain.campaign_name=dialer_campaigns.campaign_name WHERE day=CURRENT_DATE AND STRCMP(UPPER(dialer_campaigns.hub),UPPER('" + hub + "'))=0 GROUP BY 1,2,3,4,6 ORDER BY agents_real DESC;"
            cnx = connector.connect(user=user, password=password,
                                    host=host, port=port,
                                    database=database)
            df = pd.read_sql(query, cnx)
            cnx.close()
            return df
        except Exception:
            cnx.close()
            return pd.DataFrame()

def get_campaigns_by_hub(hub):
    try:
        query = "SELECT distinct dialer_campaigns.campaign_name,dialer_campaigns.country_code, dialer_campaigns.city_code, segment_name as segment, dialer_campaigns.agents_real, dialer_campaigns.language, dialer_campaigns.hub, ct_campaign_id, ct_tag_id, ct_script_id, survey_id, button_0, button_1, button_2,button_3,button_4 FROM dialer_campaigns LEFT JOIN campaigns_brain on campaigns_brain.campaign_name=dialer_campaigns.campaign_name WHERE day=current_date AND STRCMP(UPPER(dialer_campaigns.hub),UPPER('" + hub + "'))=0;"
        cnx = connector.connect(user=user, password=password,
                                      host=host, port=port,
                                      database=database)
        df = pd.read_sql(query,cnx)
        cnx.close()
        return df
    except Exception:
        cnx.close()
        time.sleep(1)
        try:
            query = "SELECT distinct dialer_campaigns.campaign_name,dialer_campaigns.country_code, dialer_campaigns.city_code, segment_name as segment, dialer_campaigns.agents_real, dialer_campaigns.language, dialer_campaigns.hub, ct_campaign_id, ct_tag_id, ct_script_id, survey_id, button_0, button_1, button_2,button_3,button_4 FROM dialer_campaigns LEFT JOIN campaigns_brain on campaigns_brain.campaign_name=dialer_campaigns.campaign_name WHERE day=current_date AND STRCMP(UPPER(dialer_campaigns.hub),UPPER('" + hub + "'))=0;"
            cnx = connector.connect(user=user, password=password,
                                    host=host, port=port,
                                    database=database)
            df = pd.read_sql(query, cnx)
            cnx.close()
            return df
        except Exception:
            cnx.close()
            return pd.DataFrame()

def campaigns2mysql(df, table_name = 'campaigns_brain'):
    try:
        cnx = connector.connect(user=user, password=password,host=host, port=port,database=database)
        cursor = cnx.cursor()
        for(rows,rs) in df.iterrows():
            sql_add = 'INSERT INTO ' + table_name + ' VALUES ("' + str(rs[0]) + '", "' + str(rs[1]) + '", "' + str( rs[2]) + '", "' + str(rs[3]) + '", "' + str(rs[4]) + '", "' + str(rs[5]) + '", "' + str( rs[6]) + '", "' + str(rs[7]) + '", "' + str(rs[8]) + '", "' + str(rs[9]) + '", "' + str(rs[10]) + '", "' + str( rs[11]) + '", "' + str(rs[12]) + '", "' + str(rs[13]) + '", "' + str(rs[14]) + '", "' + str( rs[15]) + '") ON DUPLICATE KEY UPDATE ' + df.columns.values[0] + ' = "' + str(rs[0]) + '", ' + df.columns.values[1] + '= "' + str(rs[1]) + '", ' + df.columns.values[3] + '= "' + str(rs[3]) + '", ' + df.columns.values[4] + '= "' + str(rs[4]) + '", ' + df.columns.values[5] + '= "' + str(rs[5]) + '", ' + df.columns.values[6] + '= "' + str(rs[6]) + '", ' + df.columns.values[7] + '= "' + str(rs[7]) + '", ' +df.columns.values[8] + '= "' + str(rs[8]) + '", ' + df.columns.values[9] + '= "' + str(rs[9]) + '", ' +df.columns.values[10] + '= "' + str(rs[10])+'", ' + df.columns.values[11] + '= "' + str(rs[11])+'", '  + df.columns.values[12] + '= "' + str(rs[12])+'", '  + df.columns.values[13] + '= "' + str(rs[13]) + '", ' + df.columns.values[14] + '= "' + str(rs[14]) + '", ' + df.columns.values[15] + '= "' + str(rs[15]) + '";'
            cursor.execute(sql_add)
            cnx.commit()
        cursor.close()
        cnx.close()
        return True
    except Exception:
        cursor.close()
        cnx.close()
        time.sleep(1)
        try:
                cnx = connector.connect(user=user, password=password, host=host, port=port, database=database)
                cursor = cnx.cursor()
                for (rows, rs) in df.iterrows():
                    sql_add = 'INSERT INTO ' + table_name + ' VALUES ("' + str(rs[0]) + '", "' + str(rs[1]) + '", "' + str( rs[2]) + '", "' + str(rs[3]) + '", "' + str(rs[4]) + '", "' + str(rs[5]) + '", "' + str( rs[6]) + '", "' + str(rs[7]) + '", "' + str(rs[8]) + '", "' + str(rs[9]) + '", "' + str(rs[10]) + '", "' + str( rs[11]) + '", "' + str(rs[12]) + '", "' + str(rs[13]) + '", "' + str(rs[14]) + '", "' + str( rs[15]) + '") ON DUPLICATE KEY UPDATE ' + df.columns.values[0] + ' = "' + str(rs[0]) + '", ' + df.columns.values[1] + '= "' + str(rs[1]) + '", ' + df.columns.values[3] + '= "' + str(rs[3]) + '", ' + df.columns.values[4] + '= "' + str(rs[4]) + '", ' + df.columns.values[5] + '= "' + str(rs[5]) + '", ' + df.columns.values[6] + '= "' + str(rs[6]) + '", ' + df.columns.values[7] + '= "' + str(rs[7]) + '", ' +df.columns.values[8] + '= "' + str(rs[8]) + '", ' + df.columns.values[9] + '= "' + str(rs[9]) + '", ' +df.columns.values[10] + '= "' + str(rs[10])+'", ' + df.columns.values[11] + '= "' + str(rs[11])+'", '  + df.columns.values[12] + '= "' + str(rs[12])+'", '  + df.columns.values[13] + '= "' + str(rs[13]) + '", ' + df.columns.values[14] + '= "' + str(rs[14]) + '", ' + df.columns.values[15] + '= "' + str(rs[15]) + '";'
                    cursor.execute(sql_add)
                    cnx.commit()
                cursor.close()
                cnx.close()
                return True
        except Exception:
            cursor.close()
            cnx.close()
            return False

def agents2mysql(df,table_name='agents_brain'):
    try:
        cnx = connector.connect(user=user, password=password,host=host, port=port,database=database)
        cursor = cnx.cursor()
        for(rows,rs) in df.iterrows():
            sql_add= 'INSERT INTO '+table_name+' (agent_id, email, cloudtalk_agent_id) VALUES ("'+str(rs[0])+'", "'+rs[1]+'", "'+str(rs[-1])+'") ON DUPLICATE KEY UPDATE '+df.columns.values[0]+' = "'+str(rs[0])+'", '+df.columns.values[1]+'= "'+rs[1]+'", '+df.columns.values[-1]+'= "'+str(rs[-1])+'";'
            cursor.execute(sql_add)
            cnx.commit()
        cursor.close()
        cnx.close()
        return True
    except Exception:
        cursor.close()
        cnx.close()
        time.sleep(1)
        try:
            cnx = connector.connect(user=user, password=password, host=host, port=port, database=database)
            cursor = cnx.cursor()
            for (rows, rs) in df.iterrows():
                sql_add = 'INSERT INTO ' + table_name + ' (agent_id, email, cloudtalk_agent_id) VALUES ("' + str(
                    rs[0]) + '", "' + rs[1] + '", "' + str(rs[-1]) + '") ON DUPLICATE KEY UPDATE ' + df.columns.values[
                              0] + ' = "' + str(rs[0]) + '", ' + df.columns.values[1] + '= "' + rs[1] + '", ' + \
                          df.columns.values[-1] + '= "' + str(rs[-1]) + '";'
                cursor.execute(sql_add)
                cnx.commit()
            cursor.close()
            cnx.close()
            return True
        except Exception:
            cursor.close()
            cnx.close()
            return False

def get_scripts_by_hub(hub):
    try:
        query = "SELECT distinct CONCAT(hub,'_',vertical,'_',segment_name) as script_name, script_body, script_id, survey_id FROM dialer_campaigns LEFT JOIN scripts_brain on script_name=CONCAT(hub,'_',vertical,'_',segment_name)  WHERE day=CURRENT_DATE AND STRCMP(UPPER(dialer_campaigns.hub), UPPER('" + hub + "'))=0;"
        cnx = connector.connect(user=user, password=password, host=host, port=port, database=database)
        df = pd.read_sql(query, cnx)
        cnx.close()
        return df
    except Exception:
        cnx.close()

def get_campaign_2_scripts(hub):
    try:
        query = "SELECT distinct campaign_name, CONCAT(hub,'_',vertical,'_',segment_name) as script_name FROM dialer_campaigns WHERE day=CURRENT_DATE AND STRCMP(UPPER(dialer_campaigns.hub), UPPER('" + hub + "'))=0;"
        cnx = connector.connect(user=user, password=password, host=host, port=port, database=database)
        df = pd.read_sql(query, cnx)
        cnx.close()
        return df
    except Exception:
        cnx.close()

def scripts2mysql(df, table_name='scripts_brain'):
    try:
        cnx = connector.connect(user=user, password=password, host=host, port=port, database=database)
        cursor = cnx.cursor()
        for (rows, rs) in df.iterrows():
            sql_add = 'INSERT INTO ' + table_name + ' (script_name, script_body, script_id, survey_id) VALUES ("' + str(
                rs[0]) + '", "' + str(rs[1]) + '", "' + str(rs[2]) + '", "' + str(
                rs[3]) + '") ON DUPLICATE KEY UPDATE ' + df.columns.values[0] + ' = "' + str(rs[0]) + '", ' + \
                      df.columns.values[1] + '= "' + str(rs[1]) + '", ' + df.columns.values[2] + '= "' + str(
                rs[2]) + '", ' + df.columns.values[3] + '= "' + str(rs[3]) + '";'
            cursor.execute(sql_add)
            cnx.commit()
        cursor.close()
        cnx.close()
        return True
    except Exception:
        cursor.close()
        cnx.close()
        time.sleep(1)
        try:
            cnx = connector.connect(user=user, password=password, host=host, port=port, database=database)
            cursor = cnx.cursor()
            for (rows, rs) in df.iterrows():
                sql_add = 'INSERT INTO ' + table_name + ' (script_name, script_body, script_id, survey_id) VALUES ("' + str(
                    rs[0]) + '", "' + str(rs[1]) + '", "' + str(rs[2]) + '", "' + str(
                    rs[3]) + '") ON DUPLICATE KEY UPDATE ' + df.columns.values[0] + ' = "' + str(rs[0]) + '", ' + \
                          df.columns.values[1] + '= "' + str(rs[1]) + '", ' + df.columns.values[2] + '= "' + str(
                    rs[2]) + '", ' + df.columns.values[3] + '= "' + str(rs[3]) + '";'
                cursor.execute(sql_add)
                cnx.commit()
            cursor.close()
            cnx.close()
            return True
        except Exception:
            cursor.close()
            cnx.close()
            return False

def get_campaigns_with_assigned_agents_by_hub(hub):
    try:
        query = "SELECT distinct dialer_campaigns.campaign_name,dialer_campaigns.country_code, dialer_campaigns.city_code, segment_name as segment, dialer_campaigns.agents_list, dialer_campaigns.language, dialer_campaigns.hub, ct_campaign_id, ct_tag_id, ct_script_id, survey_id, button_0, button_1, button_2,button_3,button_4 FROM dialer_campaigns LEFT JOIN campaigns_brain on campaigns_brain.campaign_name=dialer_campaigns.campaign_name WHERE day=current_date AND STRCMP(UPPER(dialer_campaigns.hub),UPPER('" + hub + "'))=0;"
        cnx = connector.connect(user=user, password=password,
                                      host=host, port=port,
                                      database=database)
        df = pd.read_sql(query,cnx)
        cnx.close()
        return df
    except Exception:
        cnx.close()
        time.sleep(1)
        try:
            query = "SELECT distinct dialer_campaigns.campaign_name,dialer_campaigns.country_code, dialer_campaigns.city_code, segment_name as segment, dialer_campaigns.agents_list, dialer_campaigns.language, dialer_campaigns.hub, ct_campaign_id, ct_tag_id, ct_script_id, survey_id, button_0, button_1, button_2,button_3,button_4 FROM dialer_campaigns LEFT JOIN campaigns_brain on campaigns_brain.campaign_name=dialer_campaigns.campaign_name WHERE day=current_date AND STRCMP(UPPER(dialer_campaigns.hub),UPPER('" + hub + "'))=0;"
            cnx = connector.connect(user=user, password=password,
                                    host=host, port=port,
                                    database=database)
            df = pd.read_sql(query, cnx)
            cnx.close()
            return df
        except Exception:
            cnx.close()
            return pd.DataFrame()
