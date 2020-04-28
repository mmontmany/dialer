from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from gspread.exceptions import APIError
import pandas as pd
from datetime import datetime
from datetime import timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
import time
import json
import requests
from pandas.io.json import json_normalize

global id_dashboard
global id_data
global credentials
global api_id
global api_token

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('cs_aircall_dashboard.json', scope)
id_data = '1_UDM2ZAMjg3zV9P1xbrd6NtAnuRvDiMa8jyWh6IrRdw'
api_id = 'eca55fc89ed749a0c924a806322a3b88'
api_token = '722c5995b242ce16e949d59b7cb32545'

scheduler = BlockingScheduler()


def get_df_spreadsheet(id, sheet_name):

    try:
        client = gspread.authorize(credentials)
        sheet = client.open_by_key(id)
        data = sheet.worksheet(sheet_name).get_all_values()

        return pd.DataFrame(data, columns=data.pop(0))

    except APIError:
        return pd.DataFrame()


def post_to_sheets(id, sheet_name, df_post):

    try:
        client = gspread.authorize(credentials)
        sheet = client.open_by_key(id)
        worksheet = sheet.worksheet(sheet_name)
        worksheet.clear()

        set_with_dataframe(worksheet, df_post)

        return True

    except APIError:
        return False


def get_agent_id_aircall(email):
    time.sleep(1)
    api_url = "https://" + api_id + ":" + api_token + "@api.aircall.io/v1/users"
    response_users_json = requests.get(api_url)

    response_users = json.loads(response_users_json.text)
    df_users = json_normalize(response_users['users'])
    return str(df_users[df_users['email'] == email]['id'].iloc(0)[0])


def add_dialer_campaign(agent_id_f, phone_numbers):
    time.sleep(1)
    request_dialer_campaign = {"phone_numbers": phone_numbers}
    api_post_dialer_campaign = "https://" + api_id + ":" + api_token + "@api.aircall.io/v1/users/" + agent_id_f + "/dialer_campaign"

    return requests.post(api_post_dialer_campaign, json=request_dialer_campaign)


def remove_dialer_campaign(agent_id_f):
    time.sleep(1)
    # Get user dialer campaign
    api_get_dialer_campaign = "https://" + api_id + ":" + api_token + "@api.aircall.io/v1/users/" + agent_id_f + "/dialer_campaign"
    response_get_dialer_campaign = requests.get(api_get_dialer_campaign)

    if response_get_dialer_campaign.text != '':
        # Delete dialer campaign
        api_delete_dialer_campaign = "https://" + api_id + ":" + api_token + "@api.aircall.io/v1/users/" + agent_id_f + "/dialer_campaign"
        response_delete_dialer_campaign = requests.delete(api_delete_dialer_campaign)




#@scheduler.scheduled_job('interval', minutes=0.15)
#def post_data():
if 1 == 1:

    # Read To Call (Aircall Data)
    df_to_call = get_df_spreadsheet(id_data, 'To Call')
    if not df_to_call.empty:
        time.sleep(2)
        df_to_call = get_df_spreadsheet(id_data, 'To Call')

    # Read Dashboards (Aircall Data)
    df_dashboards = get_df_spreadsheet(id_data, 'Dashboards')
    if not df_dashboards.empty:
        time.sleep(2)
        df_dashboards = get_df_spreadsheet(id_data, 'Dashboards')

    for i_dash in range(0, len(df_dashboards)):

        df_data = get_df_spreadsheet(df_dashboards['id'][i_dash], 'Data')
        df_to_call_country = df_to_call[df_to_call['country_code'] == df_dashboards['country'][i_dash]]
        if not df_data.empty:
            time.sleep(2)
            df_data = get_df_spreadsheet(df_dashboards['id'][i_dash], 'Data')

        for i_agent in range(0, len(df_data)):
        #for i_agent in range(0, 3):
            if df_data['Type'][i_agent] == 'Outbound':
                df_outbound = pd.DataFrame()
                priorities = [df_data['Priority 1'][i_agent], df_data['Priority 2'][i_agent], df_data['Priority 3'][i_agent]]
                cities = [df_data['City 1'][i_agent], df_data['City 2'][i_agent], df_data['City 3'][i_agent]]

                for i_prio in range(0, len(priorities)):
                    if priorities[i_prio] != '':
                        for i_city in range(0, len(cities)):
                            if cities[i_city] != '':
                                df_outbound = df_outbound.append(df_to_call_country[(df_to_call_country['segment'] == priorities[i_prio]) & (df_to_call_country['city_code'] == cities[i_city])])

                if not df_outbound.empty:
                    print(df_data['Email'][i_agent])
                    agent_id = get_agent_id_aircall(df_data['Email'][i_agent])
                    response_delete = remove_dialer_campaign(agent_id)
                    response_dialer = add_dialer_campaign(agent_id, df_outbound['phone'].to_list()[:int(df_data['Number calls / agent'][i_agent])])

            time.sleep(1)


#scheduler.start()




















