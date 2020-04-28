from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from gspread.exceptions import APIError
import pandas as pd
from datetime import datetime
from datetime import timedelta
import os
from apscheduler.schedulers.blocking import BlockingScheduler
import time

global id_dashboard
global id_data
global credentials

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('cs_aircall_dashboard.json', scope)
id_dashboard = '1-bEDV8fbEzIsEY9V-E3ZUXOKDm73TiHr0hcYb5omUG0'
id_data = '1_UDM2ZAMjg3zV9P1xbrd6NtAnuRvDiMa8jyWh6IrRdw'

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


if 1 == 1:

    # Read Aircall Data
    df_churn = get_df_spreadsheet(id_data, 'Churn')
    if not df_churn.empty:
        time.sleep(2)
        df_churn = get_df_spreadsheet(id_data, 'Churn')
    df_historic = get_df_spreadsheet(id_data, 'Historic D-5')
    if not df_historic.empty:
        time.sleep(2)
        df_historic = get_df_spreadsheet(id_data, 'Historic D-5')
    df_new_churn = get_df_spreadsheet(id_data, 'New Churn')
    if not df_new_churn.empty:
        time.sleep(2)
        df_new_churn = get_df_spreadsheet(id_data, 'New Churn')
    df_25w1 = get_df_spreadsheet(id_data, '25 orders W-1')
    if not df_25w1.empty:
        time.sleep(2)
        df_25w1 = get_df_spreadsheet(id_data, '25 orders W-1')
    df_slots_cfo = get_df_spreadsheet(id_data, 'Slots / CFO')
    if not df_slots_cfo.empty:
        time.sleep(2)
        df_slots_cfo = get_df_spreadsheet(id_data, 'Slots / CFO')
    df_paco = get_df_spreadsheet(id_data, 'PACO')
    if not df_paco.empty:
        time.sleep(2)
        df_paco = get_df_spreadsheet(id_data, 'PACO')

    today = datetime.now().strftime("%Y") + '-' + datetime.now().strftime("%m") + '-' + datetime.now().strftime("%d")
    today_5 = (datetime.now() - timedelta(weeks=4)).strftime("%Y") + '-' + (datetime.now() - timedelta(weeks=4)).strftime("%m") + '-' + (datetime.now() - timedelta(weeks=4)).strftime("%d")

    phones_not_to_call = []

    # Historic calls from the past 5 days
    df_historic_5 = df_historic[(df_historic['started_at'] >= today_5)]
    phones = df_historic_5['phone'].unique().tolist()

    # Historic calls from the past 5 days answered and tagged (we don't want to call)
    #df_answered_tag_5 = df_historic_5[(df_historic_5['answered_at'] != '') & (df_historic_5['tags'] != 'Call later') & (df_historic_5['tags'] != '[]')]
    df_answered_tag_5 = df_historic_5[(df_historic_5['answered_at'] != '') & (df_historic_5['tags'] != 'Call later')]
    phones_answered_tag_5 = df_answered_tag_5['phone'].unique().tolist()
    for phone in phones_answered_tag_5:
        phones.remove(phone)
        phones_not_to_call.append(phone)

    # Historic calls from the past 5 days not answered and not tagged
    df_no_answered_5 = df_historic_5[(df_historic_5['answered_at'] == '') & (df_historic_5['tags'] == '')]
    phones_no_answered_5 = df_no_answered_5['phone'].to_list()

    # We add phones that didn't answer in 4 intents
    for phone in phones:
        if phones_no_answered_5.count(phone) > 3:
            phones_not_to_call.append(phone)

    df_to_call = pd.DataFrame(columns=['phone', 'segment', 'name', 'courier_id', 'city_code', 'country_code', 'orders', 'cash_balance', 'email', 'creation_date'])

    # Churn
    print('Churn W+1')
    df_churn_week = df_churn[df_churn['churn_week'] == 'TRUE'].reset_index()
    for i_df in range(0, len(df_churn_week)):
        if df_churn_week['phone'][i_df] not in phones_not_to_call and df_churn_week['phone'][i_df] != '':
            row = {'phone': df_churn_week['phone'][i_df],
                   'segment': 'Churn W+1',
                   'name': df_churn_week['name'][i_df],
                   'courier_id': df_churn_week['courier_id'][i_df],
                   'city_code': df_churn_week['city_code'][i_df],
                   'country_code': df_churn_week['country_code'][i_df],
                   'orders': df_churn_week['delivered_orders_last_w'][i_df],
                   'cash_balance': df_churn_week['cash_balance'][i_df],
                   'email': '',
                   'creation_date': ''
                   }
            df_to_call = df_to_call.append(row, ignore_index=True)

    print('Churn M+1')
    df_churn_month = df_churn[df_churn['churn_month'] == 'TRUE'].reset_index()
    for i_df in range(0, len(df_churn_month)):
        if df_churn_month['phone'][i_df] not in phones_not_to_call and df_churn_month['phone'][i_df] != '':
            row = {'phone': df_churn_month['phone'][i_df],
                   'segment': 'Churn M+1',
                   'name': df_churn_month['name'][i_df],
                   'courier_id': df_churn_month['courier_id'][i_df],
                   'city_code': df_churn_month['city_code'][i_df],
                   'country_code': df_churn_month['country_code'][i_df],
                   'orders': df_churn_month['delivered_orders_last_m'][i_df],
                   'cash_balance': df_churn_month['cash_balance'][i_df],
                   'email': '',
                   'creation_date': ''
                   }
            df_to_call = df_to_call.append(row, ignore_index=True)

    print('Churn M+2')
    df_churn_2_month = df_churn[df_churn['churn_2_month'] == 'TRUE'].reset_index()
    for i_df in range(0, len(df_churn_2_month)):
        if df_churn_2_month['phone'][i_df] not in phones_not_to_call and df_churn_2_month['phone'][i_df] != '':
            row = {'phone': df_churn_2_month['phone'][i_df],
                   'segment': 'Churn M+2',
                   'name': df_churn_2_month['name'][i_df],
                   'courier_id': df_churn_2_month['courier_id'][i_df],
                   'city_code': df_churn_2_month['city_code'][i_df],
                   'country_code': df_churn_2_month['country_code'][i_df],
                   'orders': df_churn_2_month['delivered_orders_last_2_m'][i_df],
                   'cash_balance': df_churn_2_month['cash_balance'][i_df],
                   'email': '',
                   'creation_date': ''
                   }
            df_to_call = df_to_call.append(row, ignore_index=True)

    # New Churn
    print('New Churn')
    df_nc = df_new_churn[df_new_churn['churner'] == '1'].reset_index()
    for i_df in range(0, len(df_nc)):
        if df_nc['phone'][i_df] not in phones_not_to_call and df_nc['phone'][i_df] != '':
            row = {'phone': df_nc['phone'][i_df],
                   'segment': 'New Churn',
                   'name': df_nc['name'][i_df],
                   'courier_id': df_nc['courier_id'][i_df],
                   'city_code': df_nc['city_code'][i_df],
                   'country_code': df_nc['country_code'][i_df],
                   'orders': '',
                   'cash_balance': df_nc['cash_balance'][i_df],
                   'email': '',
                   'creation_date': ''
                   }
            df_to_call = df_to_call.append(row, ignore_index=True)

    # 25 orders W-1
    print('25 orders W-1')
    for i_df in range(0, len(df_25w1)):
        if df_25w1['phone'][i_df] not in phones_not_to_call and df_25w1['phone'][i_df] != '':
            row = {'phone': df_25w1['phone'][i_df],
                   'segment': '25 orders W-1',
                   'name': df_25w1['name'][i_df],
                   'courier_id': df_25w1['courier_id'][i_df],
                   'city_code': df_25w1['city_code'][i_df],
                   'country_code': df_25w1['country_code'][i_df],
                   'orders': '',
                   'cash_balance': '',
                   'email': '',
                   'creation_date': ''
                   }
            df_to_call = df_to_call.append(row, ignore_index=True)

    # CFO
    df_cfo = df_slots_cfo[df_slots_cfo['not_cfo_5'] == 'TRUE'].reset_index()
    for i_df in range(0, len(df_cfo)):
        if df_cfo['phone'][i_df] not in phones_not_to_call and df_cfo['phone'][i_df] != '':
            row = {'phone': df_cfo['phone'][i_df],
                   'segment': 'CFO',
                   'name': df_cfo['name'][i_df],
                   'courier_id': df_cfo['courier_id'][i_df],
                   'city_code': df_cfo['city_code'][i_df],
                   'country_code': df_cfo['country_code'][i_df],
                   'orders': '',
                   'cash_balance': '',
                   'email': '',
                   'creation_date': ''
                   }
            df_to_call = df_to_call.append(row, ignore_index=True)

    # Pending test (PACO)
    #print('Pending test (PACO)')
    #df_pending_test = df_paco[df_paco['Pending test'] == 'TRUE'].reset_index()
    #for i_df in range(0, len(df_pending_test)):
    #    if df_pending_test['Phone'][i_df] not in phones_not_to_call and df_pending_test['Phone'][i_df] != '':
    #        row = {'phone': df_pending_test['Phone'][i_df],
    #               'segment': 'Pending test (PACO)',
    #               'name': df_pending_test['Name'][i_df],
    #               'courier_id': '',
    #               'city_code': df_pending_test['City code'][i_df],
    #               'country_code': df_pending_test['Country'][i_df],
    #               'orders': '',
    #               'cash_balance': '',
    #               'email': df_pending_test['Email'][i_df],
    #               'candidate_id': df_pending_test['Candidate_ID'][i_df],
    #               'creation_date': df_pending_test['Creation date'][i_df]
    #               }
    #        df_to_call = df_to_call.append(row, ignore_index=True)

    # Pending docs (PACO)
    #print('Pending docs (PACO)')
    #df_pending_docs = df_paco[df_paco['Pending docs'] == 'TRUE'].reset_index()
    #for i_df in range(0, len(df_pending_docs)):
    #    if df_pending_docs['Phone'][i_df] not in phones_not_to_call and df_pending_docs['Phone'][i_df] != '':
    #        row = {'phone': df_pending_docs['Phone'][i_df],
    #               'segment': 'Pending docs (PACO)',
    #               'name': df_pending_docs['Name'][i_df],
    #               'courier_id': '',
    #               'city_code': df_pending_docs['City code'][i_df],
    #               'country_code': df_pending_docs['Country'][i_df],
    #               'orders': '',
    #               'cash_balance': '',
    #               'email': df_pending_docs['Email'][i_df],
    #               'candidate_id': df_pending_test['Candidate_ID'][i_df],
    #               'creation_date': df_pending_docs['Creation date'][i_df]
    #               }
    #        df_to_call = df_to_call.append(row, ignore_index=True)

    df_to_call = df_to_call.drop_duplicates('phone')
    post_to_sheets(id_data, 'To Call', df_to_call)
