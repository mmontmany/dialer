from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json

from Functions.sheets import post_sheet

import Segments.cash_churn as cash_churn
import Segments.new_churn as new_churn
import Segments.orders_25_w as orders_25_w


scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('Files/api_secret.json', scope)
id_data = '1s5feEYZ6gjfioJrqcEbxCvqK4fbl3Wym_JOptpadUbM'

# Open files
with open('Files/dwh_secret.json') as json_file:
    dwh_credentials = json.load(json_file)

# Read CloudTalk Data

# ACTIVATION
df_act = pd.DataFrame()

# New Churn
query_new_churn = open('Queries/new_churn.sql', 'r').read()
df_new_churn = new_churn.get_segment(dwh_credentials, query_new_churn)
df_act = df_act.append(df_new_churn, ignore_index=True, sort=False)
print('New Churn')

# 25 orders W-1
query_orders_25_w = open('Queries/orders_25_w.sql', 'r').read()
df_orders_25_w = orders_25_w.get_segment(dwh_credentials, query_orders_25_w)
df_act = df_act.append(df_orders_25_w, ignore_index=True, sort=False)
print('25 orders W-1')

post_sheet(id_data, 'ACT', creds, df_act, True)

# CASH
df_cash = pd.DataFrame()

# Cash
query_cash_churn = open('Queries/cash_churn.sql', 'r').read()
df_cash_churn = cash_churn.get_segment(dwh_credentials, query_cash_churn)
df_cash = df_cash.append(df_cash_churn, ignore_index=True, sort=False)
print('Cash Churn')

post_sheet(id_data, 'CASH', creds, df_cash, True)

# FLEET MANAGAMENT
df_fm = pd.DataFrame()

post_sheet(id_data, 'FM', creds, df_fm, True)
