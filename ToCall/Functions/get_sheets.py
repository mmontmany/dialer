# Need to add json creds to server in order to run this :)
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import gspread2df as g2d
from df2gspread import df2gspread as d2g

# use creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('sheet_creds.json', scope)
client = gspread.authorize(creds)


def get_campaigns():
    # we will have to change this to mysql
    # Find a workbook by name and open the first sheet
    spreadsheet_key = '1QJHK7qmLjno3Gl6_h_ueaSTvnjXOy8sKSkf5UK-UMzg'
    wks_name = 'Segments'
    df_groups = g2d.download(spreadsheet_key, wks_name, credentials=creds, col_names=True, row_names=True)
    return df_groups

def get_agents():
    # Find a workbook by name and open the first sheet
    spreadsheet_key = '1QJHK7qmLjno3Gl6_h_ueaSTvnjXOy8sKSkf5UK-UMzg'
    wks_name = 'agents'

    df_agents = g2d.download(spreadsheet_key, wks_name, credentials=creds, col_names=True, row_names=True)
    return df_agents

def post_agents(df):
    # Find a workbook by name and open the first sheet
    spreadsheet_key = '1QJHK7qmLjno3Gl6_h_ueaSTvnjXOy8sKSkf5UK-UMzg'
    wks_name = 'Sorted_agents'
    result = d2g.upload(df, spreadsheet_key, wks_name, credentials=creds, row_names=False)
    return result

def post_campaigns(df):
    spreadsheet_key = '1QJHK7qmLjno3Gl6_h_ueaSTvnjXOy8sKSkf5UK-UMzg'
    wks_name = 'Campaigns'
    result = d2g.upload(df, spreadsheet_key, wks_name, credentials=creds, row_names=False)
    return result

def post_overwrites(df):
    spreadsheet_key = '1yWTGuxCsfwE-V6QIngj6CYtxgITK7QuqTI-OAFWkdB4'
    wks_name = 'Agents assigned'
    result = d2g.upload(df, spreadsheet_key, wks_name, credentials=creds, row_names=False,col_names=False,start_cell= 'A2',clean =False)
    return result
