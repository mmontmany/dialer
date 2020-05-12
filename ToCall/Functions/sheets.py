import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
import time


def get_sheet(id, sheet_name, credentials):

    try:
        client = gspread.authorize(credentials)
        sheet = client.open_by_key(id)
        data = sheet.worksheet(sheet_name).get_all_values()

        return pd.DataFrame(data, columns=data.pop(0))

    except Exception:

        time.sleep(1)

        try:
            client = gspread.authorize(credentials)
            sheet = client.open_by_key(id)
            data = sheet.worksheet(sheet_name).get_all_values()

            return pd.DataFrame(data, columns=data.pop(0))

        except Exception:

            return pd.DataFrame()


def post_sheet(id, sheet_name, credentials, df, clear):

    try:
        client = gspread.authorize(credentials)
        sheet = client.open_by_key(id)
        worksheet = sheet.worksheet(sheet_name)
        if clear:
            worksheet.clear()

        set_with_dataframe(worksheet, df)

        return True

    except Exception:

        time.sleep(1)

        try:
            client = gspread.authorize(credentials)
            sheet = client.open_by_key(id)
            worksheet = sheet.worksheet(sheet_name)
            if clear:
                worksheet.clear()

            set_with_dataframe(worksheet, df)

            return True

        except Exception:

            return False




























