from Functions.queries import get_query


def get_segment(dwh_credentials, query):

    df_segment = get_query(dwh_credentials, query)
    df_segment['segment'] = 'Cash Churn'
    df_segment['tag'] = df_segment['city_code'] + '-CASH'

    return df_segment

