from Functions.queries import get_query


def get_segment(dwh_credentials, query):

    df_segment = get_query(dwh_credentials, query)
    df_segment = df_segment.rename(columns={'first_order_date': "onboarding_date"})
    df_segment['segment'] = '25 orders W-1'
    df_segment['tag'] = df_segment['city_code'] + '-ACT'
    columns = ['city_code', 'country_code', 'courier_id', 'name', 'phone', 'onboarding_date', 'segment', 'tag']

    return df_segment[columns]


























