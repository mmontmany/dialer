from Functions.queries import get_query


def get_segment(dwh_credentials, query):

    df_segment_raw = get_query(dwh_credentials, query)
    df_segment = df_segment_raw[df_segment_raw['churner'] == 1].reset_index().copy()
    df_segment = df_segment.rename(columns={'courier_last_order': "last_order_time"})
    df_segment = df_segment.rename(columns={'transport': "vehicle"})
    df_segment['segment'] = 'New Churn'
    df_segment['tag'] = df_segment['city_code'] + '-ACT'
    columns = ['city_code', 'country_code', 'courier_id', 'name', 'phone', 'excellence_score', 'cash_balance', 'vehicle', 'last_order_time', 'segment', 'tag']

    return df_segment[columns]


























