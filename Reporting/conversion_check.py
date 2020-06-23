import pandas
from Conversions.Functions import sql as mysql


df_conversions_queries=mysql.get_conversions_queries()
# convert to list

conversions_queries = df_conversions_queries.values.tolist()
# loop around each conversion query

for conversion_query in conversions_queries:
    # query for the calls that fall into the conversion criteria
    df_calls = mysql.get_calls(conversion_query[0], conversion_query[-1])
    calls = df_calls.values.tolist()
    # loop arround each call to get the conversion
    for call in calls:
        # prepare query
        query = conversion_query[2].replace("*courier_id*", str(call[2])).replace("*call_time*", str(call[3]))
        # check if we have to query in paco_db or dwh
        if conversion_query[1] == 'paco_db':
            df_conversion = mysql.get_mysql(query)
        if conversion_query[1] == 'dwh':
            df_conversion = mysql.get_postgres(query)

        if not df_conversion.empty:  # check if we got a result for the conversion. (if empty there is an error.)
            # conversion to list
            conversion = df_conversion.loc[0].values.tolist()
            # check if the conversion is true or false
            if conversion[1]:
                # check if conversion happen in the time limit
                if int(conversion[2]) <= int(conversion_query[3]) and conversion[2] > 0:
                    post = mysql.post_conversion(call[0], bool(conversion[1]), conversion[2])
                else:
                    post = mysql.post_conversion(call[0], False, conversion[2])
            else:
                post = mysql.post_conversion(call[0], False, '')
            if not post:
                print('Error posting to cloudtalk call id: ', call[0], bool(conversion[1]), conversion[2])
        else:  # report error not courier found
            # replace later with zap post
            print('Error not courier found with query courier id: ', call[2], ' and call id: ', call[0], ' query: ',
                  query)
