import json
import requests


def post_contact(ACCESS_KEY_ID, ACCESS_KEY_SECRET, df_dash_attrb, df_contact):

    api_url = "https://my.cloudtalk.io/api/contacts/add.json"

    contact_attribute = "["
    for i_attrb in range(0, len(df_dash_attrb)):
        if i_attrb != (len(df_dash_attrb) - 1):
            try:
                contact_attribute = contact_attribute + json.dumps(
                    {"attribute_id": df_dash_attrb['id'][i_attrb], "value": df_dash_attrb['title'][i_attrb] + ": " + df_contact[df_dash_attrb['code'][i_attrb]]}) + ','
            except Exception:
                contact_attribute = contact_attribute + json.dumps(
                    {"attribute_id": df_dash_attrb['id'][i_attrb], "value": ""}) + ','
        else:
            try:
                contact_attribute = contact_attribute + json.dumps(
                    {"attribute_id": df_dash_attrb['id'][i_attrb], "value": df_dash_attrb['title'][i_attrb] + ": " + df_contact[df_dash_attrb['code'][i_attrb]]}) + ']'
            except Exception:
                contact_attribute = contact_attribute + json.dumps(
                    {"attribute_id": df_dash_attrb['id'][i_attrb], "value": ""}) + ']'

    request_body = {"name": df_contact['name'],
                    "website": "https://beta-admin.glovoapp.com/courier/" + df_contact['courier_id'],
                    "city": df_contact['city_code'],
                    "industry" : df_contact['country_code'],
                    "ContactNumber": [
                        {
                            "public_number": "+" + df_contact['phone']
                        }
                    ],
                    "ContactEmail": [
                        {
                            "email": ""
                        }
                    ],
                    "ContactsTag": [
                        {
                            "name": df_contact['tag']
                        }
                    ],
                    "ContactAttribute": json.loads(contact_attribute)
                    }

    result = requests.put(api_url, auth=(ACCESS_KEY_ID, ACCESS_KEY_SECRET), json=request_body)
    print(result)
