import requests
import pandas as pd

cookie = 'Cookie'

# fix script id and survey id default values
def add_campaign(campaign_name, tags_id='103203', script_id='829', survey_id='379', agents_ids=[], attempts=2, button_0='PING_EMAIL', button_1='PING_SMS', button_2='REJECTED', button_3='RESCHEDULE', button_4='SKIP'):
    url= 'https://my.cloudtalk.io/pdCampaigns/add'
    #standard header
    headers = {
        "Cookie": cookie
    }
    #initiating data for campaigns
    data = {
        '_method': 'POST',
        'data[PdCampaign][is_predictive]': '0',
        'data[PdCampaign][name]': campaign_name,
        'data[PdCampaign][status]': 'active',
        'data[PdCampaign][has_schedule_date]': '0',
        'data[PdCampaign][schedule_start_date]': '',
        'data[PdCampaign][schedule_start_time]': '',
        'data[Contacts][tags]': '',
        'data[Contacts][tags][]': tags_id,
        'data[PdCampaign][attempts]': attempts,
        'data[PdCampaign][attempts_interval]': '18',
        'data[PdCampaign][calls_percentage]': '100',
        'data[PdCampaign][answer_wait_time]': '45',
        'data[PdCampaign][after_call_dialing_auto]': '0',
        'data[PdCampaign][after_call_time]': '5',
        'data[PdCampaign][is_recording]': '0',
        'data[PdCampaign][call_number_id]': '',
        'data[PdCampaign][pd_call_script_id]': script_id,
        'data[PdCampaign][pd_survey_id]': survey_id,
        'data[Groups][ids]': '',
        'data[Agents][ids]': '',
        'data[Agents][ids][]': agents_ids,
        'data[PdButton][0][title]': button_0,
        'data[PdButton][0][type]': 'successful_positive',
        'data[PdButton][0][color]': '#27ae60',
        'data[PdButton][0][description]': 'Mark this call as Sale. Action will influence conversion rate.',
        'data[PdButton][1][title]': button_1,
        'data[PdButton][1][type]': 'successful_positive',
        'data[PdButton][1][color]': '#16a085',
        'data[PdButton][1][description]': 'Mark this call as interested.',
        'data[PdButton][2][title]': button_2,
        'data[PdButton][2][type]': 'successful_negative',
        'data[PdButton][2][color]': '#f39c12',
        'data[PdButton][2][description]': 'Mark this call as Not interested.',
        'data[PdButton][3][title]': button_3,
        'data[PdButton][3][type]': 'rescheduled',
        'data[PdButton][3][color]': '#2980b9',
        'data[PdButton][3][description]': 'Reschedule this call. Set date, time and notes.',
        'data[PdButton][4][title]': button_4,
        'data[PdButton][4][type]': 'unsuccessful_call_again',
        'data[PdButton][4][color]': '#7f8c8d',
        'data[PdButton][4][description]': "Skip this call - e.g. contact didn't answer.",
    }
    #create campaign
    r=requests.post(url, headers=headers, data=data)

    #find campaign id
    start_campaign_search = '<a href="/predictiveDialer/statistics/'
    end_campaign_search = '" class="btn btn-primary"'
    campaign_id=r.text[r.text.find(start_campaign_search) + len(start_campaign_search):r.text.find(end_campaign_search)]
    return campaign_id

def edit_campaign(campaign_name, campaign_id, tags_id='103203', script_id='829', survey_id='379', agents_ids=[], attempts=2, button_0='PING_EMAIL', button_1='PING_SMS', button_2='REJECTED', button_3='RESCHEDULE', button_4='SKIP'):


    #create url with campaign id
    url = 'https://my.cloudtalk.io/pdCampaigns/edit'
    #initialize headers
    headers = {
        "Cookie": cookie
    }
    #initialize data
    data = {
        '_method': 'PUT',
        'data[PdCampaign][is_predictive]': '0',
        'data[PdCampaign][name]': campaign_name,
        'data[PdCampaign][status]': 'active',
        'data[PdCampaign][has_schedule_date]': '0',
        'data[PdCampaign][schedule_start_date]': '',
        'data[PdCampaign][schedule_start_time]': '',
        'data[Contacts][tags]': '',
        'data[Contacts][tags][]': tags_id,
        'data[PdCampaign][attempts]': attempts,
        'data[PdCampaign][attempts_interval]': '18',
        'data[PdCampaign][calls_percentage]': '100',
        'data[PdCampaign][answer_wait_time]': '45',
        'data[PdCampaign][after_call_dialing_auto]': '0',
        'data[PdCampaign][after_call_time]': '5',
        'data[PdCampaign][is_recording]': '0',
        'data[PdCampaign][call_number_id]': '',
        'data[PdCampaign][pd_call_script_id]': script_id,
        'data[PdCampaign][pd_survey_id]': survey_id,
        'data[Groups][ids]': '',
        'data[Agents][ids]': '',
        'data[Agents][ids][]': agents_ids,
        'data[PdCampaign][id]': campaign_id,
        'data[PdButton][0][title]': button_0,
        'data[PdButton][0][type]': 'successful_positive',
        'data[PdButton][0][color]': '#27ae60',
        'data[PdButton][0][description]': 'Mark this call as Sale. Action will influence conversion rate.',
        'data[PdButton][1][title]': button_1,
        'data[PdButton][1][type]': 'successful_positive',
        'data[PdButton][1][color]': '#16a085',
        'data[PdButton][1][description]': 'Mark this call as interested.',
        'data[PdButton][2][title]': button_2,
        'data[PdButton][2][type]': 'successful_negative',
        'data[PdButton][2][color]': '#f39c12',
        'data[PdButton][2][description]': 'Mark this call as Not interested.',
        'data[PdButton][3][title]': button_3,
        'data[PdButton][3][type]': 'rescheduled',
        'data[PdButton][3][color]': '#2980b9',
        'data[PdButton][3][description]': 'Reschedule this call. Set date, time and notes.',
        'data[PdButton][4][title]': button_4,
        'data[PdButton][4][type]': 'unsuccessful_do_not_call',
        'data[PdButton][4][color]': '#c0392b',
        'data[PdButton][4][description]': 'Mark this call as Do not call. Action can be used for opt out purposes.',
    }
    #edit campaign
    r = requests.post(url, headers=headers, data=data)

    #find campaign id
    start_campaign_search = '<a href="/predictiveDialer/statistics/'
    end_campaign_search = '" class="btn btn-primary"'
    campaign_id=r.text[r.text.find(start_campaign_search) + len(start_campaign_search):r.text.find(end_campaign_search)]

    return campaign_id

def get_tags(df_campaigns):
    # get tag id list
    headers = {
        "Cookie": cookie
    }
    url = "https://my.cloudtalk.io/contacts"

    r = requests.get(url, headers=headers)

    # set up data for edit
    tags = df_campaigns.values.tolist()
    text = r.text

    # loop aroung each campaign name to get tag id from contact
    for i in tags:
        # check if there are tags created (if they arent no value will be return)
        if not i[8]:
            if text.find(i[0]) != -1:
                i[8] = text[text.find(i[0]) - 8:text.find(i[0]) - 8 + 6]
    df = pd.DataFrame(columns=df_campaigns.columns)
    df = df.append(pd.DataFrame(tags, columns=df_campaigns.columns))
    return df

def add_script(script_name,script_body):
    url = "https://my.cloudtalk.io/pdCallScripts/add"
    headers = {
            "Cookie": cookie
    }
    data= {
        '_method': 'POST',
        'data[PdCallScript][name]': script_name,
        'data[PdCallScript][description]': script_body,
        'files': '',
    }
    #add new script
    r = requests.post(url, headers=headers, data=data)
    #set up search vaules to get script id
    end_script_search = '" class="table-link" title="Edit">'
    middle_script_search = ' </div>\n</td>\n<td style="width: 15%;" class="actions">\n<a href="/pdCallScripts/edit/'
    text=r.text
    #search script id
    text=text[text.find(script_name):]
    script_id=text[len(script_name)+len(middle_script_search):text.find(end_script_search)]

    return (script_id)

def edit_script(script_id,script_name,script_body):
    url= "https://my.cloudtalk.io/pdCallScripts/edit"
    headers = {
            "Cookie": cookie
    }
    data= {
        '_method': 'PUT',
        'data[PdCallScript][name]': script_name,
        'data[PdCallScript][description]': script_body,
        'data[PdCallScript][id]':script_id,
        'files': '',
    }
    #edit script
    r = requests.post(url, headers=headers, data=data)

    return r

def all_script(df_scripts):

    scripts = df_scripts.values.tolist()
    # loop for all scripts
    for i in range(len(scripts)):
        # check if script id is not empty
        if scripts[i][2]:
            # call function edit scripts
            edit_script(scripts[i][2],scripts[i][0],scripts[i][1])
    for i in range(len(scripts)):
        # check if scripts id is empty
        if not scripts[i][2]:
            # call function add scripts
            scripts[i][2] = add_script(scripts[i][0],scripts[i][1])

    df = pd.DataFrame(columns=df_scripts.columns)
    df = df.append(pd.DataFrame(scripts, columns=df_scripts.columns))

    return df

def add_survey(survey_name):
    url = 'https://my.cloudtalk.io/pdSurveys/add'

    headers = {
        "Cookie": cookie
    }

    data = {
        '_method': 'POST',
        'data[PdSurvey][name]': survey_name,
        'data[PdSurveyQuestion][0][name]': 'q1',
        'data[PdSurveyQuestion][0][type]': 'text',
        'data[PdSurveyQuestion][0][order]': '1',
    }
    r = requests.post(url, headers=headers, data=data)
    start_survey_search = '<form action="/pdSurveys/edit/'
    text = r.text
    # search survey id
    text = text[text.find(start_survey_search):]
    survey_id = text[len(start_survey_search):len(start_survey_search) + 3]
    return survey_id

def all_survey(df_surveys):
    surveys = df_surveys.values.tolist()
    # loop for all scripts
    for i in range(len(surveys)):
        # check if scripts id is empty
        if not surveys[i][-1]:
            # call function add scripts
            surveys[i][-1] = add_survey(surveys[i][0])

    df = pd.DataFrame(columns=df_surveys.columns)
    df = df.append(pd.DataFrame(surveys, columns=df_surveys.columns))

    return df

def scripts_2_campaigns(df_campaigns,df_scripts,df_scripts_2_campaigns):

    #initialize values
    campaigns=df_campaigns.values.tolist()
    scripts=df_scripts.values.tolist()
    scripts_2_campaigns=df_scripts_2_campaigns.values.tolist()

    #create table with campaign names, scripts ids and survey ids
    for campaign in scripts_2_campaigns:
        for script in scripts:
            if campaign[1]==script[0]:
                campaign.append(script[2])
                campaign.append(script[3])
    #add scripts ids and survey ids to main df_campaign
    for campaign in campaigns:
        for script in scripts_2_campaigns:
            if campaign[0]==script[0]:
                campaign[9]=script[2]
                campaign[10]=script[3]
    df = pd.DataFrame(columns=df_campaigns.columns)
    df = df.append(pd.DataFrame(campaigns, columns=df_campaigns.columns))
    return df

def all_campaign_set_up(df_campaigns):
    campaigns = df_campaigns.values.tolist()
    #I need to make a function to merge agents and campaigns ids
    # loop for all campaigns
    for i in range(len(campaigns)):
        # check if campaign id empty
        if campaigns[i][7]:
            # call function add campaigns
            campaigns[i][7] = edit_campaign(campaigns[i][0], campaigns[i][7], tags_id=campaigns[i][8], script_id = campaigns[i][9], survey_id = campaigns[i][10], button_0=campaigns[i][11], button_1=campaigns[i][12], button_2=campaigns[i][13], button_3=campaigns[i][14], button_4=campaigns[i][15])
    for i in range(len(campaigns)):
        # check if campaign id empty
        if not campaigns[i][7]:
            # call function add campaigns
            # remove script id and survey id below: campaigns[i][4] = add_campaign(campaigns[i][2], campaigns[i][5], script_id = campaigns[i][6], survey_id = campaigns[i][7], agents_ids=agents_ids, button_0=campaigns[i][8], button_1=campaigns[i][9], button_2=campaigns[i][10], button_3=campaigns[i][11], button_4=campaigns[i][12])
            #remove the code above ( to create test tag, script, survey, buttons
            campaigns[i][7] = add_campaign(campaigns[i][0])

    df = pd.DataFrame(columns=df_campaigns.columns)
    df = df.append(pd.DataFrame(campaigns, columns=df_campaigns.columns))
    return df

def assigned_agents_emails_2_id(df_campaigns_assign_agents, df_agents):
    campaigns_assign_agents = df_campaigns_assign_agents.values.tolist()
    agents = df_agents.values.tolist()
    for campaign in campaigns_assign_agents:
        for i in range(len(campaign[4])):
            for agent in agents:
                if campaign[4][i] == agent[1]:
                    print('yes ', campaign[4][i], ' ', agent)
                    campaign[4][i] = agent[-1]
                    print(campaign[4][i])
                    break
    df = pd.DataFrame(columns=df_campaigns_assign_agents.columns)
    df = df.append(pd.DataFrame(campaigns_assign_agents, columns=df_campaigns_assign_agents.columns))
    return df

def all_campaign_assign_agents(df_campaigns):
    campaigns = df_campaigns.values.tolist()
    #I need to make a function to merge agents and campaigns ids
    # loop for all campaigns
    for i in range(len(campaigns)):
        # check if campaign id empty
        if campaigns[i][7]:
            # call function add campaigns
            #campaigns[i][7] = edit_campaign(campaigns[i][0], campaigns[i][7], agents_ids=campaigns[i][4], tags_id=campaigns[i][8], script_id = campaigns[i][9], survey_id = campaigns[i][10], button_0=campaigns[i][11], button_1=campaigns[i][12], button_2=campaigns[i][13], button_3=campaigns[i][14], button_4=campaigns[i][15])
            campaigns[i][7] = edit_campaign(campaigns[i][0], campaigns[i][7], agents_ids=campaigns[i][4], tags_id=campaigns[i][8], script_id = campaigns[i][9], survey_id = campaigns[i][10])

