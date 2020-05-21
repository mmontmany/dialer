import requests
import pandas as pd
import time

ACCESS_KEY_ID = 'KEY'
ACCESS_KEY_SECRET = 'SECRET'

def add_agent(firstname, lastname, email, list_agents):
    try:
        ct_agent_id = ""
        url = 'https://my.cloudtalk.io/api/agents/add.json'
        data = {
            "firstname": firstname,
            "lastname": lastname,
            "email": email,
            "pass": "glovo1234",
            "status_outbound": True,
        }
        for i in list_agents.json()['responseData']['data']:
            if i['Agent']['email'] == email:
                ct_agent_id = i['Agent']['id']
                break
        if not ct_agent_id:
            time.sleep(0.1)
            r = requests.put(url, auth=(ACCESS_KEY_ID, ACCESS_KEY_SECRET), json=data)
            if r.json()['responseData']['status'] == 201:
                ct_agent_id = r.json()['responseData']['data']['id']
        return ct_agent_id

    except Exception:
        try:
            time.sleep(5)
            ct_agent_id = ""
            url = 'https://my.cloudtalk.io/api/agents/add.json'
            data = {
                "firstname": firstname,
                "lastname": lastname,
                "email": email,
                "pass": "glovo1234",
                "status_outbound": True,
            }
            for i in list_agents.json()['responseData']['data']:
                if i['Agent']['email'] == email:
                    ct_agent_id = i['Agent']['id']
                    break
            if not ct_agent_id:
                time.sleep(0.1)
                r = requests.put(url, auth=(ACCESS_KEY_ID, ACCESS_KEY_SECRET), json=data)
                if r.json()['responseData']['status'] == 201:
                    ct_agent_id = r.json()['responseData']['data']['id']
            return ct_agent_i

        except Exception:
            return 0

def edit_agent(firstname, lastname, email,agent_id):
    url ='https://my.cloudtalk.io/api/agents/edit/'+agent_id+'.json'
    data = {
        "firstname": firstname,
        "lastname": lastname,
        "email": email,
        "status_outbound": True,
    }
    r = requests.post(url, auth=(ACCESS_KEY_ID, ACCESS_KEY_SECRET), json=data)
    # return agent cloudtalk id for edit and assignment
    return r.json()['responseData']['status']

def all_agents(df_agents):
    #initialize variables
    agents=df_agents.values.tolist()
    list_agents = requests.get("https://my.cloudtalk.io/api/agents/index.json", auth=(ACCESS_KEY_ID, ACCESS_KEY_SECRET),params={'limit': '1000'})
    for row in agents:
        #check if there is an agent id
        if not row[-1]:
            row[-1]= add_agent(row[2],row[3],row[1],list_agents)
    df = pd.DataFrame(columns=df_agents.columns)
    df = df.append(pd.DataFrame(agents,columns=df_agents.columns))
    return df

def sort_agents(df_groups,df_agents):
    # initialize variables
    groups = df_groups.values.tolist()
    agents = df_agents.values.tolist()
    sorted_agents = []
    # part time workeers solve issue
    for i in range(len(groups)):
        # append empty list to add agentas
        agent_count = 0
        while agent_count < float(groups[i][4]-.001): #adding small percentage difference to avoid loop
            # check if empty for assigning agents ( not remaining agents)
            if not agents:
                break
            # check if the remaining agents doesnt have remaining hours to be assign.
            elif not agents[0][4]:
                agents.pop(0)
            else:
                # remove the agent from list to evalute its involvement
                temp_agent = agents.pop(0)
                # reduce the group to check status
                needed_group = float(groups[i][4]) - agent_count
                # check if we have more workinmg hours than needed for the group
                if float(temp_agent[4]) > needed_group:
                    not_assigned_agent = temp_agent.copy()
                    not_assigned_agent[4] = float(not_assigned_agent[4]) - needed_group
                    # append at the beginning:
                    agents.insert(0, not_assigned_agent)
                    # calculate assigned to list
                    temp_agent[4] = float(temp_agent[4]) - float(not_assigned_agent[4])
                # add agent to new list
                temp_agent[2] = groups[i][0]  # assign first name as group name
                sorted_agents.append(temp_agent)
                agent_count += float(sorted_agents[-1][-2])
    # add mixed agents
    if agents:
        for i in range(len(agents)):
            temp_agent = agents.pop(0)
            temp_agent[2] = 'MIXED_AGENTS'
            sorted_agents.append(temp_agent)

    df = pd.DataFrame(
        columns=['agent_id', 'email', 'campaign_name', 'last_name', 'dedication', 'cloudtalk_agent_id'])
    df = df.append(pd.DataFrame(sorted_agents,
                                columns=['agent_id', 'email', 'campaign_name', 'last_name', 'dedication',
                                         'cloudtalk_agent_id']))
    del df['email']
    del df['agent_id']
    del df['last_name']
    del df['dedication']
    return df

def assign_agents_2_campaign(df_campaigns,df_sorted_agents):
    campaigns = df_campaigns.values.tolist()
    sorted_agents = df_sorted_agents.values.tolist()
    #loop arround all campaigns
    for i in range(len(campaigns)):
        #set up agents_real as a list to insert values of agents there
        campaigns[i][4]=[]
        #loop around sorted agents
        for j in range(len(sorted_agents)):
            #check if trimmed campaign name is equal to the sorted agents group name
            if campaigns[i][0][0:10] == sorted_agents[j][0]:
                #add the agent to the list
                campaigns[i][4].append(sorted_agents[j][1])
    df = pd.DataFrame(columns=df_campaigns.columns)
    df = df.append(pd.DataFrame(campaigns, columns=df_campaigns.columns))
    return df

def sorted_agents_4_overwites(df_groups,df_sorted_agents,df_agents):
    groups = df_groups.values.tolist()
    sorted_agents = df_sorted_agents.values.tolist()
    agents = df_agents.values.tolist()

    # replace ids for email for display
    for agent in sorted_agents:
        for email in agents:
            if agent[1]==email[5]:
                agent[1]=email[1]
    #add segment name:
    for group in groups:
                group.append(group[0][7:])
    #add emails as columns for overwrites
    for i in range(len(groups)):
        for j in range(len(sorted_agents)):
            if groups[i][0] == sorted_agents[j][0]:
                groups[i].append(sorted_agents[j][1])
    #create data frame
    df_overwrites= pd.DataFrame()
    df_overwrites = df_overwrites.append(pd.DataFrame(groups))
    #set up colmuns fix for sheets upload
    cols = df_overwrites.columns.tolist()
    #dixing for variable number of columns
    cols_end=cols[7:]
    cols_replace=[1,2,6,0,4]+cols_end
    #convert overwrites df into de fixed version with the correct order of columns to be upload
    df_overwrites= df_overwrites[cols_replace]
    df_overwrites=df_overwrites.fillna("")
    return df_overwrites

