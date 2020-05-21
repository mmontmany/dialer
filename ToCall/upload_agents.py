import Functions.CT_campaigns as campaigns
import Functions.get_mysql as mysql

#set up hub:
hub = "latam"

#execute agent assignement to get the agents to each required campaign id
#(this is not uploaded anywhere)
#df_campaigns_assign_agents=agents.assign_agents_2_campaign(df_campaigns,df_sorted_agents) #being remove to get assigned agents from sheet
df_campaigns_assign_agents = mysql.get_campaigns_with_assigned_agents_by_hub(hub)

#get agents for email to id conversion
df_agents= mysql.get_agents_by_hub(hub)

#convert emails into ids
df_campaigns_assign_agents =campaigns.assigned_agents_emails_2_id(df_campaigns_assign_agents, df_agents)

#execute the final function tu assign the agents to each required campaign id in cloudtalk
campaigns.all_campaign_assign_agents(df_campaigns_assign_agents)
