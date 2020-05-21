import Functions.CT_campaigns as campaigns
import Functions.CT_agents as agents
import Functions.get_mysql as mysql
import Functions.get_sheets as sheets

#set up hub:
hub = "latam"

# get dataframes
df_agents = mysql.get_agents_by_hub(hub)
df_groups = mysql.get_groups_by_hub(hub)
df_campaigns = mysql.get_campaigns_by_hub(hub)

#execute agents so we have agents_ids from cloudtalk. this also updates mysql
df_agents = agents.all_agents(df_agents)

#update mysql with cloudtalk ids
mysql.agents2mysql(df_agents)

#execute campaigns setup so we make sure all campaigns are in both mysql and cloudtalk
df_campaigns = campaigns.all_campaign_set_up(df_campaigns)

#get scripts and scripts ids and survey ids if any
df_scripts = mysql.get_scripts_by_hub(hub)
#add scripts or edit script
df_scripts = campaigns.all_script(df_scripts)
#add survey or dont add survey (cant edit)
df_scripts = campaigns.all_survey(df_scripts)
#upload scripts ids and surveys ids to brain
mysql.scripts2mysql(df_scripts)
#get scripts 2 campaings query to match and add scripts and survey ids to df_campaigns
df_scripts_2_campaigns = mysql.get_campaign_2_scripts(hub)
#add script ids and surveyids to df campaigns
df_campaigns = campaigns.scripts_2_campaigns(df_campaigns,df_scripts,df_scripts_2_campaigns)

#add tags ids to campaigns if there is not a tag id number
df_campaigns = campaigns.get_tags(df_campaigns)

mysql.campaigns2mysql(df_campaigns)

#execute agents sorting by group
df_sorted_agents = agents.sort_agents(df_groups,df_agents)

#get df for overwrites
df_overwrites = agents.sorted_agents_4_overwites(df_groups,df_sorted_agents,df_agents)
#put overwrites in shhet
sheets.post_overwrites(df_overwrites)
