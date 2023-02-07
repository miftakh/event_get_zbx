import time
from datetime import datetime
from pyzabbix import ZabbixAPI
import pandas as pd
import pytz
from sqlalchemy import create_engine, MetaData, Table

zabbix_url = "http://<ip>/zabbix"
username = "<username>"
password = "<password>"
group_id = 'groupid'

gmt7 = pytz.timezone("Asia/Jakarta")
date_from = datetime.strptime("2023-02-01 00:00:00", "%Y-%m-%d %H:%M:%S")
date_till = datetime.strptime("2023-02-04 23:59:59", "%Y-%m-%d %H:%M:%S")

# Convert the datetime object to a Unix timestamp
time_from = time.mktime(date_from.timetuple())
time_till = time.mktime(date_till.timetuple())

def get_zabbix_events(zabbix_url, username, password, group_id, time_from, time_till, event_type):
    zapi = ZabbixAPI(url=zabbix_url, user=username, password=password)
    event_types = {1: {"severity": 5, "value": 1}, 0: {"value": 0}}
    events = zapi.event.get(groupids=group_id, 
                            selectHosts = ["hostid","name"], 
                            output=["eventid","objectid","r_eventid", "name", "clock","severity","value"], 
                            selectTags = "extend", 
                            select_acknowledges = "extend",
                            time_from=time_from, 
                            time_till=time_till, 
                            sortfield=["clock"], 
                            filter=event_types[event_type])
    return events

def process_events_data(events, event_type):
    events_data = []
    tz = pytz.timezone('Asia/Jakarta')
    for event in events:
        acknowledges = [ack["message"] for ack in event["acknowledges"]]
        event_time = datetime.fromtimestamp(int(event["clock"]), tz=tz)
        event_data = {
            "Event ID": event["r_eventid"] if event_type == 1 else event["eventid"],
            "Object ID": event["objectid"],
            "Message": event["name"],
            "R event": event["r_eventid"],
            "Severity": event["severity"],
            "Value": event["value"],
            "Acknowledges": acknowledges,
            "Host ID": event["hosts"][0]["hostid"],
            "Host Name": event["hosts"][0]["name"],
            f"Time {'Problem' if event_type==1 else 'Recover'}": event_time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        events_data.append(event_data)
    return events_data

def import_to_mysql(final_df):
    try:
        engine = create_engine("mysql+pymysql://<user>:<password>@<ip>/<dbname>")
        meta = MetaData()
        table_name = "events_data"
        table = Table(table_name, meta, autoload=True, autoload_with=engine)
        exists = engine.dialect.has_table(engine, table_name)
        if not exists:
            final_df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            print("Data successfully imported to MySQL")
        else:
            final_df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            print("Data successfully updated to MySQL")
    except Exception as e:
        print("Error occurred while importing data to MySQL:", str(e))


events_down = get_zabbix_events(zabbix_url, username, password, group_id, time_from, time_till, 1)
events_up = get_zabbix_events(zabbix_url, username, password, group_id, time_from, time_till, 0)

data_down = process_events_data(events_down, 1)
data_up = process_events_data(events_up, 0)

df_down = pd.DataFrame(data_down)
df_up = pd.DataFrame(data_up)

final_df = pd.merge(df_down.sort_values(by=['Host ID','Event ID','Time Problem']),
                    df_up.sort_values(by=['Host ID','Event ID','Time Recover'])[['Event ID','Time Recover','Value']],
                    on=['Event ID'], how='left')

final_df['Time Problem'] = pd.to_datetime(final_df['Time Problem'])
final_df['Time Recover'] = pd.to_datetime(final_df['Time Recover'])
final_df['duration'] = (final_df['Time Recover'] - final_df['Time Problem']).apply(lambda x: float(x.total_seconds() / 60))

final_df = final_df.rename(columns={'Event ID': 'eventid','Object ID':'objectid','Message':'message','R event':'r_eventid','Severity':'severity',
                                    'Value_x':'status','Acknowledges':'ack','Host ID':'hostid','Host Name': 'host_name',
                                    'Time Problem':'time_problem','Time Recover':'time_recover','Value_y':'status_r'
                                })

final_df = final_df[['eventid','objectid','hostid','host_name','status','severity','status_r','time_problem','time_recover','duration','message']]

print(final_df)
import_to_mysql(final_df)
