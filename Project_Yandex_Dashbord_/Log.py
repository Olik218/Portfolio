#!/usr/bin/python
# -*- coding: utf-8 -*-
 
import getopt
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import sys
 
 
 
if __name__ == "__main__":
 
  unixOptions = "sdt:edt"
  gnuOptions = ["start_dt=", "end_dt="]
  fullCmdArguments = sys.argv
  argumentList = fullCmdArguments[1:]
 
  try:
    arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
  except getopt.error as err:
    print (str(err))
    sys.exit(2)
 
  start_dt = ''
  end_dt = ''
 
  for currentArgument, currentValue in arguments:
    if currentArgument in ("-sdt", "--start_dt"):
      start_dt = currentValue
    elif currentArgument in ("-edt", "--end_dt"):
      end_dt = currentValue
 
 
  db_config = {'user': 'praktikum_student',
             'pwd': 'Sdf4$2;d-d30pp',
             'host': 'rc1b-wcoijxj3yxfsf3fs.mdb.yandexcloud.net',
             'port': 6432,
             'db': 'data-analyst-zen-project-db'}
 
  connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
    db_config['pwd'],
    db_config['host'],
    db_config['port'],
    db_config['db'])
 
  engine = create_engine(connection_string)
 
 
  query = ''' SELECT 
                event_id,
                age_segment,
                event,
                item_id,
                item_topic,
                item_type,
                source_id,
                source_topic,
                source_type,
                TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'Etc/UTC' as dt,
                user_id
            FROM log_raw
            WHERE (TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'Etc/UTC') BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP
        '''.format(start_dt, end_dt)
 
 
  log_raw = pd.io.sql.read_sql(query, con = engine, index_col = 'event_id')
  log_raw['dt'] = pd.to_datetime(log_raw['dt']).dt.round('min')
 
 
 
  dash_visits = log_raw.groupby(['item_topic', 'source_topic', 'age_segment', 'dt']).agg({'event': 'count'}).rename(columns = {'event': 'visits'}).reset_index()
 
 
 
  dash_visits.to_sql(name = dash_visits, con = engine, if_exists = 'append', index = False)
  dash_visits.to_csv(r'/Users/dmitrij/Downloads/Dash_visits.csv', index = False)