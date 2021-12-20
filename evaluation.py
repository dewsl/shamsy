# -*- coding: utf-8 -*-
"""
Created on Wed May 15 15:21:31 2019

@author: Dynaslope
"""

import memcache
import analysis.querydb as qdb
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from db_config import mysql
import dynadb.db as db
import re

def evaluate():
    memc = memcache.Client(['127.0.0.1:11211'], debug=1)
    accelerometers=memc.get('DF_ACCELEROMETERS')
    tsm_sensors = memc.get('DF_TSM_SENSORS')
    tsm_sensors = tsm_sensors[pd.isnull(tsm_sensors.date_deactivated)]
    
    query = "SELECT * FROM data_counter where ts = (SELECT max(ts) FROM data_counter)"
    #df_count = qdb.get_db_dataframe(query)
#    engine=create_engine('mysql+pymysql://root:senslope@192.168.150.77:3306/senslopedb', echo = False)
    conn = mysql.connect()
    df_count = pd.read_sql(query, con=conn)
    
    query_sms_stat = "SELECT * FROM tsm_sms_stat"
    #df_count = qdb.get_db_dataframe(query)
#    engine=create_engine('mysql+pymysql://root:senslope@192.168.150.77:3306/senslopedb', echo = False)
    conn = mysql.connect()
    df_sms = pd.read_sql(query_sms_stat, con=conn)

    query_stat = ("Select accel_id, status, remarks from "
                  "(SELECT max(stat_id) as 'latest_stat_id' FROM "
                  "accelerometer_status group by accel_id) as stat "
                  "inner join accelerometer_status on latest_stat_id = stat_id")
    dfstat = db.df_read(query_stat, connection="analysis")
    #engine=create_engine('mysql+mysqlconnector://root:senslope@192.168.150.75:3306/senslopedb', echo = False)
    #dfstat = pd.read_sql(query_stat, con=engine)

    
    df_count = pd.merge(accelerometers[['tsm_id','accel_id','node_id','accel_number','in_use']], df_count, how = 'inner', on = 'accel_id')
    df_count = pd.merge(df_count,dfstat, how = 'outer', on = 'accel_id')
    
    df_count = pd.merge(tsm_sensors[['tsm_id','tsm_name']], df_count, how = 'inner', on = 'tsm_id')
    
    df_count['good_raw']=0
    df_count['good_raw'][df_count.percent_raw>=75]=1
    
    df_count['good_volt']=np.nan
    df_count['good_volt'][df_count.percent_voltf>=75]=1
    df_count['good_volt'][df_count.percent_voltf<75]=0
           
           
    df_count['good_range']=np.nan
    df_count['good_range'][df_count.percent_rangef>=70]=1
    df_count['good_range'][df_count.percent_rangef<70]=0
    
    df_count['good_ortho']=np.nan
    df_count['good_ortho'][df_count.percent_orthof/df_count.percent_rangef * 100.0>=80]=1
    df_count['good_ortho'][df_count.percent_orthof/df_count.percent_rangef * 100.0<80]=0
           
    df_count['good_outlier']=np.nan
    df_count['good_outlier'][df_count.percent_outlierf/df_count.percent_orthof * 100.0>=80]=1
    df_count['good_outlier'][df_count.percent_outlierf/df_count.percent_orthof * 100.0<80]=0
    
    query_validity = ("SELECT tsm_id,node_id, COUNT(IF(na_status=1,1, NULL))/count(ts)*100.0 "
                      "as 'percent_valid' FROM node_alerts group by tsm_id, node_id")
    df_validity = db.df_read(query_validity, connection="analysis")
    df_validity = pd.merge(accelerometers[['tsm_id','accel_id','node_id']], df_validity, how = 'outer', on = ['tsm_id','node_id'])
    df_validity = df_validity.drop(['tsm_id','node_id'],axis=1) 
    
    df_count = pd.merge(df_count, df_validity, how = 'inner', on = 'accel_id')
    
    df_count['recommendation'] = np.nan
    df_count['recommendation'][(df_count.percent_valid <50)] = 'tag as use with caution - alert invalid'
    df_count['recommendation'][(df_count.good_outlier == 0)] = 'tag as fluctuating data'
    df_count['recommendation'][(df_count.good_ortho == 0) & (df_count.status != 4)] = 'tag as not ok - invalid magnitude'
    df_count['recommendation'][(df_count.good_range == 0) & (df_count.status != 4)] = 'tag as not ok - out of range'
    
    
    #count good raw (with at least 75% of data)
    good_raw = df_count['ts'][(df_count.percent_raw>=75) & (df_count.status!=4)].groupby([df_count.tsm_id,df_count.tsm_name]).size().rename('good_raw')
    bad_raw = df_count['ts'][(df_count.percent_raw<75) & (df_count.status!=4)].groupby([df_count.tsm_id,df_count.tsm_name]).size().rename('bad_raw')
    not_ok = df_count['ts'][df_count.status==4].groupby([df_count.tsm_id,df_count.tsm_name]).size().rename('not_ok')
    df_raw = pd.concat([good_raw,bad_raw,not_ok], axis = 1)
    df_raw[np.isnan(df_raw)]=0
    df_raw = df_raw.reset_index()
    df_raw = df_raw.sort_values(by = 'tsm_name').reset_index(drop = True)
    df_raw['percent_good_raw'] = df_raw.good_raw / (df_raw.good_raw + df_raw.bad_raw ) * 100.0
    df_raw['raw_status'] = np.nan
    df_raw['raw_status'][df_raw.percent_good_raw>50] = 'Ok'
    df_raw['raw_status'][df_raw.percent_good_raw<=50] = 'Not Ok'
          
    #count good volt (with at least 75% of data)
    good_volt = df_count['ts'][df_count.percent_voltf>=75].groupby([df_count.tsm_id,df_count.tsm_name]).size().rename('good_volt')
    bad_volt = df_count['ts'][df_count.percent_voltf<75].groupby([df_count.tsm_id,df_count.tsm_name]).size().rename('bad_volt')
    nan_volt = df_count['ts'][np.isnan(df_count.percent_voltf)].groupby([df_count.tsm_id,df_count.tsm_name]).size().rename('nan_volt')
    df_volt = pd.concat([good_volt,bad_volt,nan_volt], axis = 1)
    df_volt[np.isnan(df_volt)]=0
    df_volt = df_volt.reset_index()
    df_volt['percent_good_volt'] = df_volt.good_volt / (df_volt.good_volt + df_volt.bad_volt) * 100.0
    df_volt['volt_status'] = 'no data'
    df_volt['volt_status'][df_volt.percent_good_volt>50] = 'Ok'
    df_volt['volt_status'][df_volt.percent_good_volt<=50] = 'Not Ok'
           
    #count good filtered (with at least 70% of data)
    good_filter = df_count['ts'][df_count.percent_outlierf>=70].groupby([df_count.tsm_id,df_count.tsm_name]).size().rename('good_filtered')
    bad_filter = df_count['ts'][df_count.percent_outlierf<70].groupby([df_count.tsm_id,df_count.tsm_name]).size().rename('bad_filtered')
    nan_filter = df_count['ts'][np.isnan(df_count.percent_outlierf)].groupby([df_count.tsm_id,df_count.tsm_name]).size().rename('nan_filtered')
    to_tag_filter = df_count['ts'][df_count.recommendation.notnull()].groupby([df_count.tsm_id,df_count.tsm_name]).size().rename('to_tag_filtered')
    
    zero_filter = df_count['ts'][(df_count.percent_outlierf==0) & (df_count.in_use ==1)].groupby([df_count.tsm_id,df_count.tsm_name]).size().rename('zero_filtered')
    
    
    df_filter = pd.concat([good_filter,bad_filter,nan_filter,to_tag_filter,zero_filter], axis = 1)
#    df_filter = pd.merge(tsm_sensors[['tsm_id', 'tsm_name','number_of_segments']],df_filter, how = 'inner', on = ['tsm_id','tsm_name'])
    df_filter[np.isnan(df_filter)]=0
    df_filter = df_filter.reset_index()
    df_filter = pd.merge(tsm_sensors[['tsm_id', 'tsm_name','number_of_segments']],df_filter, how = 'inner', on = ['tsm_id','tsm_name'])
    df_filter['percent_good_filtered'] = df_filter.good_filtered / (df_filter.good_filtered + df_filter.bad_filtered) * 100.0
    df_filter['filter_status'] = 'no data'
    df_filter['filter_status'][df_filter.percent_good_filtered>10] = 'Ok'
    df_filter['filter_status'][df_filter.to_tag_filtered>0] = 'to tag accelerometers'
    df_filter['filter_status'][df_filter.percent_good_filtered<=10] = 'Not Ok'
    df_filter['filter_status'][df_filter.zero_filtered/df_filter.number_of_segments >=0.90] = 'Too Bad'
    

    df_count = df_count.drop(['count_id','ts','good_raw','good_volt','good_range','good_ortho','good_outlier'],axis=1)        
    
    df_summary = pd.merge(df_raw[['tsm_id', 'tsm_name','raw_status']],df_filter[['tsm_id', 'tsm_name','filter_status']], how = 'inner', on = ['tsm_id','tsm_name'])
    df_summary = pd.merge(df_summary,df_volt[['tsm_id', 'tsm_name','volt_status']], how = 'inner', on = ['tsm_id','tsm_name'])
    df_summary = pd.merge(df_summary,df_sms, how = 'left', on = 'tsm_id')
    
    #for decomission
    df_summary["decom_status"]=""
    df_summary["decom_status"][(df_summary.volt_status=='no data') & (df_summary.sms_msg.str.contains("no data parsed"))] = "for decommission"
    df_summary["decom_status"][(df_summary.filter_status=='Too Bad')] = "for decommission"
    
    
    return df_summary, df_count, df_raw, df_filter,df_volt

