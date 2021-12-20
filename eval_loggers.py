# -*- coding: utf-8 -*-
"""
Created on Fri Feb  5 10:14:27 2021

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
    
    query_logger = ("SELECT logger_id, logger_name, loggers.model_id, has_rain, logger_type FROM loggers " 
                    "inner join logger_models "
                    "on logger_models.model_id = loggers.model_id "
                    "where site_id not in (51,52,53) "
                    "and date_deactivated is NULL")
    loggers = db.df_read(query_logger, connection = "common")
    
    
#    memc = memcache.Client(['127.0.0.1:11211'], debug=1)
#    accelerometers=memc.get('DF_ACCELEROMETERS')
#    tsm_sensors = memc.get('DF_TSM_SENSORS')
#    tsm_sensors = tsm_sensors[pd.isnull(tsm_sensors.date_deactivated)]
    
    query = "SELECT * FROM senslopedb.logger_counter"
    #df_count = qdb.get_db_dataframe(query)
#    engine=create_engine('mysql+pymysql://root:senslope@192.168.150.77:3306/senslopedb', echo = False)
    conn = mysql.connect()
    df_count = pd.read_sql(query, con=conn)
    


    
    df_count = pd.merge(loggers, df_count, how = 'inner', on = 'logger_id')

    df_count['eval_count'] = ""
    df_count['eval_count'][df_count.data_count>33] = "Ok"                      #more than 70%
    df_count['eval_count'][df_count.data_count<=33] = "Not Ok"                 #less than 70%
    df_count['eval_count'][df_count.data_count==0] = "No data"                 #0%
    
    df_count['eval_rain'] = ""
    df_count['eval_rain'][df_count.overrain>1]= "Not ok Rain gauge"
    
    df_count['eval_batt'] = ""
    df_count['eval_batt'][df_count.lowbatt1>12]= "Low Batt 1"
    df_count['eval_batt'][df_count.lowbatt2>12]= "Low Batt 2"
    df_count['eval_batt'][(df_count.lowbatt1>12)&(df_count.lowbatt2>12)]= "Low Batt both"
    
    df_count['eval_csq'] = ""
    df_count['eval_csq'][df_count.lowcsq/df_count.data_count>0.7] = "low signal"
    
    df_count['eval_rssi'] = ""
    df_count['eval_rssi'][df_count.lowrssi/df_count.data_count>0.7] = "low rssi"
    

    df_summary = df_count.drop(['logger_id','ts','model_id','data_count','overrain','lowbatt1','lowbatt2','lowcsq'],axis=1)
    df_summary = df_summary.sort_values(by = 'logger_name')
    return df_summary

