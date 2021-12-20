# -*- coding: utf-8 -*-
"""
Created on Fri Feb  5 08:09:50 2021

@author: Dynaslope
"""


import pandas as pd
import memcache
import numpy as np
from sqlalchemy import create_engine

import dynadb.db as db

def main():
#    memc = memcache.Client(['127.0.0.1:11211'], debug=1)
#    tsm_sensors=memc.get('DF_TSM_SENSORS')
    df = pd.DataFrame()
    
#arq
    query_logger = ("SELECT logger_id, logger_name, loggers.model_id, has_rain, logger_type FROM loggers " 
                    "inner join logger_models "
                    "on logger_models.model_id = loggers.model_id "
                    "where site_id not in (51,52,53) "
                    "and date_deactivated is NULL  and logger_type in ('arq','regular')")
    loggers = db.df_read(query_logger, connection = "common")
    
    
    for l_id,l_name in zip(loggers.logger_id, loggers.logger_name):
        print(l_id, l_name)
        
        query_data = ("SELECT {} as logger_id,DATE_FORMAT(NOW(), '%%Y-%%m-%%d %%H:%%i') AS ts, "
                      "count(data_id) as data_count,SUM(IF(rain>50, 1, 0)) AS overrain, "
                      "SUM(IF(battery1<3.7, 1, 0)) AS lowbatt1, "
                      "SUM(IF(battery2<3.7, 1, 0)) AS lowbatt2, "
                      "SUM(IF(csq<13 or csq>=98, 1, 0)) AS lowcsq FROM rain_{} "
                      "where ts >= DATE_SUB(Now(), interval 1 DAY)".format(l_id, l_name))
        logger_count = db.df_read(query_data, connection = "analysis")
        df = df.append(logger_count, ignore_index = True)

#gateway
    query_logger = ("SELECT logger_id, logger_name, loggers.model_id, has_rain, logger_type FROM loggers " 
                    "inner join logger_models "
                    "on logger_models.model_id = loggers.model_id "
                    "where site_id not in (51,52,53) "
                    "and date_deactivated is NULL and logger_type = 'gateway'")
    loggers = db.df_read(query_logger, connection = "common")
    
    
    for l_id,l_name in zip(loggers.logger_id, loggers.logger_name):
        print(l_id, l_name)
        
        query_data = ("SELECT {} as logger_id,DATE_FORMAT(NOW(), '%%Y-%%m-%%d %%H:%%i') AS ts, "
                      "count(data_id) as data_count,SUM(IF(rain>50, 1, 0)) AS overrain, "
                      "SUM(IF(battery1<9, 1, 0)) AS lowbatt1, "
                      "SUM(IF(battery2<3.7, 1, 0)) AS lowbatt2, "
                      "SUM(IF(csq<13 or csq>=98, 1, 0)) AS lowcsq FROM rain_{} "
                      "where ts >= DATE_SUB(Now(), interval 1 DAY)".format(l_id, l_name))
        logger_count = db.df_read(query_data, connection = "analysis")
        df = df.append(logger_count, ignore_index = True)

#routers
        
    query_router = ("SELECT logger_id, logger_name, loggers.model_id, has_rain, logger_type FROM loggers " 
                "inner join logger_models "
                "on logger_models.model_id = loggers.model_id "
                "where site_id not in (51,52,53) "
                "and date_deactivated is NULL and logger_type = 'router'")
    routers = db.df_read(query_router, connection = "common")
    
    query_data = ("SELECT logger_id,DATE_FORMAT(NOW(), '%%Y-%%m-%%d %%H:%%i') AS ts,count(ts) as data_count, "
                  "SUM(IF(rssi_val>90, 1, 0)) AS lowrssi, "
                  "SUM(IF(battery<9, 1, 0)) AS lowbatt1 "
                  " FROM analysis_db.router_rssi "
                  "where ts >= DATE_SUB(Now(), interval 1 DAY) group by logger_id")
    router_count = db.df_read(query_data, connection = "analysis")
    df_router = pd.merge(router_count,routers['logger_id'], how = 'outer', on = ['logger_id'])
    df_router.data_count[df_router.data_count.isna()]=0
    try:
        df_router.ts[df_router.ts.isna()]=df_router.ts.loc[0]
    except:
        df_router.ts[df_router.ts.isna()]=df.ts.loc[0]
        
    dffinal = df_router.append(df, ignore_index = True, sort = True).sort_values('logger_id')
    dffinal = dffinal[["logger_id","ts", "data_count","overrain", "lowbatt1","lowbatt2","lowcsq","lowrssi"]]
    
    engine=create_engine('mysql+pymysql://root:senslope@127.0.0.1:3306/senslopedb', echo = False)
    dffinal.to_sql(name = 'logger_counter', con = engine, if_exists = 'replace', index = False)

    
if __name__ == "__main__":
    main()

