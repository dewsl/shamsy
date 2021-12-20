# -*- coding: utf-8 -*-
"""
Created on Tue May 07 17:12:18 2019

@author: Dynaslope
"""

import analysis.querydb as qdb
import analysis.subsurface.filterdata as fsd
import pandas as pd
from datetime import datetime as dt
from datetime import timedelta as td
import memcache
import numpy as np
from sqlalchemy import create_engine
import time
import dynadb.db as db
import loggercount

def outlierf(dfl):
    if dfl.empty:
        return dfl
    dfl = dfl.groupby(['accel_id'])
    dfl = dfl.apply(fsd.resample_df)
    dfl = dfl.set_index('ts').groupby('accel_id').apply(fsd.outlier_filter)
    dfl = dfl.reset_index(level = ['ts'])
    dfl = dfl.reset_index(drop=True) 
    return(dfl)


def filter_counter(tsm_id = '', days_interval = 3):  
    time_now = pd.to_datetime(dt.now())
    from_time = time_now-td(days = days_interval)
    
    memc = memcache.Client(['127.0.0.1:11211'], debug=1)
    accelerometers=memc.get('DF_ACCELEROMETERS')
    accel = accelerometers[accelerometers.tsm_id ==tsm_id]

    df = qdb.get_raw_accel_data(tsm_id=tsm_id, from_time=from_time, batt =1)
    
    if not df.empty:
        count_raw = df['ts'].groupby(df.accel_id).size().rename('raw')
        
        dfv = fsd.volt_filter(df)
        count_volt = dfv['ts'].groupby(dfv.accel_id).size().rename('voltf')
        
        dfr = fsd.range_filter_accel(df)
        if not dfr.empty:
            count_range = dfr['ts'].groupby(dfr.accel_id).size().rename('rangef')
        else:
            count_range = pd.DataFrame(accel.accel_id).set_index('accel_id')
            count_range['rangef'] = 0
        
        dfo = fsd.orthogonal_filter(dfr)
        if not dfo.empty:
            count_ortho = dfo['ts'].groupby(dfo.accel_id).size().rename('orthof')
        else:
            count_ortho = pd.DataFrame(accel.accel_id).set_index('accel_id')
            count_ortho['orthof'] = 0
        
        dfor = outlierf(dfo)
        if not dfor.empty:
            count_outlier = dfor['ts'].groupby(dfor.accel_id).size().rename('outlierf')
        else:
            count_outlier = pd.DataFrame(accel.accel_id).set_index('accel_id')
            count_outlier['outlierf'] = 0
        
        dfa=pd.concat([count_raw,count_volt, count_range, count_ortho, count_outlier],axis=1)
#        dfa[np.isnan(dfa)]=0    
        
        dfa = dfa.reset_index()
    
    else:
        dfa = pd.DataFrame(columns = ['accel_id','raw','voltf','rangef','orthof','outlierf'])
        dfa = dfa.astype(float)
    
    dfa = pd.merge(accel[['accel_id']], dfa, how = 'outer', on = 'accel_id')
    dfa[(np.isnan(dfa))]=0
    dfa['ts'] = time_now
    
    dfa['percent_raw'] = dfa.raw / (48 * days_interval) * 100
    dfa['percent_voltf'] = dfa.voltf / dfa.raw * 100
    dfa['percent_rangef'] = dfa.rangef / dfa.raw * 100
    dfa['percent_orthof'] = dfa.orthof / dfa.raw * 100
    dfa['percent_outlierf'] = dfa.outlierf / dfa.raw *100
       
    dfa.ts = dfa.ts.dt.round('H')
    dfa = dfa.round(2)
    return dfa[['accel_id','ts','percent_raw','percent_voltf','percent_rangef','percent_orthof','percent_outlierf']]
def main():
    memc = memcache.Client(['127.0.0.1:11211'], debug=1)
    tsm_sensors=memc.get('DF_TSM_SENSORS')
    
    
    dffc = pd.DataFrame(columns = ['accel_id','ts','percent_raw','percent_voltf','percent_rangef','percent_orthof','percent_outlierf'])
    for i in tsm_sensors.tsm_id:
        print(i)
        dft = filter_counter(i)
        engine=create_engine('mysql+pymysql://root:senslope@127.0.0.1:3306/senslopedb', echo = False)
        dft.to_sql(name = 'data_counter', con = engine, if_exists = 'append', index = False)
        dffc = pd.concat([dffc,dft], ignore_index = True)

    query = ("SELECT l.logger_id, sms_msg FROM smsinbox_loggers "
             "inner join (SELECT mobile_id,logger_id from logger_mobile) as lm "
             "on lm.mobile_id = smsinbox_loggers.mobile_id "
             "inner join (SELECT logger_name, logger_id from commons_db.loggers) as l "
             "on lm.logger_id = l.logger_id "
             "where inbox_id >= (SELECT max(inbox_id)-10000 FROM smsinbox_loggers) "
             "and sms_msg like '%%no %%' or sms_msg like 'pow%%' "
             "and ts_sms >= DATE_SUB(Now(), interval 1 DAY) ")
#             "#group by l.logger_id")
    sms_stat = db.df_read(query, connection="gsm_pi")
    sms_stat.sms_msg = sms_stat.sms_msg.str.rstrip("*1234567890")   #remove timestamp
    sms_stat = sms_stat.groupby(['logger_id']).agg(lambda x: pd.Series.mode(x)[0]).reset_index()
    
    sms_summary = pd.merge(tsm_sensors[['tsm_id','tsm_name','logger_id']], sms_stat, how = 'left', on = 'logger_id')
    print (sms_summary)
#    engine=create_engine('mysql+pymysql://root:senslope@192.168.150.77:3306/senslopedb', echo = False)
    engine=create_engine('mysql+pymysql://root:senslope@127.0.0.1:3306/senslopedb', echo = False)
    sms_summary[['tsm_id','sms_msg']].to_sql(name = 'tsm_sms_stat', con = engine, if_exists = 'replace', index = False)
    
    loggercount.main()
if __name__ == "__main__":
    main()
#engine=create_engine('mysql+mysqlconnector://root:senslope@192.168.150.77:3306/senslopedb', echo = False)
#dffc.to_sql(name = 'data_counter', con = engine, if_exists = 'append', index = False)


#dffc = filter_counter(1)

