from flask_table import Table, Col, LinkCol

class Results(Table):
    tsm_id = Col('tsm_id', show=False)
    tsm_name = Col('tsm_name')
    raw_status = Col('Raw Status')
    filter_status = Col('Filter Status')
    volt_status = Col('Volt Status')
    sms_msg = Col('SMS Status')
    decom_status = Col('Decommission')
    show = LinkCol('Show', 'show', url_kwargs=dict(name ='tsm_name'))
    
class all_data(Table):
    tsm_id = Col('tsm_id', show=False)
    tsm_name = Col('tsm_name')
    accel_id = Col('accel_id', show=False)
    node_id = Col('node_id')
#    in_use = Col('in_use')
    in_use = LinkCol('in_use', 'update_in_use_view', url_kwargs=dict(accel_id ='accel_id'), attr='in_use')
    percent_raw = Col('percent_raw')
    percent_voltf = Col('percent_voltf')
    percent_rangef = Col('percent_rangef')
    percent_orthof = Col('percent_orthof')
    percent_outlierf = Col('percent_outlierf')
    percent_valid = Col('percent_valid')
    status = Col('Status')
    remarks = Col('Remarks')
    recommendation = Col('recommendation')
    add = LinkCol('Tag', 'add_status_view', url_kwargs=dict(accel_id ='accel_id'))

class raw_data(Table):
    tsm_id = Col('tsm_id', show=False)
    tsm_name = Col('tsm_name')
    good_raw = Col('good_raw')
    bad_raw = Col('bad_raw')
    not_ok = Col('not_ok')
    percent_good_raw = Col('percent_good_raw')
    raw_status = Col('Raw Status')
    
#    show = LinkCol('Show', 'show', url_kwargs=dict(name ='tsm_name'))

class filter_data(Table):
    tsm_id = Col('tsm_id', show=False)
    tsm_name = Col('tsm_name')
    good_filtered = Col('good_filtered')
    bad_filtered = Col('bad_filtered')
    nan_filtered = Col('nan_filtered')
    to_tag_filtered = Col('to_tag_filtered')
    percent_good_filtered = Col('percent_good_filtered')
    filter_status = Col('Filter Status')

#    show = LinkCol('Show', 'show', url_kwargs=dict(name ='tsm_name'))

class volt_data(Table):
    tsm_id = Col('tsm_id', show=False)
    tsm_name = Col('tsm_name')
    good_volt = Col('good_volt')
    bad_volt = Col('bad_volt')
    nan_volt = Col('nan_volt')
    percent_good_volt = Col('percent_good_volt')
    volt_status = Col('Volt Status')
#    show = LinkCol('Show', 'show', url_kwargs=dict(name ='tsm_name'))
#    delete = LinkCol('Delete', 'delete_status', url_kwargs=dict(id='tsm_id'))
    
class status(Table):
    stat_id = Col('stat_id', show=False)
    accel_id = Col('accel_id')
    tsm_name = Col('tsm_name')
    node_id = Col('node_id')
    accel_number = Col('accel')
    ts_flag = Col('ts_flag')
    date_identified = Col('date_identified')
    status = Col('status', show = False)
    accel_status = Col('accel_status')
    remarks = Col('remarks')
    edit = LinkCol('Edit', 'edit_view', url_kwargs=dict(id='stat_id'))
    delete = LinkCol('Delete', 'delete_status', url_kwargs=dict(id='stat_id'))
	
	
class loggers(Table):
    logger_name = Col('Logger')
    has_rain = Col('has_rain')
    logger_type = Col('Logger Type')
    eval_count = Col('Data Count')
    eval_rain = Col('Rain Gauge')
    eval_batt = Col('Batt Status')
    eval_csq = Col('CSQ Status')
    eval_rssi = Col('RSSI Status')    