import pymysql
#from app import app
from tables import Results, all_data, raw_data, filter_data, volt_data, status, loggers
from db_config import mysql
from flask import Flask, flash, render_template, request, redirect, send_from_directory
from flask_socketio import SocketIO
from werkzeug import generate_password_hash, check_password_hash
import socket
import evaluation
import eval_loggers
import pandas as pd
import filtercounter as fc
import memcache
import os
import time
import analysis.querydb as qdb
import volatile.init as init
import dynadb.db as db

app = Flask(__name__)
app.config['SECRET_KEY'] = 'vnkdjnfjknfl1232#'
socketio = SocketIO(app)



@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                          'favicon.ico',mimetype='image/vnd.microsoft.icon')

@app.route('/new_status')
def add_status_view():
    id = request.args.get('accel_id', default='', type=int)
    print (id)
    return render_template('add.html',
#        data=[{'status':'Ok'}, {'status':'Use with Caution'}, {'status':'Special Case'}, {'status':'Not Ok'}])
        data=[{'status':'Ok', 'stat':1}, {'status':'Use with Caution', 'stat':2}, {'status':'Special Case', 'stat':3}, {'status':'Not Ok', 'stat':4}],
        name = [{'name': 'Kate Justine Flores'},{'name': 'Brainerd Cruz'},{'name': 'Kennex Razon'}],
        accel_id = id)

@app.route('/index')
def index():
    return render_template(
        'index.html',
        data=[{'name':'red'}, {'name':'green'}, {'name':'blue'}])
    
@app.route("/test" , methods=['GET', 'POST'])
def test():
    select = request.form.get('comp_select')
    return(str(select)) # just to see what select is
    

@app.route('/add', methods=['POST'])
def add_status():
    try:  
        accel_id = request.form['input_accel_id']
        date_identified = request.form['input_date_identified']
        status = request.form['input_status']
        remarks = request.form['input_remarks']
        flagger = request.form['input_flagger']
        
#        if status_text == "Ok":
#            status = 1
#        elif status_text == "Use with Caution"
        # validate the received values
        if accel_id and date_identified and status and flagger and request.method == 'POST':
            #do not save password as a plain text
            
            # save edits
            sql = ("INSERT INTO accelerometer_status(accel_id, ts_flag, "
                   "date_identified, flagger, status, remarks) VALUES('{}', "
                   "NOW(), '{}', '{}', '{}', '{}')".format(accel_id, date_identified, flagger, status, remarks))
#            print(sql)
#            data = (accel_id, date_identified, flagger, status, remarks)
#            conn = mysql.connect()
#            cursor = conn.cursor()
#            cursor.execute(sql, data)
#            conn.commit()
#            qdb.execute_query(sql)
            db.write(sql,connection = 'analysis')
            
            flash('Status added successfully!')
            return redirect('/')
        else:
            return 'Error while adding status'
    except Exception as e:
        print(e)
#    finally:
#        cursor.close() 
#        conn.close()
  
@app.route('/')
def users():
    global df_summary, df_count, df_raw, df_filter, df_volt 
    
    
    
    try:
        #get latest ts
        conn = mysql.connect()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("SELECT max(ts) as tsupdate FROM data_counter ")
        ts = cursor.fetchone()
        
        
        df_summary, df_count, df_raw, df_filter, df_volt = evaluation.evaluate()
        summary = df_summary.to_dict('r')
        table = Results(summary)
        table.border = True
        return render_template('summary.html', table=table, tsupdate = ts['tsupdate'], title = "Sensors")#rows.to_html(index=False))
    except Exception as e:
        print(e)
#    finally:
#        cursor.close() 
#        conn.close()


@app.route('/loggers')
def loggers_view():
    global df_summary, df_count, df_raw, df_filter, df_volt 
    
    
    
    try:
        #get latest ts
        conn = mysql.connect()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("SELECT max(ts) as tsupdate FROM logger_counter ")
        ts = cursor.fetchone()
        
        
        df_logger = eval_loggers.evaluate()
        summary = df_logger.to_dict('r')
        table = loggers(summary)
        table.border = True
        return render_template('summary.html', table=table, tsupdate = ts['tsupdate'], title = "Loggers")#rows.to_html(index=False))
    except Exception as e:
        print(e)
        
@app.route("/<name>" )
def show(name):
    global df_summary, df_raw, df_filter, df_volt
    
    print (name)
    
#    summary = df_summary[df_summary.tsm_name == name]
#    summary = summary.to_dict('r')
    
    raw = df_raw[df_raw.tsm_name == name]
    raw = raw.to_dict('r')
    raw = raw_data(raw)
    raw.border = True
    
    count = df_count[(df_count.tsm_name == name)& (df_count.accel_number == 1)]
    count = count.to_dict('r')
    count = all_data(count)
    count.border = True    
    
    if len(name)==5:
        count2 = df_count[(df_count.tsm_name == name)& (df_count.accel_number == 2)]
        count2 = count2.to_dict('r')
        count2 = all_data(count2)
        count2.border = True    
    else:
        count2 = ""
    
    filtered = df_filter[df_filter.tsm_name == name]
    filtered = filtered.to_dict('r')
    filtered = filter_data(raw)
    filtered.border = True
    
    volt = df_volt[df_volt.tsm_name == name]
    volt = volt.to_dict('r')
    volt = volt_data(raw)
    volt.border = True    
    
    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT max(ts) as tsupdate FROM data_counter ")
    ts = cursor.fetchone()
    
    return render_template(
    'summary.html',
    table = count ,table2 = count2  , tsupdate = ts['tsupdate'], flag = 1)

@app.route('/loading')
def update_datacounter():
    try:  

            
        fc.main()
#        time.sleep(10)

        flash('data counter updated successfully!')
        return redirect('/')
#        else:
#            return 'Error while updating status'
    except Exception as e:
        print(e)
#    finally:
#        cursor.close() 
#        conn.close()

@app.route('/status')
def view_status():
    try:
        conn = mysql.connect()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        query = ("SELECT stat_id, accelerometer_status.accel_id, tsm_name, "
                       "node_id, accel_number, ts_flag, date_identified, status, "
                       "IF(status=1,'Ok', IF(status=2,'Use with Caution', "
                       "IF(status=3,'Special Case', IF(status=4,'Not Ok', NULL)))) "
                       "as accel_status, remarks FROM accelerometer_status "
                       "inner join accelerometers on "
                       "accelerometer_status.accel_id = accelerometers.accel_id "
                       "inner join tsm_sensors on accelerometers.tsm_id = tsm_sensors.tsm_id "
                       "order by tsm_name, node_id, accel_number")
#        rows = cursor.fetchall()
        
        rows = db.df_read(query, connection="analysis")
        rows = rows.to_dict('r')
        
        table = status(rows)
        table.border = True
        return render_template('summary.html', table=table, tsupdate='')
    except Exception as e:
        print(e)
    finally:
        cursor.close() 
        conn.close()

@app.route('/edit/<int:id>')
def edit_view(id):
    print (int(id))
    try:
#        conn = mysql.connect()
#        cursor = conn.cursor(pymysql.cursors.DictCursor)
        query = "SELECT * FROM accelerometer_status WHERE stat_id={}".format(id)
#        row = cursor.fetchone()
        row = db.df_read(query, connection="analysis")
#        print "###################################################"
#        print id
#        print row
        row = row.to_dict('r')[0]
        if row:
            print ("nagana")
            print(row)
            return render_template('edit.html', row=row,
                                   data=[{'status':'Ok', 'stat':1}, {'status':'Use with Caution', 'stat':2}, {'status':'Special Case', 'stat':3}, {'status':'Not Ok', 'stat':4}],
                                   name = [{'name': 'Kate Justine Flores'},{'name': 'Brainerd Cruz'},{'name': 'Kennex Razon'}])
        
        else:
            return 'Error loading #{id}'.format(id=id)
    except Exception as e:
        print(e)
#    finally:
#        cursor.close()
#        conn.close()

@app.route('/update_in_use')
def update_in_use_view():
    memc = memcache.Client(['127.0.0.1:11211'], debug=1)
    tsm_sensors = memc.get('DF_TSM_SENSORS')
    
    id = request.args.get('accel_id', default='', type=int)
    print (id)
    active_tsm = tsm_sensors[['tsm_id','tsm_name']][pd.isnull(tsm_sensors.date_deactivated)].sort_values(by='tsm_name')
    active_tsm = active_tsm.to_dict('r')
    
    if id:
        try:
    #        conn = mysql.connect()
    #        cursor = conn.cursor(pymysql.cursors.DictCursor)
            query = ("SELECT * FROM accelerometers "
                     "where tsm_id = (select tsm_id FROM accelerometers "
                     "where accel_id = {}) "
                     "and node_id = (select node_id FROM accelerometers "
                     "where accel_id = {})".format(id,id))
    #        row = cursor.fetchone()
            row = db.df_read(query, connection="analysis")
            row = row.to_dict('r')
            if row:
                print ("nagana in_use")
                print(row)
                return render_template('update_inuse.html', row=row,active_tsm = active_tsm)
    #                                   data=[{'status':'Ok', 'stat':1}, {'status':'Use with Caution', 'stat':2}, {'status':'Special Case', 'stat':3}, {'status':'Not Ok', 'stat':4}],
    #                                   name = [{'name': 'Kate Justine Flores'},{'name': 'Brainerd Cruz'},{'name': 'Kennex Razon'}])
            else:
                return 'Error loading #{id}'.format(id=id)
        except Exception as e:
            print(e)
    else:
        return render_template('update_inuse.html', 
                               row=[{'accel_number': '1','in_use':'1'},{'accel_number': '2','in_use':'0'}],
                               active_tsm = active_tsm)
        print("bago")
        
        
@app.route('/update', methods=['POST'])
def update_status():
    try:  
        accel_id = request.form['input_accel_id']
        date_identified = request.form['input_date_identified']
        status = request.form['input_status']
        remarks = request.form['input_remarks']
        flagger = request.form['input_flagger']
        stat_id = request.form['id']
        # validate the received values
        if accel_id and date_identified and status and flagger and request.method == 'POST':
            #do not save password as a plain text
#            _hashed_password = generate_password_hash(_password)
            # save edits
#            sql = "UPDATE tbl_user SET user_name=%s, user_email=%s, user_password=%s WHERE user_id=%s"
            sql = ("UPDATE accelerometer_status SET accel_id = '{}', "
                   "ts_flag = NOW(), date_identified = '{}', flagger='{}', "
                   "status='{}', remarks='{}' WHERE stat_id = '{}'".format(accel_id, date_identified, flagger, status, remarks, stat_id))
#            conn = mysql.connect()
#            cursor = conn.cursor()
#            cursor.execute(sql, data)
#            conn.commit()
#            qdb.execute_query(sql)
            db.write(sql,connection = 'analysis')
            flash('Status updated successfully!')
            return redirect('/')
        else:
            return 'Error while updating status'
    except Exception as e:
        flash('Status updated ERROR!')
        return redirect('/')
        print(e)
#    finally:
#        cursor.close() 
#        conn.close()
        
@app.route('/updating_in_use', methods=['POST'])
def update_in_use():
    print('verygood')
    try:  
        tsm_id = request.form['input_tsm_id']
        node_id = request.form['input_node_id']
        accel_inuse = request.form['input_accel_inuse']
        

#        date_identified = request.form['input_date_identified']
#        status = request.form['input_status']
#        remarks = request.form['input_remarks']
#        flagger = request.form['input_flagger']
#        stat_id = request.form['id']
        # validate the received values
        if tsm_id and request.method == 'POST':
            query = ("SELECT * FROM accelerometers "
                     "where tsm_id = '{}' and node_id = '{}'".format(tsm_id,node_id))
            accel = db.df_read(query, connection="analysis")
            
            for i in accel.accel_number:
                print (i)
                if int(accel_inuse) == i:
                    in_use = 1
                else:
                    in_use = 0
                
                update_query = ("UPDATE `accelerometers` "
                                "SET `in_use`='{}', `ts_updated` = NOW() WHERE `tsm_id`='{}' and "
                                "`node_id`='{}' and `accel_number` = '{}';".format(in_use, tsm_id, node_id, i))
                print (update_query)
#                qdb.execute_query(update_query)
                db.write(update_query,connection = 'analysis')
#            #do not save password as a plain text
##            _hashed_password = generate_password_hash(_password)
#            # save edits
##            sql = "UPDATE tbl_user SET user_name=%s, user_email=%s, user_password=%s WHERE user_id=%s"
#            sql = ("UPDATE accelerometer_status SET accel_id = '{}', "
#                   "ts_flag = NOW(), date_identified = '{}', flagger='{}', "
#                   "status='{}', remarks='{}' WHERE stat_id = '{}'".format(accel_id, date_identified, flagger, status, remarks, stat_id))
##            conn = mysql.connect()
##            cursor = conn.cursor()
##            cursor.execute(sql, data)
##            conn.commit()
#            qdb.execute_query(sql)
            init.main()
            flash('accel switched successfully!')
            return redirect('/')
        else:
            return 'Error while updating status'
    except Exception as e:
        print(e)
        flash('accel switched ERROR!')
        return redirect('/')
  
@app.route('/delete/<int:id>')
def delete_status(id):
    try:
#        conn = mysql.connect()
#        cursor = conn.cursor()
        sql = "DELETE FROM accelerometer_status WHERE stat_id={}".format(id)
#        qdb.execute_query(sql)
        db.write(sql,connection = 'analysis')
        flash('User deleted successfully!')
        return redirect('/')
    except Exception as e:
        print(e)
#    finally:
#        cursor.close() 
#        conn.close()
  
if __name__ == "__main__":
#    ip = socket.gethostbyname(socket.gethostname())
#    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#    s.connect(("8.8.8.8", 80))
#    ip = s.getsockname()[0]
#    print(ip)
    socketio.run(app, host= "0.0.0.0")
    app.run(port=5000,debug=True)