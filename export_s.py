from config_db import *
import psycopg2
import sys
import os
import csv




path_export = os.path.join (os.path.dirname(os.path.abspath(__file__)), 'output')

d={
               'linee_s.t_agency':'agency.csv',
               'linee_s.t_calendar':'calendar.csv',
               'linee_s.t_calendar_dates':'calendar_dates.csv',
               'linee_s.t_feed_info':'feed_info.csv',
               'linee_s.t_routes':'routes.csv',
               'linee_s.t_shapes':'shapes.csv',
               'linee_s.t_stop_times':'stop_times.csv',
               'linee_s.t_stop':'stops.csv',
               'linee_s.t_trips':'trips.csv'
              }

conn_string = "host=%s dbname=%s user=%s password=%s"  % (DB_HOST, DB_NAME, DB_USER, DB_PASS)




def export_from_postgres_to_transit ():

    con = None
    try:
        con = psycopg2.connect(conn_string)
        cur = con.cursor()

        print 'TRANSIT - Avvio export dei file'


        for table,out_file in d.iteritems():
            file_out = os.path.join (path_export,out_file)
            f_out=open(file_out, 'wb')
            cur.copy_expert ("COPY %s TO STDOUT with DELIMITER AS ',' CSV HEADER"  %(table), f_out)
            f_out.close
            f_in=open(file_out, 'r')
            reader = csv.reader(f_in)
            f_out=open(file_out.replace('.csv', '.txt'), 'wb')
            writer = csv.writer(f_out, delimiter=',', quoting=csv.QUOTE_ALL)
            writer.writerows([[y.decode('latin-1').encode('utf-8') for y in x] for x in reader])
            f_out.close
            f_in.close
            os.remove(file_out)

            print 'exported %s' %(table) + ' to ' + path_export+'/'+out_file


    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


    finally:

        if con:
            con.close()


export_from_postgres_to_transit ()


"""
            print 'TRANSIT - Avvio export dei file'


            for table,out_file in d.iteritems():
              sql="COPY %s TO STDOUT with DELIMITER AS ',' CSV HEADER" %(table)
              file_out = os.path.join(path,out_file)
              f_out=open(file_out, 'wb')
              self.c_q.esegui_export(sql, f_out)
              f_out.close

              f_in=open(file_out, 'r')
              reader = csv.reader(f_in)
              f_out=open(file_out.replace('.csv', '.txt'), 'wb')
              writer = csv.writer(f_out, delimiter=',', quoting=csv.QUOTE_ALL)
              writer.writerows([[y.decode('latin-1').encode('utf-8') for y in x] for x in reader])
              f_out.close
              f_in.close
              os.remove(file_out)
"""
