import pandas as pd
import numpy as np

import pandas.io.sql as psql
from sqlalchemy import create_engine, MetaData
#from sqlalchemy.pool import NullPool
from config_db import *

import fnmatch
import os



def add_validity_to_df(dim_array,column_number,filename,sep):
    validity=np.ndarray((dim_array-1,),dtype=(str,10))
    df_val = pd.read_csv(filename,sep,usecols=column_number,names =[0])
    validity[:(dim_array-1)/2] = df_val.loc[0]
    validity[(dim_array-1)/2:] = df_val.loc[dim_array]
    return validity


def parse_csv(filename, sep):
    l_df=len (pd.read_csv(filename,sep))
    df1=pd.read_csv(filename,sep,usecols=[0,1,2,3,4,5,6], names =[0,1,2,3,4,5,6],skiprows=1,nrows=l_df-1,dtype={0: np.str, 1: np.str, 2: np.str, 3: np.str,4: np.str, 5: np.str,6: np.str} )
    df1[7] = pd.Series(add_validity_to_df(l_df,[6],filename,';'), index=df1.index)
    trip_id=1
    df1[8] = pd.Series(trip_id, index=df1.index)

    for i in range(7,len (pd.read_csv(filename,sep).columns)):
        trip_id+=1
        #print trip_id
        df2 =pd.read_csv(filename,sep,usecols=[0,1,2,3,4,5,i],names =[0,1,2,3,4,5,6] ,skiprows=1,nrows=l_df-1,dtype={0: np.str, 1: np.str, 2: np.str, 3: np.str,4: np.str, 5: np.str,6: np.str} )
        df2[7] = pd.Series(add_validity_to_df(l_df,[i],filename,';'), index=df2.index)
        df2[8] = pd.Series(trip_id, index=df2.index)
        df_c=pd.concat([df1, df2], ignore_index=True)
        df1=df_c
    df1 = df1.where((pd.notnull(df1)), None) ###replace Nan with null
    return df1


def get_nrows(filename,sheetname):
    df = pd.read_excel(filename,sheetname = sheetname, skiprows=4, parse_cols=[1])
    return df.count()[0]

def add_trip_typ_to_df(dim_array,column_number,filename, sheetname):
    validity = np.ndarray((dim_array,),dtype=(str,10))
    df_val = pd.read_excel(filename, sheetname, parse_cols=[column_number],skiprows=3)
    #print df_val
    validity[:] = df_val.loc[0][0]
    return validity


def parse_excel_timetable_a (filename, sheetname, servizio, orario):

    if filetype == 'calendar':
        df1 = pd.read_excel(filename, sheetname = sheetname, skiprows=1, parse_cols=[0,16,17],keep_default_na=False )
        return df1
    elif filetype == 'timetable':
        df1 = pd.read_excel(filename, sheetname = sheetname, skiprows=5, parse_cols=[1,2],keep_default_na=False )
        df1 = df1.iloc[0:get_nrows(filename, sheetname)]
        #print get_nrows(filename, sheetname)
        df1[3] = pd.Series(add_trip_typ_to_df(get_nrows(filename, sheetname),2,filename,sheetname), index=df1.index)
        trip_id = 1
        df1[4] = pd.Series(trip_id, index=df1.index)
        for i in range (3, len(pd.read_excel(filename,sheetname = sheetname,skiprows=4).columns)):
            df2 = pd.read_excel(filename, sheetname = sheetname, skiprows=5, parse_cols=[1,i], keep_default_na=False)
            df2 = df2.iloc[0:get_nrows(filename, sheetname)]
            trip_id += 1
            df2[3] = pd.Series(add_trip_typ_to_df(get_nrows(filename, sheetname),i,filename,sheetname), index=df2.index)
            df2[4] = pd.Series(trip_id, index=df2.index)
            df_c = pd.concat([df1, df2], ignore_index=True)
            df1=df_c
        df1 = df1.rename(columns={ 'Unnamed: 0' : 'id_fermata', 'Unnamed: 1' : 'ora', 3 :'corsa_tipo', 4 : 'corsa_id'})
        df1 ['servizio'] = servizio
        df1 ['orario'] = orario
        return df1


def parse_excel_timetable ( filename, sheetname, servizio, orario):
    df1 = pd.read_excel(filename, sheetname = sheetname, skiprows=5, parse_cols=[1,2],keep_default_na=False )
    df1 = df1.iloc[0:get_nrows(filename, sheetname)]
    #print get_nrows(filename, sheetname)
    df1[3] = pd.Series(add_trip_typ_to_df(get_nrows(filename, sheetname),2,filename,sheetname), index=df1.index)
    trip_id = 1
    df1[4] = pd.Series(trip_id, index=df1.index)
    for i in range (3, len(pd.read_excel(filename,sheetname = sheetname,skiprows=4).columns)):
        df2 = pd.read_excel(filename, sheetname = sheetname, skiprows=5, parse_cols=[1,i], keep_default_na=False)
        df2 = df2.iloc[0:get_nrows(filename, sheetname)]
        trip_id += 1
        df2[3] = pd.Series(add_trip_typ_to_df(get_nrows(filename, sheetname),i,filename,sheetname), index=df2.index)
        df2[4] = pd.Series(trip_id, index=df2.index)
        df_c = pd.concat([df1, df2], ignore_index=True)
        df1=df_c
    df1 = df1.rename(columns={ 'Unnamed: 0' : 'id_fermata', 'Unnamed: 1' : 'ora', 3 :'corsa_tipo', 4 : 'corsa_id'})
    df1 ['servizio'] = servizio
    df1 ['orario'] = orario
    print df1


def parse_excel_timetable_t ( filename, servizio, orario):
    df1 = pd.read_excel(filename, sheetname = None) #, skiprows=5, parse_cols=[1,2],keep_default_na=False )
    for key, value in df1.iteritems() :
        print key
    #print df1.keys

def parse_excel_timetable_sheet ( filename,  servizio, orario):
    df1 = pd.read_excel(filename) #, skiprows=5, parse_cols=[1,2],keep_default_na=False )
    a=[]
    for key in df1.iteritems() :
        print key
        sheetname = key
        df1 = pd.read_excel(filename, sheetname = sheetname, skiprows=5, parse_cols=[1,2],keep_default_na=False )
        df1 = df1.iloc[0:get_nrows(filename, sheetname)]
        #print get_nrows(filename, sheetname)
        df1[3] = pd.Series(add_trip_typ_to_df(get_nrows(filename, sheetname),2,filename,sheetname), index=df1.index)
        trip_id = 1
        df1[4] = pd.Series(trip_id, index=df1.index)
        for i in range (3, len(pd.read_excel(filename,sheetname = sheetname,skiprows=4).columns)):
            df2 = pd.read_excel(filename, sheetname = sheetname, skiprows=5, parse_cols=[1,i], keep_default_na=False)
            df2 = df2.iloc[0:get_nrows(filename, sheetname)]
            trip_id += 1
            df2[3] = pd.Series(add_trip_typ_to_df(get_nrows(filename, sheetname),i,filename,sheetname), index=df2.index)
            df2[4] = pd.Series(trip_id, index=df2.index)
            df_c = pd.concat([df1, df2], ignore_index=True)
            df1=df_c
        df1 = df1.rename(columns={ 'Unnamed: 0' : 'id_fermata', 'Unnamed: 1' : 'ora', 3 :'corsa_tipo', 4 : 'corsa_id'})
        df1 ['servizio'] = servizio
        df1 ['orario'] = orario
        a.append(df1)
    for df in a:
        dfa = pd.concat (df)

    print dfa



def parse_excel_calendar (filename,sheetname):
    df1 = pd.read_excel(filename, sheetname = sheetname, skiprows=1, parse_cols=[0,16,17],keep_default_na=False )
    return df1


def parse_import_dir (dir):
    a = []
    for i in os.listdir(dir):
        #print (i.rsplit('_')[0]).rsplit('-')[0]
        dict = {}
        dict ["filename"] = os.path.join (dir,i)
        dict ["filetype"] = os.path.splitext(i)[0].rsplit('_')[0]
        if '_' in i:
            dict ["orario"] = os.path.splitext(i)[0].rsplit('_')[1]
            dict ["servizio"] = os.path.splitext(i)[0].rsplit('_')[2]
            a.append(dict)
        a.append(dict)
    return a



def write_to_db(dataframe,table_name):
    engine = create_engine(r'postgresql://%s:%s@%s/%s' % (DB_USER, DB_PASS, DB_HOST, DB_NAME))
    dataframe.to_sql(table_name,engine,schema=DB_SCHEMA,if_exists='append')



def main():
    #dir_import='/home/franco/workspace/amat/linee_s/orari/linee_s/csv/'
    print '---------------------'
    dir = os.path.dirname(os.path.abspath(__file__)) +'/orari/tranvia'
    print parse_import_dir (dir)
    filename = os.path.join (dir,'orario_LV_INV.xls')
    parse_excel_timetable (filename, 0, 'LV','INT')
    #parse_excel_timetable_t ( filename, 'a', 'b')

    #for i in os.listdir(dir):
    #    print i
"""
        filename = os.path.join (dir,i)
        print '+++++++++++++++++++++++++' + filename
        if 'Calend' in i:
            print 'processing  ' + filename
            #write_to_db(parse_excel_calendar (filename,1), imp_t_calendario)
            continue
        if 'LV-INT' in i:
            print 'processing  ' + filename
            #print parse_excel_timetable_t (filename,'LV','INT')
            #parse_excel_timetable (filename,0,'LV','INT')
            #df = pd.concat([parse_excel_timetable (filename,0,'LV','INT') , parse_excel_timetable (filename,1,'LV','INT')], ignore_index=True)

            continue
        if 'LV-INV' in i:
            print 'processing  ' + filename
            df = pd.concat([parse_excel_timetable (filename,0,'LV','INV') , parse_excel_timetable (filename,1,'LV','INV')], ignore_index=True)
            #print df
            continue
        if 'S-INV' in i:
            print 'processing  ' + filename
            df = pd.concat([parse_excel_timetable (filename,0,'S','INV') , parse_excel_timetable (filename,1,'S','INV')], ignore_index=True)
            #print df
            continue
        if 'F-INV' in i:
            print 'processing  ' + filename
            df = pd.concat([parse_excel_timetable (filename,0,'F','INV') , parse_excel_timetable (filename,1,'F','INV')], ignore_index=True)
            #print df
            continue
        else:
            #print '+++++danger++++++' + filename + '   anomalo'
            continue
"""


    #filename = os.path.join (dir,'T9-LV-Invernale 2015.xls')
    #filename = os.path.join (dir,'c_2016.xlsx')
    #parse_excel_calendar (filename,1)

    #print 'processing  ' + filename
    #df = pd.concat([parse_excel_timetable (filename,0) , parse_excel_timetable (filename,1)], ignore_index=True)
    #write_to_db(df,'imp_t_corse')
    #print filename + '  imported to postgres'




##### import per linee_s
    #for i in os.listdir(dir_import):
        #parse_csv(os.path.join(dir_import,i), ';')
        #write_to_db(parse_csv(os.path.join(dir_import,i), ';'),'imp_orario')
        #print os.path.join(dir_import,i) + '      imported to postgres'



if __name__ == "__main__":
    main()
