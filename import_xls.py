import pandas as pd
import numpy as np

import os

from config_db import *
import pandas.io.sql as psql
from sqlalchemy import create_engine, MetaData

import config_calendar as cal




# il calendario ha un solo foglio valido
def parse_excel_calendar (filename,sheetname):
    df1 = pd.read_excel(filename, sheetname = sheetname, skiprows=0, parse_cols=[0,1],keep_default_na=False )
    print df1['Foglio1']

def get_nrows(filename,sheetname):

    df = pd.read_excel(filename,sheetname = sheetname, skiprows=4, parse_cols=[1])
    return df.count()[0]

def add_TripType_to_df(dim_array,filename, sheetname):
    TripType = np.ndarray((dim_array,),dtype=(str,10))
    #print dim_array
    df_tt = pd.read_excel(filename, sheetname, parse_cols = 1)

    #df_tt = pd.read_excel(filename, sheetname, parse_cols=[column_number], skiprows=0)

    print df_tt
    #TripType[:] = df_tt.loc[0][0]
    #print TripType

def test(filename, sheetname):

    #print dim_array
    df_tt = pd.read_excel(filename, sheetname, parse_cols = [1], skip_blank_lines=False)
    print df_tt

def parse_excel_timetable (filename, sheetname, servizio, orario):
    df1 = pd.read_excel(filename, sheetname = sheetname, skiprows=5, parse_cols=[1,2],keep_default_na=False )
    df1 = df1.iloc[0:get_nrows(filename, sheetname)]
    #print get_nrows(filename, sheetname)
    df1[3] = pd.Series(add_TripType_to_df(get_nrows(filename, sheetname),2,filename,sheetname), index=df1.index)
    trip_id = 1
    df1[4] = pd.Series(trip_id, index=df1.index)
    print df1




def main():
    #dir_import='/home/franco/workspace/amat/linee_s/orari/linee_s/csv/'
    print '---------------------'
    dir = os.path.dirname(os.path.abspath(__file__)) +'/orari/tranvia'
    filename = os.path.join (dir,'test.xls')
    sheetname = 0
    servizio = 'LV'
    orario = 'INV'
    #parse_excel_calendar (filename, None)
    #get_nrows(filename, sheetname)

    #add_TripType_to_df (get_nrows(filename, sheetname),filename,sheetname)
    #parse_excel_timetable (filename, sheetname, servizio, orario)
    test(filename,sheetname)

if __name__ == "__main__":
    main()
