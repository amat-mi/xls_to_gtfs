import pandas as pd
import numpy as np

import pandas.io.sql as psql
from sqlalchemy import create_engine, MetaData
#from sqlalchemy.pool import NullPool
from config_db import *

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
    id_corsa=1
    df1[8] = pd.Series(id_corsa, index=df1.index)

    for i in range(7,len (pd.read_csv(filename,sep).columns)):
        id_corsa+=1
        #print id_corsa
        df2 =pd.read_csv(filename,sep,usecols=[0,1,2,3,4,5,i],names =[0,1,2,3,4,5,6] ,skiprows=1,nrows=l_df-1,dtype={0: np.str, 1: np.str, 2: np.str, 3: np.str,4: np.str, 5: np.str,6: np.str} )
        df2[7] = pd.Series(add_validity_to_df(l_df,[i],filename,';'), index=df2.index)
        df2[8] = pd.Series(id_corsa, index=df2.index)
        df_c=pd.concat([df1, df2], ignore_index=True)
        df1=df_c
    df1 = df1.where((pd.notnull(df1)), None) ###replace Nan with null

    return df1


def write_to_db(dataframe,table_name):
    engine = create_engine(r'postgresql://%s:%s@%s/%s' % (DB_USER, DB_PASS, DB_HOST, DB_NAME))
    dataframe.to_sql(table_name,engine,schema=DB_SCHEMA,if_exists='append')



def main():

    dir_import='/home/franco/workspace/amat/linee_s/orari/csv/'

    for i in os.listdir(dir_import):
        print i
        write_to_db(parse_csv(os.path.join(dir_import,i), ';'),'imp_orario')
        print os.path.join(dir_import,i) + '      imported to postgres'



if __name__ == "__main__":
    main()
