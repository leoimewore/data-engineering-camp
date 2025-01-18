import argparse
import os
from time import time
import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq 
import subprocess



def main(params):


    user=params.user
    password=params.password
    host = params.host
    port = params.port
    db = params.db
    table_name= params.table_name
    url= params.url

    csv_name='taxi_zone_lookup.csv'
    



    # os.system(f"wget {url} -O {csv_name}")
   

    # download the csv
    engine= create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


    df_iter = pd.read_csv(csv_name, iterator=True , chunksize=50)
    df= next(df_iter)

    df.head(n=0).to_sql(name= table_name , con=engine, if_exists='replace')
    df.to_sql(name= table_name , con=engine, if_exists='append')


    while True:

        try:

            t_start=time()
            df =next(df_iter)

            df.to_sql(name= table_name , con=engine, if_exists='append')

            t_end=time()

            print('inserted another chunk ....., took %.3f second' % (t_end-t_start))

        except StopIteration:
            print("Finished ingesting data into postgress database")
            break



if __name__ == '__main__':



    parser = argparse.ArgumentParser( description='Ingest CSV data from Postgres' )

    # user
    # password
    # host
    # port
    # database name
    # table name
    # url of the csv

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password' , help='password for postgres')
    parser.add_argument('--host' , help='host for postgres')
    parser.add_argument('--port' , help='port for postgres')
    parser.add_argument('--db' , help='database for postgres')
    parser.add_argument('--table_name' , help='table where we will write results')
    parser.add_argument('--url' , help='url of the csv file')


    args = parser.parse_args()



    main(args)