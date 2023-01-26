import os
import argparse
import pandas as pd
from urllib.parse import quote
from sqlalchemy import create_engine
from time import time
from os import path
import warnings
warnings.filterwarnings(action='ignore')

def main(params):

    if params.url.endswith('.csv.gz'):
        csv_path = 'output.csv.gz'
    else:
        csv_path = 'output.csv'
    if path.exists(csv_path):
        print('File',csv_path,'already exist')
    else:
        os.system(f"wget {params.url} -O {csv_path}")

    # create db connection
    engine = create_engine(
            f'postgresql://%s:{params.user}@{params.host}:{params.port}/{params.db}' % quote(params.pswd)
        )
    engine.connect()
    print(engine)

    # read file & chunk it
    df_iter = pd.read_csv(csv_path, iterator=True, chunksize=200000)
    print('Ingesting to database...')

    # ingest to db
    i = 1
    while True:
        try:
            t_start = time()

            # read chunked data
            df = next(df_iter)

            # change datatype
            try:
                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            except:
                try:
                    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
                    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
                except Exception as e:
                    print(e)

            # insert
            df.to_sql(name=params.table_name, con=engine, if_exists='append', index=False)
            del df

            print(f'--({i}) inserted chunk, took %.3f second' % (time() - t_start))
            i = i + 1
        except:
            print('--End of the loop')
            break

    # checking
    query = f"""
            SELECT COUNT(*) FROM public.{params.table_name}
            """
    n = pd.read_sql(query, con=engine)['count'][0]
    print(f'There are {n} record(s) in the database')

if __name__ == '__main__':
    # create argument parser
    parser = argparse.ArgumentParser(description='Ingest csv data to Postgres')

    # db con parameters
    parser.add_argument('--user', help='username:')
    parser.add_argument('--pswd', help='password:')
    parser.add_argument('--host', help='postgres host:')
    parser.add_argument('--port', help='postgres port:')
    parser.add_argument('--db', help='database:')
    parser.add_argument('--table_name', help='table:')
    parser.add_argument('--url', required=False, help='csv file url:')
    
    args = parser.parse_args()
    main(args)
