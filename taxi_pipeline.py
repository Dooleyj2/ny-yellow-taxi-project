import requests
import csv
import pandas as pd
from sqlalchemy import create_engine
from time import time

import luigi
import luigi.contrib.postgres

import pyarrow.csv as pv
import pyarrow.parquet as pq
from io import StringIO

class extract_data(luigi.Task):

	def run(self):

		host = "localhost"
		user = "root"
		password = "root"
		port = 5432
		db = "ny_taxi"
		table = "ny_taxi_yellow"


		url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"

		engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
		
		with requests.Session() as s:
			download = s.get(url)

		decoded_content = download.content.decode('utf-8')
		text = StringIO(decoded_content)
		df_iter = pd.read_csv(text, iterator=True, chunksize=10000)
		df = next(df_iter)

		df.head(n=0).to_sql(name=table, con=engine, if_exists='replace')
		df.to_sql(name=table, con=engine, if_exists='append')

		try:
			while True:

				t_start = time()

				df = next(df_iter)
				df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
				df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

				df.to_sql(name=table, con=engine, if_exists='append')
				t_end = time()

				print('insert another chunk, took %.3f seconds' % (t_end - t_start))
		except StopIteration:
			pass

	def output(self):
		return(luigi.contrib.postgres.PostgresTarget("localhost:5432","ny_taxi", "root", "root", "ny_taxi_yellow", 1))




	
