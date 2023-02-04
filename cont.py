# while True:
#     try:
#         t_start = time()

#         df = next(df_iter)

#         df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
#         df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

#         df.to_sql(name=table_name, con=engine, if_exists="append")

#         t_end = time()

#         print("inserted another chunk, took %.3f second" % (t_end - t_start))

#     except StopIteration:
#         print("Finished ingesting data into the postgres database")
#         break
##
##
