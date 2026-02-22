import pandas as pd

#read data from raw
df-Dec = pd.read_csv("/Users/lea/lakehouse-spark-sflake/data:raw/event-hist/2019-Dec.csv") 
df = pd.read_csv("/Users/lea/lakehouse-spark-sflake/data:raw/event-hist/2019-Nov.csv") 
df = pd.read_csv("/Users/lea/lakehouse-spark-sflake/data:raw/event-hist/2019-Oct.csv") 
df = pd.read_csv("/Users/lea/lakehouse-spark-sflake/data:raw/event-hist/2019-Feb.csv") 
df = pd.read_csv("/Users/lea/lakehouse-spark-sflake/data:raw/event-hist/2019-Jan.csv") 
print("Data read successfully from raw")
print("Dataframe info:")
print(df.info())
print(df.head())
