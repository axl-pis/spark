from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LoadSpotifyData") \
    .master("spark://spark-master:7077") \
    .getOrCreate()
print('builder')

csv_path= "/opt/spark/data/spotify_data.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True)
print('redear')

df.show()
print('show')

df.printSchema()

spark.stop()
