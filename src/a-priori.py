from pyspark.sql import functions as F, SparkSession, DataFrame

class APriori:

	def __init__(self, df:DataFrame) -> None:
		"""Initializes the A Priori class. """
		
		self.df = df


if __name__ == "__main__":
	spark = SparkSession.builder.appName("test").getOrCreate()
	df = spark.sql('SELECT * FROM VALUES (ARRAY(1,2,3)), (ARRAY(2, 4)), (ARRAY(2, 3)) AS t(baskets)')
	df.show()
	items = df.select(F.explode('baskets').alias('item'))
	items.show()

	support = items.groupBy('item').count().withColumnRenamed('count', 'support').sort(F.desc('support')).show()
