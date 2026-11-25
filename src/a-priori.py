from pyspark.sql import functions as F, SparkSession, DataFrame

class APriori:

	def __init__(self, df:DataFrame, s:int) -> None:
		"""Initializes the A Priori class. 
		
		Args:
			df (DataFrame): The data frame with all baskets. 
			s (int): The support. """
		
		self.df = df
		"""The data frame with all baskets. """
		self.s = s
		"""The support. """


	def _first_filter(self) -> DataFrame:
		"""Does the first step of filtering. 
		
		Returns:
			supported (DataFrame): The dataframe with only the sets with enough support. """

		items = self.df.select(F.explode('baskets').alias('set')) # Each item in its own row

		item_lists = items.withColumn('set', F.array('set')) # Each item in its own list (set)
	
		support = item_lists.groupBy('set').count().withColumnRenamed('count', 'support') # Count each set
	
		supported = support.filter(F.col('support') >= self.s) # Filter based on how many times the set appears

		return supported


if __name__ == "__main__":
	spark = SparkSession.builder.appName("test").getOrCreate()
	df = spark.sql('SELECT * FROM VALUES (ARRAY(1,2,3)), (ARRAY(2, 4)), (ARRAY(2, 3)) AS t(baskets)')
	df.show()

	apriori = APriori(df=df, s=2)

	first = apriori._first_filter()

	first.show()