from pyspark.sql import functions as F, SparkSession, DataFrame
import itertools

class APriori:

	def __init__(self, df:DataFrame, s:int) -> None:
		"""Initializes the A Priori class. 
		
		Args:
			df (DataFrame): The data frame with all baskets (sorted sets). 
			s (int): The support. """

		self.df = df
		"""The data frame with all baskets (sorted sets). """
		self.s = s
		"""The support. """


	def _first_filter(self) -> DataFrame:
		"""Does the first step of filtering. 
		
		Returns:
			supported (DataFrame): The dataframe with only the sets with enough support. """

		items = self.df.select(F.explode('basket').alias('set')) # Each item in its own row

		item_lists = items.withColumn('set', F.array('set')) # Each item in its own list (set)
	
		support = item_lists.groupBy('set').count().withColumnRenamed('count', 'support') # Count each set
	
		supported = support.filter(F.col('support') >= self.s) # Filter based on how many times the set appears

		return supported


	def _frequent_sets(self, basket:list[int], potential_sets:list[list[int]]) -> list[list[int]]:
		"""Get the frequent sets in the basket. 
		
		Args:
			basket (list[int]): The sorted basket to check if the sets exist in. 
			potential_sets (list[list[int]]): The sorted sets we should check if they are in the basket. 
		
		Returns:
			frequent (list[list[int]]): The sets that are in the basket. """

		basket_set = set(basket)

		frequent = []

		for potential in potential_sets:
			potential_set = set(potential)
			if potential_set <= basket_set:
				frequent.append(potential)

		return frequent


def test_frequent_sets():

	apriori = APriori(None, None)

	assert apriori._frequent_sets([1, 3, 5, 6, 8], [[1, 2], [2, 4], [1, 5], [6, 8]]) == [[1, 5], [6, 8]], "frequent_sets not correct"
	assert apriori._frequent_sets([1, 3, 5, 6, 8], [[1, 2], [2, 4], [1, 2], [4, 8]]) == [], "Not correct when no match"
	assert apriori._frequent_sets([], [[1, 2], [2, 4], [1, 2], [4, 8]]) == [], "Not correct when empty basket"
	assert apriori._frequent_sets([1, 3, 5, 6, 8], []) == [], "Not correct when no potential set"


if __name__ == "__main__":
	test_frequent_sets()
	"""
	spark = SparkSession.builder.appName("test").getOrCreate()
	df = spark.sql('SELECT * FROM VALUES (ARRAY(1,2,3)), (ARRAY(2, 4)), (ARRAY(2, 3)) AS t(basket)')
	df.show()

	apriori = APriori(df=df, s=2)

	first = apriori._first_filter()

	first.show()

	#first.select(F.)

	first.crossJoin()
	F.array_contains()
	F.array_intersect()
	F.size()"""