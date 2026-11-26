from __future__ import annotations
from typing import TYPE_CHECKING

from pyspark.sql import functions as F, SparkSession, DataFrame
from pyspark.sql.types import IntegerType, ArrayType
import itertools

if TYPE_CHECKING:
	from pyspark.sql._typing import UserDefinedFunctionLike  # pyright: ignore[reportMissingModuleSource]

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


	def apriori_algorithm(self, prints:bool=False) -> list[DataFrame]:
		"""Get the dataframes of the supported sets at each number of elements in increasing order. 
		
		Returns:
			df_list (list[DataFrame]): List of the dataframes with the supported sets. """

		df_list = []

		items = self.df.select(F.explode('basket').alias('set')) # Each item in its own row
		
		set_list = items.withColumn('set', F.array('set')) # Each item in its own list (set)

		supported_set = self._scan(potential_sets=set_list)

		n_supported = supported_set.count()

		df_list.append(supported_set)

		cycle_num = 2
		while 0 < n_supported:

			if prints: print(f"Started cycle number {cycle_num}")
			
			potential_set = self._permute_increasing(df=supported_set, show=False)

			supported_set = self._scan(potential_sets=potential_set)

			n_supported = supported_set.count()
			
			df_list.append(supported_set)

			cycle_num += 1

		df_list.pop()

		return df_list
			


	def _scan(self, potential_sets:DataFrame) -> DataFrame:
		"""Filters out the relevant sets based on the potential sets and the original data. 
				
		Args:
			potential_sets (DataFrame): Distinct potential sets. 
		
		Returns:
			supported_sets (DataFrame): The sets with enough support. """

		udf_frequent_sets = self._udf_subsets(potential_sets=potential_sets)

		subsets = self.df.withColumn('subsets', udf_frequent_sets(F.col('basket')))

		set_list = subsets.select(F.explode('subsets').alias('set')) # Each item in its own row

		support = set_list.groupBy('set').count().withColumnRenamed('count', 'support') # Count each set
			
		supported = support.filter(F.col('support') >= self.s) # Filter based on how many times the set appears

		supported_sets = supported.drop('support')

		return supported_sets


	def _udf_subsets(self, potential_sets:DataFrame) -> UserDefinedFunctionLike:
		"""
		
		Args:
			potential_sets (DataFrame): Distinct potential sets. 
		
		Returns:
			udf (UserDefinedFunctionLike): User defined function to compute the sets in each basket. """

		potential_tuples = {tuple(potential_list[0]) for potential_list in potential_sets.toLocalIterator()}

		def _subsets(basket:list[int]) -> list[list[int]]:
			"""Get the frequent sets in the basket. 
			
			Args:
				basket (list[int]): The sorted basket to check if the sets exist in.
			
			Returns:
				frequent_sets (list[list[int]]): The sets that are in the basket. """

			if len(potential_tuples) == 0:
				return []
						
			set_len = len(next(iter(potential_tuples)))

			basket_tuple_set = set(itertools.combinations(basket, set_len))

			frequent_tuples = basket_tuple_set.intersection(potential_tuples)

			frequent_lists = [list(frequent) for frequent in frequent_tuples]

			return frequent_lists

		return F.udf(_subsets, ArrayType(ArrayType(IntegerType())))


	def _permute_increasing(self, df: DataFrame, show: bool = True):
		"""Takes a one column dataframe of set/lists and returns a dataframe with permutations of these with one more length.\\
			Eg: [{A, B}, {B, C}, {C, A}] -> [{A, B, C}]\\
			Expecting the column with sets to be named 'sets'."""
		k = df.select(F.size("set")).first()[0]+1
		required_pairs = (k*(k-1)//2)
		merged = (df.alias("a")
				.join(df.alias("b"), F.col("a.set") > F.col("b.set"))
				.select(F.array_distinct(F.sort_array(F.concat('a.set', 'b.set'))).alias('merged')).filter(F.size('merged') == k))

		new_sets = (
			merged.groupBy("merged")
				.count()
				.filter(F.col("count") == required_pairs)
				.select(F.col("merged").alias("set"))
		)
		if show: new_sets.show()
		return new_sets


def test_scan():

	spark = SparkSession.builder.appName("test").getOrCreate()
	df = spark.sql('SELECT * FROM VALUES (ARRAY(1,2,3)), (ARRAY(1, 2, 4)), (ARRAY(2, 3)) AS t(basket)')
	df.show()
	apriori = APriori(df=df, s=2)

	potential_sets = spark.sql('SELECT * FROM VALUES (ARRAY(2,3)), (ARRAY(1,2)), (ARRAY(1,3)) AS t(set)')
	potential_sets.show()

	apriori._scan(potential_sets=potential_sets).show()


def test_apriori():

	spark = SparkSession.builder.appName("test").getOrCreate()
	df = spark.sql('SELECT * FROM VALUES (ARRAY(1,2,3)), (ARRAY(1, 2, 4)), (ARRAY(2, 3)) AS t(basket)')
	df.show()
	apriori = APriori(df=df, s=2)

	df_list = apriori.apriori_algorithm()

	for df in df_list:
		df.show()


if __name__ == "__main__":
	test_apriori()