from pyspark.sql import functions as F, SparkSession
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pathlib import Path
import re
from pyspark.testing import assertDataFrameEqual

ROOT = Path().resolve()

class DataProcessing:
    def __init__(self, filepath: Path = ROOT / "Data" / "sales.dat"):
        self.filepath = str(filepath)
        if not Path.exists(filepath):
            raise ValueError(f"Did not find the data file at {filepath}!")

    def load_dataframe(self, spark: SparkSession, prints: bool = True):
        """Loads the .dat file from self.filepath and loads it into a spark dataframe."""
        reader = spark.read.text(self.filepath)
        df = (reader.select(F.split(F.trim(F.col("value")), r"\s+")
                            .cast("array<int>")
                            .alias("basket"))
                            .filter(F.size(F.col("basket")) > 0))
        
        if prints: 
            df.show()
            print(f"Successfully loaded dataframe with {df.count()} nbr of baskets!")

        return df
        


def permute_increasing(df: DataFrame, show: bool = True):
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

if __name__ == "__main__":
    spark = SparkSession.builder.appName(name="data_proc_main").getOrCreate()
    #dataProc = DataProcessing()
    #df = dataProc.load_dataframe(spark)
    
    input = spark.sql('SELECT * FROM VALUES (ARRAY(1,2,3)), (ARRAY(1,2,4)), (ARRAY(1,3,4)), (ARRAY(2, 3, 4)) AS t(set)')
    answer = spark.sql('SELECT * FROM VALUES (ARRAY(1,2,3,4)) AS t(set)')
    assertDataFrameEqual(answer, permute_increasing(input))

    input = spark.sql('SELECT * FROM VALUES (ARRAY(1)), (ARRAY(2)), (ARRAY(3)) AS t(set)')
    answer = spark.sql('SELECT * FROM VALUES (ARRAY(1,2)), (ARRAY(1,3)), (ARRAY(2,3)) AS t(set)')
    assertDataFrameEqual(answer, permute_increasing(input))
    
