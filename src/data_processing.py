from pyspark.sql import functions as F, SparkSession
from pyspark.sql.types import ArrayType, IntegerType
from pathlib import Path
import re

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
                            .alias("baskets"))
                            .filter(F.size(F.col("baskets")) > 0))
        
        if prints: 
            df.show()
            print(f"Successfully loaded dataframe with {df.count()} nbr of baskets!")

        return df
        




if __name__ == "__main__":
    spark = SparkSession.builder.appName(name="data_proc_main").getOrCreate()
    dataProc = DataProcessing()
    df = dataProc.load_dataframe(spark)
