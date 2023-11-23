import sys
import pyspark.sql.functions as F

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


def main():
    base_input_path = sys.argv[1]
    base_output_path = sys.argv[2]

    conf = SparkConf().setAppName('Events_ODD')
    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)

    events = spark.read.parquet(base_input_path)
    events.write.format('parquet').mode("overwrite").partitionBy('date').save(base_output_path)


if __name__ == "__main__":
    main()
