from pyspark.sql import SparkSession
import os
from src.aggregate import aggregate_to_gold

def test_aggregate(tmp_path):
    spark = SparkSession.builder.appName("TestAggregate").getOrCreate()
    # Mock silver data
    data = [
        {'brewery_type': 'micro', 'country': 'US'},
        {'brewery_type': 'brewpub', 'country': 'US'},
        {'brewery_type': 'micro', 'country': 'US'}
    ]
    df = spark.createDataFrame(data)
    input_dir = str(tmp_path / 'silver')
    df.write.parquet(input_dir)

    output_path = str(tmp_path / 'gold' / 'agg.parquet')
    aggregate_to_gold(spark, input_dir, output_path)

    result_df = spark.read.parquet(output_path)
    result = result_df.collect()
    # Sort for comparison
    result_sorted = sorted(result, key=lambda x: (x['brewery_type'], x['country']))
    expected = [
        {'brewery_type': 'brewpub', 'country': 'US', 'count': 1},
        {'brewery_type': 'micro', 'country': 'US', 'count': 2}
    ]
    assert result_sorted == expected
    spark.stop()
