from pyspark.sql import SparkSession
import os
from src.aggregate import aggregate_to_gold

def test_aggregate(tmp_path):
    spark = SparkSession.builder.appName("TestAggregate").getOrCreate()
    # Mock silver data
    data = [
        {'brewery_type': 'micro', 'state': 'CA'},
        {'brewery_type': 'brewpub', 'state': 'NY'},
        {'brewery_type': 'micro', 'state': 'CA'}
    ]
    df = spark.createDataFrame(data)
    input_dir = str(tmp_path / 'silver')
    os.makedirs(input_dir, exist_ok=True)
    df.write.parquet(input_dir)

    output_path = str(tmp_path / 'gold' / 'agg.parquet')
    aggregate_to_gold(spark, input_dir, output_path)

    result_df = spark.read.parquet(output_path)
    result = result_df.collect()
    # Sort for comparison
    result_sorted = sorted(result, key=lambda x: (x['brewery_type'], x['state']))
    expected = [
        {'brewery_type': 'brewpub', 'state': 'NY', 'count': 1},
        {'brewery_type': 'micro', 'state': 'CA', 'count': 2}
    ]
    assert result_sorted == expected
    spark.stop()