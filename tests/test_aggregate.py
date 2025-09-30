import pandas as pd
import os
from src.aggregate import aggregate_to_gold

def test_aggregate(tmp_path):
    # Mock silver data
    data = pd.DataFrame({
        'brewery_type': ['micro', 'brewpub', 'micro'],
        'state': ['CA', 'NY', 'CA']
    })
    input_dir = tmp_path / 'silver'
    input_dir.mkdir()
    data.to_parquet(input_dir / 'mock.parquet')

    output_path = str(tmp_path / 'gold' / 'agg.parquet')
    aggregate_to_gold(str(input_dir), output_path)

    result = pd.read_parquet(output_path)
    expected = pd.DataFrame({
        'brewery_type': ['brewpub', 'micro'],
        'state': ['NY', 'CA'],
        'count': [1, 2]
    })
    pd.testing.assert_frame_equal(result.sort_values(['brewery_type', 'state']).reset_index(drop=True), expected)