import pytest
import pandas as pd
from kedro_datasets.pandas import CSVDataSet
import species_observations.utils as utl

def test_load_partitionedDS_kedro():
    path = "data//01_raw//species_bigQuery_sample"
    dataset = CSVDataSet
    expected_type = dict
    expected_item_type = pd.DataFrame
    ds_dict = utl.load_partitionedDS_kedro(path, dataset)
    assert type(ds_dict) == expected_type
    for k,v in ds_dict.items():
        assert type(v()) == expected_item_type


def test_PartitionedDS2df():
    path = "data//01_raw//species_bigQuery_sample"
    dataset = CSVDataSet
    expected_type = pd.DataFrame
    ds_dict = utl.load_partitionedDS_kedro(path, dataset)
    df = utl.PartitionedDS2df(ds_dict)
    assert type(df) == expected_type
    for k,v in ds_dict.items():
        assert (v().columns == df.columns).all()