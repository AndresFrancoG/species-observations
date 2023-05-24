import os
import pytest

import pandas as pd
import species_observations.utils as utl

@pytest.mark.parametrize(
        ('kedro_env', 'config_entry'),
        [
            ('test_cloud', 'preprocessing')
    ])
def test_load_PDS_from_catalog(kedro_env: str, config_entry: str):
    """Test cases:
        Path is a directory
        dataset is not empty/None

    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    config_entry : str
        Entry from the parameters which contains the testing dataset info
    """
    path, dataset = utl.load_PDS_from_catalog(kedro_env, config_entry)
    
    assert os.path.isdir(path)
    assert type(dataset) is not None


@pytest.mark.parametrize(
        ('kedro_env','expected_type'),
        [
            ('test_cloud', dict)
    ])
def test_load_config_file_kedro(kedro_env: str, expected_type: type):
    """Test cases:
        Catalog and parameters are loaded as the expected type. Typically a dict

    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    expected_type : type
        Expected type of the loaded info.
    """
    config = utl.load_config_file_kedro(kedro_env = kedro_env)
    assert type(config['catalog']) == expected_type
    assert type(config['parameters']) == expected_type


@pytest.mark.parametrize(
        ('kedro_env','expected_type','expected_item_type'),
        [
            ('test_cloud', dict, pd.DataFrame)
    ])
def test_load_partitionedDS_kedro(kedro_env: str, expected_type: type, expected_item_type: type):
    """Test cases:
        The loaded partitioned dataset is of the expected type, typically dict
        The individual items of the partitioned dataset are of the expected type,
        typically pd.DataFram

    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    expected_type : type
        Expected type of the loaded partitioned dataset.
    expected_item_type : type
        Expected type of individual items in the loaded partitioned dataset.
    """
    path, dataset = utl.load_PDS_from_catalog(kedro_env)
    ds_dict = utl.load_partitionedDS_kedro(path, dataset)

    assert type(ds_dict) == expected_type
    for k,v in ds_dict.items():
        assert type(v()) == expected_item_type

@pytest.mark.parametrize(
        ('kedro_env','expected_type'),
        [
            ('test_cloud', pd.DataFrame)
    ])
def test_PartitionedDS2df(kedro_env: str, expected_type: type):
    """Test cases:
        Data is loaded as the expected type. Typically pd.DataFrame
        The columns of each entry of the loaded partitioned dataset
        are the same as the columns of the resulting dataframe

    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    expected_type : type
        Expected type of the loaded info.
    """    
    path, dataset = utl.load_PDS_from_catalog(kedro_env)
    ds_dict = utl.load_partitionedDS_kedro(path, dataset)
    df = utl.PartitionedDS2df(ds_dict)
    assert type(df) == expected_type
    for k,v in ds_dict.items():
        assert (v().columns == df.columns).all()


