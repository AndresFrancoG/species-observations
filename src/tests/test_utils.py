import pytest
import pandas as pd
import species_observations.utils as utl

from typing import Tuple


def load_PDS_from_catalog(kedro_env: str, config_entry: str = 'preprocessing') -> Tuple[str, ]:
    """Loads info of the entry for testing from the kedro catalog.

    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    config_entry : str, optional
        Entry within the .yml parameters files which contains the information, by default 'preprocessing'
        The entry name from the catalog is recovered from
        config_entry:
            tests:
                partitioned_sample_catalog: CATALOG_ENTRY_NAME                   
    Returns
    -------
    Tuple[str, ]
        From the catalog entry, returns
        path, dataset: type:  
    """
    config = utl.load_config_file_kedro(kedro_env = kedro_env)
    
    test_info = config['parameters'][config_entry]['tests']
    ds_catalog_name = test_info['partitioned_sample_catalog']    
    dataset_info = config['catalog'][ds_catalog_name]

    path = dataset_info['path']
    dataset = dataset_info['dataset']['type']

    return path, dataset


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
    path, dataset = load_PDS_from_catalog(kedro_env)
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
    path, dataset = load_PDS_from_catalog(kedro_env)
    ds_dict = utl.load_partitionedDS_kedro(path, dataset)
    df = utl.PartitionedDS2df(ds_dict)
    assert type(df) == expected_type
    for k,v in ds_dict.items():
        assert (v().columns == df.columns).all()


