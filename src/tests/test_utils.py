"""Unit tests for the file utils.py"""
from typing import Dict
import os
import pytest

import pandas as pd
from kedro_datasets.pandas import CSVDataSet

import species_observations.utils as utl
import species_observations.scripts.data_processing as dtp


@pytest.mark.parametrize(
    ("kedro_env", "config_entry"), [("test_cloud", "preprocessing")]
)
def test_load_pds_from_catalog(kedro_env: str, config_entry: str):
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
    path, dataset = utl.load_pds_from_catalog(kedro_env, config_entry)
    assert os.path.isdir(path)
    assert dataset is not None


@pytest.mark.parametrize(("kedro_env", "expected_type"), [("test_cloud", dict)])
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
    config = utl.load_config_file_kedro(kedro_env=kedro_env)
    assert isinstance(config["catalog"], expected_type)
    assert isinstance(config["parameters"], expected_type)


@pytest.mark.parametrize(
    ("kedro_env", "expected_type", "expected_item_type"),
    [("test_cloud", dict, pd.DataFrame)],
)
def test_load_partitioned_ds_kedro(
    kedro_env: str, expected_type: type, expected_item_type: type
):
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
    path, dataset = utl.load_pds_from_catalog(kedro_env)
    ds_dict = utl.load_partitioned_ds_kedro(path, dataset)

    assert isinstance(ds_dict, expected_type)
    for _, value in ds_dict.items():
        assert isinstance(value(), expected_item_type)


@pytest.mark.parametrize(("kedro_env", "expected_type"), [("test_cloud", pd.DataFrame)])
def test_partitioned_ds_to_df(kedro_env: str, expected_type: type):
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
    path, dataset = utl.load_pds_from_catalog(kedro_env)
    ds_dict = utl.load_partitioned_ds_kedro(path, dataset)
    df_sample = utl.partitioned_ds_to_df(ds_dict)
    assert isinstance(df_sample, expected_type)
    for _, value in ds_dict.items():
        assert (value().columns == df_sample.columns).all()


@pytest.mark.parametrize(
    ("kedro_env", "attribute_list", "type_mapping"),
    [("test_cloud", {"_full_cols": "dict"}, {"dict": dict})],
)
def test_attribute_names_types(
    kedro_env: str, attribute_list: Dict, type_mapping: Dict
):
    """Test cases:
            All keys of returned dictionary are in the keys of type_mapping
            All items of the returned dictionary have the keys 'value' and 'type'
    Parameters
    ----------
    kedro_env : str
        _description_
    attribute_list : Dict
        _description_
    type_mapping : Dict
        _description_
    """
    config = utl.load_config_file_kedro(kedro_env=kedro_env)
    parameters = config["parameters"]
    prep = dtp.Preprocessing(parameters)
    name_types = utl.attribute_names_types(attribute_list, prep, type_mapping)
    for key, value in name_types.items():
        assert key in attribute_list.keys()
        assert "value" in list(value.keys())
        assert "type" in list(value.keys())


@pytest.mark.parametrize(("filepath"), [("data//01_raw//species_bigQuery_sample.csv")])
def test_load_csv_from_filepath_type(filepath):
    """Test cases:
            Output is of type pd.DataFrame
    Parameters
    ----------
    filepath : str
        Location of csv
    """
    df_sample = utl.load_csv_from_filepath(filepath)
    assert isinstance(df_sample, pd.DataFrame)


@pytest.mark.parametrize(
    ("filepath"), [("species-observations//data//01_raw//species_bigQuery_sample.csv")]
)
def test_load_csv_from_filepath_error(filepath):
    """Test cases:
            Exception on invalid path
    Parameters
    ----------
    filepath : str
        Invalid path
    """
    with pytest.raises(Exception):
        utl.load_csv_from_filepath(filepath)


@pytest.mark.parametrize(
    ("data_type", "path"),
    [
        ("CSV", "data//01_raw//species_bigQuery_sample.csv"),
        (
            "Partitioned",
            "data//01_raw//species_bigQuery_sample//",
        ),
    ],
)
def test_validates_dataframe(data_type: str, path: str):
    """Test cases:
            Output type is pd.DataFrame
    Parameters
    ----------
    data_type : str
        What kind of data is used as source
            'Partitioned': Partitioned dataset, expecting CSVDataSet
            'CSV': .csv
    filepath : str
        Location of csv or folder with multiple .csv
    """
    if data_type == "Partitioned":
        df_sample = utl.load_partitioned_ds_kedro(path, CSVDataSet)
    elif data_type == "CSV":
        df_sample = utl.load_csv_from_filepath(path)

    df_out = utl.validates_dataframe(df_sample)
    assert isinstance(df_out, pd.DataFrame)


@pytest.mark.parametrize(
    ("input_data"),
    [
        (1.0),
    ],
)
def test_validates_dataframe_type(input_data: object):
    with pytest.raises(ValueError):
        utl.validates_dataframe(input_data)
