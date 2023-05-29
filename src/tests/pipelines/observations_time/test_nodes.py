"""
Unit tests for node functions of pipeline observations_time
"""
import pytest
import pandas as pd

import species_observations.utils as utl
import species_observations.pipelines.observations_time.nodes as nd


@pytest.mark.parametrize(
    ("kedro_env", "catalog_entry"), [("test_cloud", "preprocessing")]
)
def node_preprocessing_time_data_type(kedro_env: str, catalog_entry: str):
    """Test cases:
        Output is of DataFrame type

    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    catalog_entry : str
        Entry from the .yml configuration file associated to the kedro pipeline
        The CSV to be used as test data is specified catalog_entry
        as tests -> csv_sample_catalog
    """
    config = utl.load_config_file_kedro(kedro_env=kedro_env)
    parameters = config["parameters"]

    df_sample = utl.load_csv_from_catalog(kedro_env, config_entry=catalog_entry)
    df_resampled = nd.node_preprocessing_time_data(df_sample, parameters)
    assert isinstance(df_resampled, pd.DataFrame)


@pytest.mark.parametrize(
    ("kedro_env", "catalog_entry"), [("test_cloud", "preprocessing")]
)
def node_preprocessing_time_data_nan(kedro_env: str, catalog_entry: str):
    """Test cases:
        Output has no NaN

    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    catalog_entry : str
        Entry from the .yml configuration file associated to the kedro pipeline
        The CSV to be used as test data is specified catalog_entry
        as tests -> csv_sample_catalog
    """
    config = utl.load_config_file_kedro(kedro_env=kedro_env)
    parameters = config["parameters"]

    df_sample = utl.load_csv_from_catalog(kedro_env, config_entry=catalog_entry)
    df_resampled = nd.node_preprocessing_time_data(df_sample, parameters)
    assert df_resampled.isna().sum().sum() == 0
