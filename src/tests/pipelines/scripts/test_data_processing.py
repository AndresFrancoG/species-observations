"""Unit tests for the file data_processing.py"""
import pytest
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from kedro_datasets.pandas import CSVDataSet
from monthdelta import monthdelta

import species_observations.utils as utl
import species_observations.scripts.data_processing as dtp



@pytest.mark.parametrize(
        ('kedro_env', 'catalog_entry'),
        [
            ('test_cloud', 'preprocessing')
    ])
def test_preprocessing_class_attributes(kedro_env: str, catalog_entry: str):
    """Test that the list of data_columns and the necessary parameters for the 
        preprocessing class are defined in the configuration .yml

        Test cases:
            Attributes of Preprocessing() are of correct types 
    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    catalog_entry : str
        Entry from the .yml configuration file associated to the kedro pipeline
    """
    config = utl.load_config_file_kedro(kedro_env = kedro_env)
    parameters = config['parameters']
    prep = dtp.Preprocessing(parameters)

    # Checks if members of Preprocessing are correct types
    type_mapping = {
        'str': str,
        'dict': dict,
    } # Add types as needed
    name_types = utl.attribute_names_types(parameters[catalog_entry]['tests']['member_variables'],
                                           prep, type_mapping)
    for _, value in name_types.items():
        assert isinstance(value['value'], value['type'])


@pytest.mark.parametrize(
        ('kedro_env', 'catalog_entry'),
        [
            ('test_cloud', 'preprocessing')
    ])
def test_preprocessing_class_data_cols(kedro_env: str, catalog_entry: str):
    """Test that the list of data_columns and the necessary parameters for the 
        preprocessing class are defined in the configuration .yml

        Test cases:
            columns defined in 'tests' entries of the .yml are in 'data_cols' and are strings
    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    catalog_entry : str
        Entry from the .yml configuration file associated to the kedro pipeline
    """
    config = utl.load_config_file_kedro(kedro_env = kedro_env)
    parameters = config['parameters']

    assert 'data_cols' in parameters.keys()
    # List of columns of interest for the class
    str_data_cols = (parameters[catalog_entry]['tests']['columns'] +
        [parameters[catalog_entry]['tests']['index_col']]
    )
    for data_column in str_data_cols:
        assert data_column in list(parameters['data_cols'].keys())
        assert isinstance(parameters['data_cols'][data_column], str)

    assert catalog_entry in parameters.keys()


@pytest.mark.parametrize(
        ('kedro_env', 'catalog_entry'),
        [
            ('test_cloud', 'preprocessing')
    ])
def test_preprocessing_class_resample_period(kedro_env: str, catalog_entry: str):
    """Test cases:
            'resample_period' is in catalog_entry and are strings
            'resample_period' is in ['D','M']
    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    catalog_entry : str
        Entry from the .yml configuration file associated to the kedro pipeline
    """
    config = utl.load_config_file_kedro(kedro_env = kedro_env)
    parameters = config['parameters']
    individual_restrictions = ['D','M']

    # Expected entries in the parameters along with expected types
    param_entries_types = {'resampling_period': str}

    for k,v in param_entries_types.items():
        assert k in parameters[catalog_entry].keys()
        assert isinstance(parameters[catalog_entry][k], v)

    # Individual restrictions
    assert parameters[catalog_entry]['resampling_period'] in individual_restrictions


@pytest.mark.parametrize(
        ('kedro_env',  'catalog_entry', 'data_type'),
        [
            ('test_cloud', 'preprocessing', 'Partitioned'),
            ('test_cloud', 'preprocessing', 'CSV')
    ])
def test_preprocessing_time_data(kedro_env: str, catalog_entry: str, data_type: str):
    """Test cases:
            Output dataframe has no NaN
            The column eventdate_datetime exists and is of type datetime
    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    catalog_entry : str
        Entry from the .yml configuration file associated to the kedro pipeline
    data_type : str
        What kind of data is used as source
            'Partitioned': Partitioned dataset entry from the data catalog.
                Defined in catalog_entry as partitioned_sample_catalog
            'CSV': CSVPandasDS entry from the data catalog.
                Defined in catalog_entry as csv_sample_catalog                

    """
    config = utl.load_config_file_kedro(kedro_env = kedro_env)
    parameters = config['parameters']
    prep = dtp.Preprocessing(parameters)

    if data_type == 'Partitioned':
        path, dataset = utl.load_pds_from_catalog(kedro_env, config_entry=catalog_entry)
        ds_dict = utl.load_partitioned_ds_kedro(path, dataset)
        df_sample = utl.partitioned_ds_to_df(ds_dict)
    elif data_type == 'CSV':
        df_sample = utl.load_csv_from_catalog(kedro_env, config_entry=catalog_entry)

    df_out = prep.preprocessing_time_data(df_sample)
    assert df_out.isna().sum().sum() == 0
    assert is_datetime(df_out[prep.get_date_col() + prep.get_datetime_suffix()])


@pytest.mark.parametrize(
        ('kedro_env',  'catalog_entry'),
        [
            ('test_cloud', 'preprocessing')
    ])
def test_time_resampling_index(kedro_env: str, catalog_entry: str):
    """Test cases:
            Index is datetime
    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    catalog_entry : str
        Entry from the .yml configuration file associated to the kedro pipeline
        The CSV to be used as test data is specified in catalog_entry 
        as tests -> csv_sample_catalog
    """
    config = utl.load_config_file_kedro(kedro_env = kedro_env)
    parameters = config['parameters']
    prep = dtp.Preprocessing(parameters)

    df_sample = utl.load_csv_from_catalog(kedro_env,
        config_entry=catalog_entry, entry_name='csv_sample_catalog_preprocessed_stage_01'
        )

    # Index is datetime?
    df_out = prep.time_resampling(df_sample)
    assert is_datetime(df_out.index)


@pytest.mark.parametrize(
        ('kedro_env',  'catalog_entry', 'resample'),
        [
            ('test_cloud', 'preprocessing', 'D'),
            ('test_cloud', 'preprocessing', 'M')
    ])
def test_time_resampling_diff(kedro_env: str, catalog_entry: str, resample: str):
    """Test cases:
            Minimum time difference resampling is appropriate with the 
            expected resampling
    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    catalog_entry : str
        Entry from the .yml configuration file associated to the kedro pipeline
        The CSV to be used as test data is specified in catalog_entry 
        as tests -> csv_sample_catalog
    resample : str
        Resample period. 'D' -> Daily.  'M' -> Montly
    """
    config = utl.load_config_file_kedro(kedro_env = kedro_env)
    parameters = config['parameters']
    prep = dtp.Preprocessing(parameters)

    df_sample = utl.load_csv_from_catalog(kedro_env,
        config_entry=catalog_entry, entry_name='csv_sample_catalog_preprocessed_stage_01'
        )

    # Index is datetime?
    df_out = prep.time_resampling(df_sample, resample=resample)

    # Minimum difference is equal to the expected resampling
    resample = prep.get_resample()
    if resample == 'D':
        min_time_diff = df_out.index.to_series().diff().min().total_seconds() / (3600*24)
    elif resample == 'M':
        min_time_diff = df_out.index.to_series().diff().min() / monthdelta(1)      
    assert min_time_diff >= 1


@pytest.mark.parametrize(
        ('kedro_env',  'catalog_entry', 'resample'),
        [
            ('test_cloud', 'preprocessing', 'X'),
            ('test_cloud', 'preprocessing', 'Y')
    ])
def test_time_resampling_val_error(kedro_env: str, catalog_entry: str, resample: str):
    """Test cases:
            Raises error when resample is not valid
    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    catalog_entry : str
        Entry from the .yml configuration file associated to the kedro pipeline
        The CSV to be used as test data is specified in catalog_entry 
        as tests -> csv_sample_catalog
    resample : str
        Resample period. Should not be in the list Preprocessing()._allowed_resamples
    """
    config = utl.load_config_file_kedro(kedro_env = kedro_env)
    parameters = config['parameters']
    prep = dtp.Preprocessing(parameters)

    df_sample = utl.load_csv_from_catalog(kedro_env,
        config_entry=catalog_entry, entry_name='csv_sample_catalog_preprocessed_stage_01'
        )

    # Index is datetime?
    with pytest.raises(ValueError):
        df_out = prep.time_resampling(df_sample, resample=resample)