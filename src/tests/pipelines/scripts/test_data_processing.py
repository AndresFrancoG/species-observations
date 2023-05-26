"""Unit tests for the file data_processing.py"""
import pytest

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
        Entry from the .yml configuration file in which additional parameters are stored
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
        Entry from the .yml configuration file in which additional parameters are stored
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
    """Test that the list of data_columns and the necessary parameters for the 
        preprocessing class are defined in the configuration .yml

        Test cases:
            'resample_period' is in catalog_entry and are strings
            'resample_period' is in ['D','M']
    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    catalog_entry : str
        Entry from the .yml configuration file in which additional parameters are stored
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
# @pytest.mark.parametrize(
#         ('kedro_env', 'data_type'),
#         [
#             ('test_cloud', 'Partitioned')
#     ])
# def test_preprocessing_time_data(kedro_env: str, data_type: str):
#     config = utl.load_config_file_kedro(kedro_env = kedro_env)
#     parameters = config['parameters']
#     prep = dtp.Preprocessing(parameters)
    
#     if data_type == 'Partitioned':
#         path, dataset = utl.load_pds_from_catalog(kedro_env)
#         ds_dict = utl.load_partitioned_ds_kedro(path, dataset)
#         df = utl.partitioned_ds_to_df(ds_dict)

#     df_out = prep.preprocessing_time_data(df)
