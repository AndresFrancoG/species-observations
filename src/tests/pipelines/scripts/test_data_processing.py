import pytest

import species_observations.utils as utl
import species_observations.scripts.data_processing as dtp


@pytest.mark.parametrize(
        ('kedro_env', 'catalog_entry'),
        [
            ('test_cloud', 'preprocessing')
    ])
def test_Preprocessing_class(kedro_env: str, catalog_entry: str):
    """Test that the list of data_columns and the necessary parameters for the 
        preprocessing class are defined in the configuration .yml

        Test cases:
            'event_date' and 'individual_count' are in 'data_cols' and are strings
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
    prep = dtp.Preprocessing(parameters)

    assert 'data_cols' in parameters.keys()

    # List of columns of interest for the class
    str_data_cols = ['event_date', 'individual_count']
    for dc in str_data_cols:
        assert dc in parameters['data_cols'].keys()
        assert type(parameters['data_cols'][dc]) == str

    assert 'preprocessing' in parameters.keys()

    # Expected entries in the parameters along with expected types
    param_entries_types = {'resampling_period': str}

    for k,v in param_entries_types.items():
        assert k in parameters[catalog_entry].keys()
        assert type(parameters[catalog_entry][k]) == v

    # Individual restrictions
    assert parameters[catalog_entry]['resampling_period'] in ['D','M']

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
#         path, dataset = utl.load_PDS_from_catalog(kedro_env)
#         ds_dict = utl.load_partitionedDS_kedro(path, dataset)
#         df = utl.PartitionedDS2df(ds_dict)

#     df_out = prep.preprocessing_time_data(df)

#     assert 'data_cols' in parameters.keys()
#     assert type(dataset) is not None