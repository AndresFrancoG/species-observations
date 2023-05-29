"""
Unit tests for node functions of pipeline observations_time
"""
import pytest

import species_observations.utils as utl
import species_observations.pipelines.observations_time.nodes as nd

@pytest.mark.parametrize(
        ('kedro_env', 'catalog_entry'),
        [
            ('test_cloud', 'preprocessing')
    ])
def node_preprocessing_time_data(kedro_env: str, catalog_entry: str):
    config = utl.load_config_file_kedro(kedro_env = kedro_env)
    parameters = config['parameters']

    df_sample = utl.load_csv_from_catalog(kedro_env, config_entry=catalog_entry)
    df_resampled = nd.node_preprocessing_time_data(df_sample, parameters)