"""Helper functions for various actions"""
from typing import Dict, Tuple, Type
import pandas as pd
from kedro.io import PartitionedDataSet
from kedro.config import ConfigLoader
from kedro.framework.project import settings


def partitioned_ds_to_df(pd_dict: Dict) -> pd.DataFrame:
    """Converts data loaded with PartitionedDataSet into a single pandas dataframe

    Parameters
    ----------
    pd_dict : Dict
        Data loaded with PartitionedDataSet

    Returns
    -------
    pd.DataFrame
        Single dataframe with all data
    """
    df_joined = pd.DataFrame()
    for _, partition_load_func in pd_dict.items():
        partition_data = partition_load_func()
        df_joined = pd.concat(
            [df_joined, partition_data], ignore_index=True, sort=True
        )
    return df_joined


def load_partitioned_ds_kedro(path: str, dataset: Dict) -> pd.DataFrame:
    """Loads a partitioned data stored in the folder specified in path.

    Parameters
    ----------
    path : str
        Folder of the partitioned data
    dataset : Dict
        Type of data to search for and load options 
        (dataset option in kedro's catalog of type PartitionedDataSet)

    Returns
    -------
    pd.DataFrame
        joined dataset
    """
    data_set = PartitionedDataSet(
        path=path,
        dataset=dataset,
    )

    print(dataset)

    return data_set.load()


def load_config_file_kedro(kedro_env : str = 'base') -> Dict:
    """Loads kedro's yml files in the config folder as dictionaries

    Parameters
    ----------
    kedro_env : str, optional
        Environment from which the config files will be loaded, by default 'base'

    Returns
    -------
    Dict
        Configuration info as dictionary.  
            key 'catalog' returns the data catalog
            key 'parameters' returns pipeline parameters
    """
    conf_path = str(settings.CONF_SOURCE)
    return ConfigLoader(conf_source=conf_path, env=kedro_env)


def load_pds_from_catalog(kedro_env: str, config_entry: str = 'preprocessing') -> Tuple[str, ]:
    """Loads info of the entry for testing from the kedro catalog.

    Parameters
    ----------
    kedro_env : str
        kedro environment to be used
    config_entry : str, optional
        Entry within the .yml parameters files which contains the information, 
            by default 'preprocessing'
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
    config = load_config_file_kedro(kedro_env = kedro_env)
    
    test_info = config['parameters'][config_entry]['tests']
    ds_catalog_name = test_info['partitioned_sample_catalog']
    dataset_info = config['catalog'][ds_catalog_name]

    path = dataset_info['path']
    dataset = dataset_info['dataset']['type']

    return path, dataset


def attribute_names_types(member_variables: Dict, cls: Type, type_mapping: Dict) -> Dict:
    """Using a dictionary of variable names and expected types as strings, recovers 
    the attributes from the class cls with those variables names, and assigns the 
    expected types in a format valid to be used for isinstance()

    Parameters
    ----------
    member_variables : Dict
        Contains a dictionary with the names of the attibutes and 
        their expected types (as strings) 
    cls : type
        Class for which the attibutes will be checked
    type_mapping : Dict
        Allows to convert string expected types to standard 
        expected typs

    Returns
    -------
    Dict
        Returns the values and expected types of the attributes
        {'attribute_name': {
                'value': attribute_value
                'type': expected_type
            }
        }
    """
    names_types = {}
    for key, value in member_variables.items():
        var_value = getattr(cls, key)
        names_types[key] = {'value': var_value,
                            'type': type_mapping[value]}
    return names_types
