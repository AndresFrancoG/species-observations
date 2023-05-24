import pandas as pd
from typing import Dict, Tuple
from kedro.io import PartitionedDataSet
from kedro.config import ConfigLoader
from kedro.framework.project import settings


def PartitionedDS2df(pd_dict: Dict) -> pd.DataFrame:
    """Converts data loaded with PartitionedDataSet into a single pandas datafra,e

    Parameters
    ----------
    pd_dict : Dict
        Data loaded with PartitionedDataSet

    Returns
    -------
    pd.DataFrame
        Single dataframe with all data
    """
    df = pd.DataFrame()
    for partition_id, partition_load_func in pd_dict.items():
        partition_data = partition_load_func()
        df = pd.concat(
            [df, partition_data], ignore_index=True, sort=True
        )
    return df


def load_partitionedDS_kedro(path: str, dataset: Dict) -> pd.DataFrame:
    """Loads a partitioned data stored in the folder specified in path.

    Parameters
    ----------
    path : str
        Folder of the partitioned data
    dataset : Dict
        Type of data to search for and load options (dataset option in kedro's catalog of type PartitionedDataSet)

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
    config = load_config_file_kedro(kedro_env = kedro_env)
    
    test_info = config['parameters'][config_entry]['tests']
    ds_catalog_name = test_info['partitioned_sample_catalog']    
    dataset_info = config['catalog'][ds_catalog_name]

    path = dataset_info['path']
    dataset = dataset_info['dataset']['type']

    return path, dataset