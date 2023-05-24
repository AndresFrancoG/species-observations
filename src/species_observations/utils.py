import pandas as pd
from typing import Dict
from kedro.io import PartitionedDataSet


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