import pandas as pd
from typing import Dict


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