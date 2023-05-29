"""
This is a boilerplate pipeline 'observations_time'
generated using Kedro 0.18.8
"""
from typing import Dict
import pandas as pd

from species_observations.scripts.data_processing import Preprocessing

def node_preprocessing_time_data(df_raw: pd.DataFrame, parameters: Dict) -> pd.DataFrame:
    """Raw data preprocessing

    Parameters
    ----------
    df : pd.DataFrame
        Raw data
    parameters : Dict
        Parameters defined in conf/base or kedro_env/parameters/observations_time.yml

    Returns
    -------
    pd.DataFrame
        Resampled data
    """
    prep = Preprocessing(parameters)
    df_preproc = prep.preprocessing_time_data(df_raw)
    df_resampled = prep.time_resampling(df_preproc)
    return df_resampled
