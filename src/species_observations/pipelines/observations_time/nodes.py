"""
This is a boilerplate pipeline 'observations_time'
generated using Kedro 0.18.8
"""
import pandas as pd
from typing import Dict

from species_observations.scripts.data_processing import Preprocessing

def node_preprocessing_time_data(df: pd.DataFrame, parameters: Dict) -> pd.DataFrame:
    prep = Preprocessing(parameters)
    df = prep.preprocessing_time_data(df)
    df = prep.time_resampling(df)
    return df