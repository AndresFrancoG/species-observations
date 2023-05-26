import pandas as pd
import species_observations.utils as utl

from typing import Dict

class Preprocessing:
    def __init__(self, parameters: Dict, catalog_entry = 'preprocessing'):
        """Defines the parameters necessary for data preprocessing in the current project.

        Parameters
        ----------
        parameters : Dict
            Dictionary with parameters loaded through kedro.
        """
        self._full_cols = parameters['data_cols']
        self._date_col = self._full_cols['event_date']
        self._count_col = self._full_cols['individual_count']

        self._preproc_params = parameters[catalog_entry]
        self._resample = self._preproc_params['resampling_period']
        allowed_resamples = ['D', 'M']
        if self._resample not in allowed_resamples:
            raise ValueError(f"'resampling_period' can only take values of {allowed_resamples}. '{self._resample}' was given")

        self._datetime_suffix = '_datetime'

    def preprocessing_time_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fills NaN in the column count_col with zeros.  
            Takes the column date_col of type string and transforms it to datetime. 
            Stores it in a new column of name date_col + "_datetime"
            count_col and date_col are defined at the constructor through 'parameters'

        Parameters
        ----------
        df : pd.DataFrame or dict of dataframes loaded from partitioned data
            Info to be processed.  If dict, first concatenates all entries into 
            a single df

        Returns
        -------
        pd.DataFrame
            Processed datafram
        """
        date_col = self._date_col
        date_col_datetime = date_col + self._datetime_suffix
        count_col = self._count_col

        if type(df) == dict:
            df = utl.partitioned_ds_to_df(df)

        df[date_col_datetime] = pd.to_datetime(df[date_col])
        df[count_col] = df[count_col].fillna(0)
        df_out = df[[count_col, date_col_datetime]]

        return df_out
    
    def time_resampling(self, df: pd.DataFrame) -> pd.DataFrame:
        """Resamples df to the frequency specified by "resample".
            Used the column date_col_datetime as basis for the resample,
            and sets the resampled date_col_datetime as index
            "date_col" and "resample" are defined at the constructor through 'parameters'

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to be resampled

        Returns
        -------
        pd.DataFrame
            Resample dataframe
        """
        date_col = self._date_col
        date_col_datetime = date_col + self._datetime_suffix
        resample = self._resample
        return df.set_index(date_col_datetime).resample(resample).sum()
