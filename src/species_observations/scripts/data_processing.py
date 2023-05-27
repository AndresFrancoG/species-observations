import pandas as pd
import species_observations.utils as utl

from typing import Dict

class Preprocessing:
    """Contains preprocessing methods for the project species_observation
    """
    def __init__(self, parameters: Dict, catalog_entry = 'preprocessing'):
        """Defines the parameters necessary for data preprocessing in the current project.

        Parameters
        ----------
        parameters : Dict
            Dictionary with parameters loaded through kedro with .yml files.
        """
        self._full_cols = parameters['data_cols']
        self._date_col = self._full_cols['event_date']
        self._count_col = self._full_cols['individual_count']

        self._preproc_params = parameters[catalog_entry]
        self._resample = self._preproc_params['resampling_period']
        allowed_resamples = ['D', 'M']
        if self._resample not in allowed_resamples:
            raise ValueError(f""" 'resampling_period' can only take values of {allowed_resamples}.
'{self._resample}' was given""")

        self._datetime_suffix = '_datetime'

    def get_date_col(self) -> str:
        """Allows access to the contents of protected attribute _date_col

        Returns
        -------
        str
            Contents of _date_col
        """
        return self._date_col

    def get_datetime_suffix(self) -> str:
        """Allows access to the contents of protected attribute _datetime_suffix

        Returns
        -------
        str
            Contents of _datetime_suffix
        """
        return self._datetime_suffix

    def preprocessing_time_data(self, df_in: pd.DataFrame) -> pd.DataFrame:
        """Fills NaN in the column count_col with zeros.  
            Takes the column date_col of type string and transforms it to datetime. 
            Stores it in a new column of name date_col + "_datetime"
            count_col and date_col are defined at the constructor through 'parameters'

        Parameters
        ----------
        df_in : pd.DataFrame or dict of dataframes loaded from partitioned data
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

        if isinstance(df_in, dict):
            df_in = utl.partitioned_ds_to_df(df_in)

        df_in[date_col_datetime] = pd.to_datetime(df_in[date_col])
        df_in[count_col] = df_in[count_col].fillna(0)
        df_out = df_in[[count_col, date_col_datetime]]

        return df_out

    def time_resampling(self, df_in: pd.DataFrame) -> pd.DataFrame:
        """Resamples df to the frequency specified by "resample".
            Used the column date_col_datetime as basis for the resample,
            and sets the resampled date_col_datetime as index
            "date_col" and "resample" are defined at the constructor through 'parameters'

        Parameters
        ----------
        df_in : pd.DataFrame
            DataFrame to be resampled

        Returns
        -------
        pd.DataFrame
            Resample dataframe
        """
        date_col = self._date_col
        date_col_datetime = date_col + self._datetime_suffix
        resample = self._resample
        return df_in.set_index(date_col_datetime).resample(resample).sum()
