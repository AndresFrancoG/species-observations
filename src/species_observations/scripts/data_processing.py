"""Defines Preprocessing() class which performs various data preprocessing 
    in the species_observation project
    """
from typing import Dict
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime

import species_observations.utils as utl


class Preprocessing:
    """Contains preprocessing methods for the project species_observation"""

    def __init__(self, parameters: Dict, catalog_entry="preprocessing"):
        """Defines the parameters necessary for data preprocessing in the current project.

        Parameters
        ----------
        parameters : Dict
            Dictionary with parameters loaded through kedro with .yml files.
        """
        self._full_cols = parameters["data_cols"]
        self._date_col = self._full_cols["event_date"]
        self._count_col = self._full_cols["individual_count"]

        self._preproc_params = parameters[catalog_entry]
        self._resample = self._preproc_params["resampling_period"]
        self._allowed_resamples = ["D", "M"]
        if self._resample not in self._allowed_resamples:
            raise ValueError(
                f""" 'resampling_period' can only take values of {self._allowed_resamples}.
'{self._resample}' was given"""
            )

        self._datetime_suffix = "_datetime"

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

    def get_resample(self) -> str:
        """Allows access to the contents of protected attribute _datetime_suffix

        Returns
        -------
        str
            Contents of _datetime_suffix
        """
        return self._resample

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

        df_in = utl.validates_dataframe(df_in)

        df_in[date_col_datetime] = pd.to_datetime(df_in[date_col])
        df_in[count_col] = df_in[count_col].fillna(0)
        df_out = df_in[[count_col, date_col_datetime]]

        return df_out

    def time_resampling(
        self, df_in: pd.DataFrame, resample: str = None
    ) -> pd.DataFrame:
        """Resamples df to the frequency specified by "resample".
            Used the column date_col_datetime as basis for the resample,
            and sets the resampled date_col_datetime as index
            "date_col" and "resample" are defined at the constructor through 'parameters'

        Parameters
        ----------
        df_in : pd.DataFrame
            DataFrame to be resampled
        resample : str, by default None
            Resampling period.  If not specified it uses the value defined in the constructor
            If not in the list defined by self._allowed_resamples, raises ValueError
        Returns
        -------
        pd.DataFrame
            Resample dataframe
        """
        date_col_datetime = self._date_col + self._datetime_suffix
        if resample is None:
            resample = self._resample
        if resample not in self._allowed_resamples:
            raise ValueError(
                f""" 'resample' can only take values of {self._allowed_resamples}.
'{resample}' was given"""
            )

        if not is_datetime(df_in[date_col_datetime]):
            df_in[date_col_datetime] = pd.to_datetime(df_in[date_col_datetime])
        return df_in.set_index(date_col_datetime).resample(resample).sum()
