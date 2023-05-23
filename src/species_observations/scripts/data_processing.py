import pandas as pd
import utils as utl

class Preprocessing:
    def __init__(self, parameters):
        self.full_cols = parameters['data_cols']
        self.date_col = self.full_cols['event_date']
        self.count_col = self.full_cols['individual_count']

    def preprocessing_time_data(self, df: pd.DataFrame) -> pd.DataFrame:
        date_col = self.date_col
        date_col_datetime = date_col + '_datetime'
        count_col = self.count_col

        if type(df) == dict:
            df = utl.PartitionedDS2df(df)

        df[date_col_datetime] = pd.to_datetime(df[date_col])
        df[count_col] = df[count_col].fillna(0)
        df_out = df[[count_col, date_col_datetime]]

        return df_out