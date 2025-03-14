import pandas as pd 

def file_path(path: str):
     path: str = path
     return path


def create_dataframe_csv(path: str):
     df_csv: pd.DataFrame = pd.read_csv(path)
     return df_csv


def create_dataframe_json(path: str):
     df_json: pd.DataFrame = pd.read_json(path)
     return df_json
     

def list_columns_name(dataframe: pd.DataFrame ):
     list_columns_name = dataframe.columns()
     return list_columns_name

