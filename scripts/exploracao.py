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
     


def drop_column(dataframe_csv: pd.DataFrame,column_name: str):
     dataframe = dataframe_csv.drop(columns=[column_name])
     return dataframe

def rename_column(dataframe: pd.DataFrame):
     dataframe =dataframe.rename(columns={"Nome do Item": "Nome do Produto","Classificação do Produto": "Categoria do Produto","Valor em Reais (R$)": "Preço do Produto (R$)","Nome da Loja": "Filial"})
     return dataframe


def contact_dataframe(dataframe_csv: pd.DataFrame,dataframe_json: pd.DataFrame):
     list_dataframe = [dataframe_csv,dataframe_json]
     dataframe_concat = pd.concat(list_dataframe)
     return dataframe_concat



def dataframe_to_csv(dataframe: pd.DataFrame,path:str):
     dataframe.to_csv(path)


if __name__ == "__main__":
     path_csv = file_path("../data_raw/dados_empresaB.csv")
     path_json = file_path("../data_raw/dados_empresaA.json")
     data_frame_csv = create_dataframe_csv(path_csv)
     data_frame_json = create_dataframe_json(path_json)
     data_frame_csv = drop_column(data_frame_csv,"Data da Venda")
     data_frame_csv = rename_column(data_frame_csv)
     new_data_frame_concat = contact_dataframe(data_frame_json,data_frame_csv)
     dataframe_to_csv(new_data_frame_concat, "../data_processed/dados_unificados.csv")  