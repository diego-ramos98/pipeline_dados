import mysql.connector
import pandas as pd

def connect_mysql(host: str,user: str, password: str):
    cnx = mysql.connector.connect(
        host = host,
        user = user,
        password = password
    )
    print(cnx)
    return cnx

def create_cursor(cnx):
    cursor = cnx.cursor()
    return cursor

def create_database(cursor,db_name: str):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
    print(f"Base de dados {db_name} criada")

def show_databases(cursor):
    cursor.execute(f"SHOW DATABASES")
    for x in cursor:
        print(x)

def create_table(cursor):
    cursor.execute(
        """
            CREATE TABLE  IF NOT EXISTS db_produtos_varejo.tb_produtos(
	                ID int AUTO_INCREMENT,
                    NOME_PRODUTO varchar(100),
                    CATEGORIA_PRODUTO varchar(60),
                    PRECO_PRODUTO float,
                    QUANTIDADE_ESTOQUE int,
                    FILIAL varchar(20),

                    primary key (id)
            )

        """
    )


def show_tables(cursor,database_name: str):
    cursor.execute(f"USE {database_name}")
    cursor.execute("SHOW TABLES")
    for x in cursor:
        print(x)



def read_csv(path: str):
    df = pd.read_csv(path)
    df = df.drop(columns=(["Unnamed: 0"]))
    return df

def insert_table(cnx,cursor,df: pd.DataFrame,db_name: str,table_name: str):
    data_list = [tuple(row) for _, row in df.iterrows()]

    sql = f"INSERT INTO {db_name}.{table_name} (ID,NOME_PRODUTO,CATEGORIA_PRODUTO,PRECO_PRODUTO,QUANTIDADE_ESTOQUE,FILIAL) VALUES(%s,%s,%s,%s,%s,%s)"

    cursor.executemany(sql,data_list)
    cnx.commit()
    print(f"{cursor.rowcount} registros inseridos na tabela {table_name}")


if __name__ == "__main__":
    cnx = connect_mysql("localhost","root","123456")
    cursor = create_cursor(cnx)
    create_database(cursor,"db_produtos_varejo")
    show_databases(cursor)
    create_table(cursor)
    show_tables(cursor,"db_produtos_varejo")
    data_frame = read_csv("../data_processed/dados_lojas_juntas.csv")
    insert_table(cnx,cursor,data_frame,"db_produtos_varejo","tb_produtos")




