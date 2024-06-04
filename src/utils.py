import os
import sys
import logging
from urllib.parse import quote
import sqlalchemy as sa
from sqlalchemy import text, inspect
import yaml
import pandas as pd
from pandas import DataFrame
from sqlalchemy import text, Engine, Connection, Table
import asyncio
from pandas.io.sql import SQLTable
from src.telegram_bot import enviar_mensaje
import subprocess
import re


act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, '..')
sys.path.append(proyect_dir)

log_dir = os.path.join(proyect_dir, 'log', 'logs_main.log')
yml_credentials_dir = os.path.join(proyect_dir, 'config', 'credentials.yml')

logging.basicConfig(
    level=logging.INFO,
    filename=(log_dir),
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt='%d-%b-%y %H:%M:%S'
)


class Engine_sql:

    def __init__(self, username: str, password: str, host: str, database: str, port: str = 3306) -> None:
        self.user = username
        self.passw = password
        self.host = host
        self.dat = database
        self.port = port

    def get_engine(self) -> Engine:
        return sa.create_engine(f"mysql+pymysql://{self.user}:{quote(self.passw)}@{self.host}:{self.port}/{self.dat}")

    def get_connect(self) -> Connection:
        return self.get_engine().connect()


with open(yml_credentials_dir, 'r') as f:

    try:
        config = yaml.safe_load(f)
        source1 = config['source1']
    except yaml.YAMLError as e:
        logging.error(str(e), exc_info=True)

engine_60 = Engine_sql(**source1)


class Load_raw:

    def __init__(self, file_name: str) -> None:
        self.mov = bool
        self.dir_path = '/home/miguel.hernandez/PROCESOS/CRUDOS_MOVISTAR_URUGUAY/data'
        self.file_name = file_name
        self.file_path = os.path.join(self.dir_path, self.file_name)
        self.table_dest = 'tb_consolidado_mayo'

    def to_sql_replace(self, table: SQLTable, con: Engine | Connection, keys: list[str], data_iter):

        satable: Table = table.table
        ckeys = list(map(lambda s: s.replace(' ', '_'), keys))
        data = [dict(zip(ckeys, row)) for row in data_iter]
        values = ','.join(f':{nm}' for nm in ckeys)
        stmt = f"REPLACE INTO {satable.name} VALUES ({values})"
        con.execute(text(stmt), data)

    def extract(self) -> DataFrame:

        try:
            df = pd.read_excel(self.file_path, header=0)
            logging.info(f'{len(df)}, {self.file_name}')
            return df
        except Exception as e:
            logging.error(str(e), exc_info=True)

            """asyncio.run(enviar_mensaje(
                f"\U0000274c error al extraer el archivo{self.file_name}: {str(e)}"))"""

    def transform(self, df: DataFrame) -> DataFrame:

        try:

            df.columns = df.columns.str.replace(' ', '_')
            df.insert(loc=15, column='PHONE_NUMBER2', value=None)
            df.insert(loc=17, column='CONTACTO_PRIMARIO2', value=None)
            df.insert(loc=19, column='tel12', value=None)
            df.insert(loc=21, column='tel22', value=None)
            numeros_separados = df['Otros_moviles_activos1'].str.split(
                '-', expand=True, n=19)
            for i in range(1, 21):
                df[f'Otros_moviles_activos{i}'] = numeros_separados[i-1]

            list_columns = ['PHONE_NUMBER',
                            'CONTACTO_PRIMARIO', 'tel1', 'tel2']

            """df[list_columns] = df[list_columns].astype(
                str).str.replace('/', ' ')
            df[list_columns] = df[list_columns].astype(
                str).str.replace('-', ' ')"""

            """for i in list_columns:
                print(df[f'{i}'])
                df[f'{i}'] = df[f'{i}'].str.replace(
                    '[a-zA-Z.]', '', regex=True)"""
            for i in list_columns:

                for index, row in df.iterrows():

                    numero = re.sub(r'[^0-9\/\- ]', '', str(row[f'{i}']))
                    df.at[index, f'{i}'] = numero

            for i in list_columns:

                for index, row in df.iterrows():

                    numero = re.sub(r'[^0-9\/\- ]', '', str(row[f'{i}']))
                    numero = numero.replace('-', ' ')
                    numero = numero.replace('/', ' ')

                    numero = numero.replace(' ', '')

                    if len(numero) <= 11:
                        df.at[index, f'{i}'] = numero
                    else:
                        pass

            for i in list_columns:

                for index, row in df.iterrows():
                    lista_numero = re.sub(r'[^0-9\/\- ]', '', str(row[f'{i}']))

                    lista_numero = lista_numero.split('/')

                    if len(lista_numero) == 2:
                        df.at[index, f'{i}'] = lista_numero[0]
                        df.at[index, f'{i}2'] = lista_numero[1]
                    else:
                        pass

            for i in list_columns:

                for index, row in df.iterrows():

                    numero = re.sub(r'[^0-9\/\- ]', '', str(row[f'{i}']))
                    numero = numero.replace('-', ' ')

                    numero = numero.split(' ')

                    if len(numero) == 2:
                        df.at[index, f'{i}'] = numero[0]
                        df.at[index, f'{i}2'] = numero[1]
                    elif len(numero) == 3:
                        if (len(str(numero[0])+str(numero[1])) <= 11):
                            df.at[index, f'{i}'] = str(
                                numero[0])+str(numero[1])
                            df.at[index, f'{i}2'] = str(numero[2])
                        elif (len(str(numero[1])+str(numero[2])) <= 11):
                            df.at[index, f'{i}'] = str(
                                numero[1])+str(numero[2])
                            df.at[index, f'{i}2'] = numero[2]
                        else:
                            pass
            print(numeros_separados)
            print(df)
            for i in df.columns:
                print(i)
            return df
        except Exception as e:

            logging.error(str(e), exc_info=True)
            """asyncio.run(enviar_mensaje(
                    f"\U0000274c error al tranformar el archivo{self.file_name}: {str(e)}"))"""

    def load(self, df: pd.DataFrame, engine: Connection) -> None:

        with engine as con:

            try:

                df.to_sql(self.table_dest, con=con, if_exists='replace',
                          index=False)

                logging.info(
                    f'\U0000303d Se cargan {(len_df := len(df))} datos')
                # asyncio.run(enviar_mensaje(f"Cargados {len_df} registros"))

            except Exception as e:

                logging.error(str(e), exc_info=True)
                """asyncio.run(enviar_mensaje(
                    f"\U0000274c error al cargar al servidor: {str(e)}"))"""

    def exec(self):
        self.load(self.transform(self.extract()), engine_60.get_connect())
        # self.load(self.transform(self.extract()), engine_138.get_connect())
        # self.validate(engine_138.get_connect())

    def verify(self):
        if self.file_name not in (os.listdir(self.dir_path)):
            logging.info(f"{self.file_name} sin base")
            pass
        else:
            self.exec()
