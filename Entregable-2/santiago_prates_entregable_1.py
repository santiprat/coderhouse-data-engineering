'''
DATA ENGINEERING (FLEX) 2023

--Entregable 1--

AUTOR: Santiago Prates

FECHA LIMITE DEL ENTREGABLE: 16/05/2023 23:59

CONSIGNA: Desarrollar un script que extraiga datos de una API pública. 
          A su vez, el alumno debe crear una tabla en Redshift para posterior 
          carga de los datos extraidos.

FORMATO: Código en Python subido ya sea en repositorio de GitHub o en Google Drive.
         Y tabla creada en Redshift.

++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

API PARA OBTENER DATOS DE LA NBA: balldontlie.io


CASO DE ESTUDIO: OBTENER METRICAS DE JUGADORES DEL EQUIPO SAN ANTONIO SPURS PARA LA TEMPORADA 2017

JUGADOR - PLAYERID
Kawhi Leonard - 274
Brandon Paul - 2198
Derrick White - 473
Dejounte Murray - 334
Patty Mills - 319
Tony Parker - 363
Matt Costello - 2206 
Bryn Forbes - 159
LaMarcus Aldridge - 6
Danny Green - 184
Manu Ginobili - 1412
Rudy Gay - 171
Davis Bertans - 44
Pau Gasol - 170
Joffrey Lauvergne - 2118

'''

import requests, psycopg2, os, pandas as pd
from psycopg2.extras import execute_values


BASE_API_URL = 'https://www.balldontlie.io'

SEASON_AVERAGES_PATH = '/api/v1/season_averages'
SEASON_AVERAGES_COLUMNS_TO_KEEP = ['player_id', 'games_played', 'season', 'min', 'ftm', 'fgm', 'fg3m', 'dreb', 'oreb', 'ast', 'pf']
SEASON_AVERAGES_COLUMNS_RENAME = {
    'season': 'Temporada',
    'player_id' : 'PlayerID',
    'games_played': 'PartidosJugados',
    'min': 'MinutosPromedio', 
    'ftm': 'Libres',
    'fgm': 'Dobles',
    'fg3m': 'Triples',
    'dreb': 'RebotesDefensivos',
    'oreb': 'RebotesOfensivos',
    'ast' : 'Asistencias',
    'pf' : 'FaltasCometidas'

}

SEASON_AVERAGES_YEAR = [2015, 2016, 2017]
SEASON_AVERAGES_PLAYERS_IDS = [274, 2198, 473, 334, 319, 363, 2206, 159, 6, 184, 1412, 171, 44, 170, 2118] 


REDSHIFT_HOST = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
REDSHIFT_SCHEMA_NAME = 'santiago_prates_coderhouse'

DB_HOST = 'localhost'
DB_PORT = '5439'
DB_DATA_BASE = 'data-engineer-database'
DB_TABLE_NAME = 'average_player_season_stats'
DB_USER = 'santi_prates7_coderhouse'
DB_PWD_PATH = os.path.join(os.path.dirname(__file__), 'pwd_coder.txt')

TYPE_MAP = {'int64': 'INT','int32': 'INT','float64': 'FLOAT','object': 'VARCHAR(50)','bool':'BOOLEAN'}

class SeasonAverages:

    def __init__(self, season, player_id):
        self.endpoint = BASE_API_URL + SEASON_AVERAGES_PATH
        self.payload = {
            'season': season,
            'player_ids[]': player_id
        }

    # Obtener los datos de promedio de temporada de los jugadores
    def get_season_averages( self ):
        try:
            #Realizar una solicitud GET al endpoint de la API
            response = requests.get(self.endpoint, params=self.payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            # Capturar excepciones relacionadas con la solicitud HTTP
            print(f"REQUEST ERROR: {exc}")
            return None

    # Procesamiento de los datos obtenidos, filtrado, eliminacion de duplicados
    def process_data (self, season_averages):
        try:
            if 'data' in season_averages:
                data_frame = pd.DataFrame(season_averages['data'])

                data_frame_filtered = data_frame.loc[:, SEASON_AVERAGES_COLUMNS_TO_KEEP]
                data_frame_filtered.rename(columns=SEASON_AVERAGES_COLUMNS_RENAME, inplace=True)
                # Remover duplicados en data_frame
                data_frame_filtered = data_frame_filtered.drop_duplicates()

                return data_frame_filtered
        except KeyError as exc:
            print( f"DATA PROCESSING ERROR: {exc}" )
            return None
    
    # Funcion de conexion a la base de datos
    def db_connect (self):
        with open(DB_PWD_PATH) as f:
            PWD = f.read()
            
            try:
                conn = psycopg2.connect(
                    host = DB_HOST,
                    dbname = DB_DATA_BASE,
                    user = DB_USER,
                    password = PWD,
                    port = DB_PORT
                )
                return conn
            except Exception as exc:
                print ("UNABLE TO CONNECT TO DB SERVER.")
                print (exc)
            finally:
                f.close()
    
    # Funcion para enviar datos a la base de datos
    def send_data_to_server (self, conn, data_frame, season, table_name=DB_TABLE_NAME):
        try:
            # Definir formato SQL VARIABLE TIPO_DATO
            column_defs = [f"{name} {TYPE_MAP[str(dtype)]}" for name, dtype in zip(data_frame.columns, data_frame.dtypes)]

            # Crear la tabla si no existe
            cur = conn.cursor()
            cur.execute(f"CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA_NAME}.{table_name} ({', '.join(column_defs)}, PRIMARY KEY (PlayerID, Temporada));")

            # Generar los valores a insertar
            values = [tuple(x) for x in data_frame.to_numpy()]

            # Definir INSERT y CONTROLAR DUPLICADOS al insertar
            insert_sql = f"INSERT INTO {REDSHIFT_SCHEMA_NAME}.{table_name} ({', '.join(data_frame.columns)}) VALUES %s " \
                         f"ON CONFLICT DO NOTHING"

            # Ejecutar la transacción para insertar los datos
            cur.execute("BEGIN")
            execute_values(cur, insert_sql, values)
            cur.execute("COMMIT")
            print(f'PROCESS FINISHED FOR SEASON {season}')
        except Exception as exc:
            print(f"ERROR: {exc}")

for year in SEASON_AVERAGES_YEAR:
    season_avg_instance = SeasonAverages( year, SEASON_AVERAGES_PLAYERS_IDS )
    season_averages = season_avg_instance.get_season_averages()

    if season_averages is not None:
        print("SEASON AVERAGES RETRIEVED SUCCESFULLY.")
        # Procesar los datos obtenidos de la API
        processed_data = season_avg_instance.process_data( season_averages )
        if processed_data is not None:
            print("DATA PROCESSED SUCCESSFULLY.")
            # Conectar a la base de datos y enviar los datos procesados
            conn = season_avg_instance.db_connect()
            if conn:
                print("CONNECTED TO THE DATABASE SUCCESSFULLY.")
                season_avg_instance.send_data_to_server(conn, processed_data, year)
        else:
            print("ERROR PROCESSING DATA.")
    else:
        print("ERROR RETRIEVING SEASON AVERAGES.")