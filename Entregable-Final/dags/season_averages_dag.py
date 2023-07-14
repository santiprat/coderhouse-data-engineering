'''
DATA ENGINEERING (FLEX) 2023
--Entregable Final--
AUTOR: Santiago Prates
FECHA LIMITE DEL ENTREGABLE: 14/07/2023 23:59

CONSIGNA:  Partiendo del último entregable, el script ya debería funcionar correctamente dentro de Airflow en un contenedor Docker. 
           En este entregable, añadiremos alertas en base a thresholds de los valores que estemos analizando.

FORMATO:   Dockerfile y código con todo lo necesario para correr (si es necesario incluir un manual de instrucciones o pasos para correrlo) 
           subido en repositorio de Github o en Google Drive.
           Proporcionar screenshot de un ejemplo de un correo enviado habiendo utilizado el código.

OBJETIVOS: Incorporar código que envie alertas mediante SMTP.
           Incorporar una forma fácil de configurar thresholds que van a ser analizados para el envio de alertas.

++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
API PARA OBTENER DATOS DE LA NBA: balldontlie.io
CASO DE ESTUDIO: OBTENER METRICAS DE JUGADORES DEL EQUIPO SAN ANTONIO SPURS PARA LA TEMPORADA 2015, 2016, 2017
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
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from email.message import EmailMessage
from airflow.models import DAG, Variable
import requests, json, psycopg2, os, pandas as pd
from psycopg2.extras import execute_values

import smtplib

# Obtengo valores de constantes en base a variables de entorno definidas en archivo .env
BASE_API_URL = os.getenv("BASE_API_URL")

SEASON_AVERAGES_PATH = os.getenv("SEASON_AVERAGES_PATH")
PLAYER_DATA_PATH = os.getenv("PLAYER_DATA_PATH")

SEASON_AVERAGES_COLUMNS_TO_KEEP = ['season', 'player_id', 'games_played', 'min', 'ftm', 'fgm', 'fg3m', 'dreb', 'oreb', 'ast', 'pf']
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

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_SCHEMA_NAME = os.getenv("REDSHIFT_SCHEMA_NAME")

DB_HOST =  os.getenv("DB_HOST")
DB_PORT =  os.getenv("DB_PORT")
DB_DATA_BASE =  os.getenv("DB_DATA_BASE")
DB_TABLE_NAME =  os.getenv("DB_TABLE_NAME")
DB_USER =  os.getenv("DB_USER")
THRESHOLD = os.getenv("PLAYER_THRESHOLD")

FROM_EMAIL = os.getenv("FROM_EMAIL")
TO_EMAIL = os.getenv("TO_EMAIL")
SMTP_PWD = os.getenv("SMTP_PWD")

DB_PWD_PATH = os.path.join(os.path.dirname(__file__), 'pwd_coder.txt')

TYPE_MAP = {'int64': 'INT','int32': 'INT','float64': 'FLOAT','object': 'VARCHAR(50)','bool':'BOOLEAN'}

class SeasonAverages:

    def __init__(self, season, player_id):
        self.season = season
        self.player_id = player_id
        self.endpoint = BASE_API_URL + SEASON_AVERAGES_PATH
        self.payload = {
            'season': season,
            'player_ids[]': player_id
        }
    
    # Obtener los datos de promedio de temporada de los jugadores
    def get_season_averages(self):
        try:
            # Realizar una solicitud GET al endpoint de la API para obtener los datos de temporada
            response = requests.get(self.endpoint, params=self.payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            # Capturar excepciones relacionadas con la solicitud HTTP
            print(f"REQUEST ERROR: {exc}")
            return None

    # Obtener información de jugadores específicos (NUEVO)
    def get_player_data(self, player_id):
        try:
            get_player_endpoint = f"{BASE_API_URL + PLAYER_DATA_PATH + str(player_id)}"
            response = requests.get(get_player_endpoint)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            print(f"REQUEST ERROR: {exc}")
            return None

    # Procesamiento de los datos obtenidos, filtrado, eliminacion de duplicados
    def process_data(self, season_averages, season):
        try:
            if 'data' in season_averages:
                data_frame = pd.DataFrame(season_averages['data'])
                data_frame_filtered = data_frame.loc[:, SEASON_AVERAGES_COLUMNS_TO_KEEP]
                data_frame_filtered.rename(columns=SEASON_AVERAGES_COLUMNS_RENAME, inplace=True)
                # Remover duplicados en data_frame
                data_frame_filtered = data_frame_filtered.drop_duplicates()

                # Obtener el timestamp actual
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # Agregar la columna de timestamp al DataFrame
                data_frame_filtered['Timestamp'] = timestamp
                
                # Itero sobre el dataframe filtrado para evaluar umbral de partidos jugados en la temporada
                for index, row in data_frame_filtered.iterrows():
                    player_id = row['PlayerID']
                    matches_played = row['PartidosJugados']
                    
                    # Evalúo el threshold utilizando el valor de PartidosJugados
                    if int(matches_played) < int(THRESHOLD):
                        player_data = self.get_player_data(player_id)
                        player_name = f"{player_data['last_name']}, {player_data['first_name']}"
                        
                        # Envío correo electrónico si el jugador en la temporada procesada no supera el umbral definido
                        enviar(player_name, season, matches_played)

                return data_frame_filtered
        except KeyError as exc:
            print(f"DATA PROCESSING ERROR: {exc}")
            return None
        
    # Funcion de conexion a la base de datos
    def db_connect(self):
        with open(DB_PWD_PATH) as f:
            PWD = f.read()
            try:
                conn = psycopg2.connect(
                    host=REDSHIFT_HOST,
                    dbname=DB_DATA_BASE,
                    user=DB_USER,
                    password=PWD,
                    port=DB_PORT
                )
                return conn
            except Exception as exc:
                print("UNABLE TO CONNECT TO DB SERVER.")
                print(exc)
            finally:
                f.close()

    # Funcion para enviar datos a la base de datos
    def send_data_to_server(self, conn, data_frame):
        try:
            # Definir formato SQL VARIABLE TIPO_DATO
            column_defs = [f"{name} {TYPE_MAP[str(dtype)]}" for name, dtype in zip(data_frame.columns, data_frame.dtypes)]
            
            # Crear la tabla si no existe
            cur = conn.cursor()
            cur.execute(f"CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA_NAME}.{DB_TABLE_NAME} ({', '.join(column_defs)}, PRIMARY KEY (PlayerID, Temporada));")
            
            # Generar los valores a insertar
            values = [tuple(x) for x in data_frame.to_numpy()]
            insert_sql = f"INSERT INTO {REDSHIFT_SCHEMA_NAME}.{DB_TABLE_NAME} ({', '.join(data_frame.columns)}) VALUES %s"

            # Ejecutar la transacción para insertar los datos
            cur.execute("BEGIN")
            execute_values(cur, insert_sql, values)
            cur.execute("COMMIT")
            print(f'PROCESS FINISHED FOR SEASON {self.season}')
        except Exception as exc:
            print(f"ERROR: {exc}")

# Obtiene y procesa los promedios de la temporada para los jugadores específicos
def execute_season_averages(season, player_ids):
    season_avg_instance = SeasonAverages(season, player_ids)
    season_averages = season_avg_instance.get_season_averages()
    if season_averages is not None:
        print("SEASON AVERAGES RETRIEVED SUCCESFULLY.")
        # Procesar los datos obtenidos de la API
        processed_data = season_avg_instance.process_data(season_averages, season)
        if processed_data is not None:
            print("DATA PROCESSED SUCCESSFULLY.")
            # Conectar a la base de datos y enviar los datos procesados
            conn = season_avg_instance.db_connect()
            if conn:
                print("CONNECTED TO THE DATABASE SUCCESSFULLY.")
                season_avg_instance.send_data_to_server(conn, processed_data)
        else:
            print("ERROR PROCESSING DATA.")
    else:
        print("ERROR RETRIEVING SEASON AVERAGES.")

# Función que envía correo electrónico cuando es requerido
def enviar(player_id, year, matches):
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()

        # Logueo con la cuenta que se enviará el correo
        x.login(FROM_EMAIL, SMTP_PWD)

        # Manejo de asunto y cuerpo del mensaje
        subject = 'Notificación SeasonAverages'
        body_text = f'El jugador {player_id} no supera los {THRESHOLD} partidos jugados ({matches}) en la temporada {year}'
        message = EmailMessage()
        message['Subject'] = subject
        message.set_content(body_text)

        # Envío de correo
        x.sendmail(FROM_EMAIL, TO_EMAIL, message.as_string())
        print("¡CORREO ENVIADO!")
        
        x.quit()
    except Exception as exception:
        print(exception)
        print('ERROR SENDING EMAIL')

default_args={
    'owner': 'SantiagoPrates'
}

with DAG(
    'season_averages_dag',
    description='DAG for retrieving and processing season averages',
    default_args= default_args,
    schedule_interval=None,
    start_date=datetime(2023, 7, 13),
    catchup=False
) as dag:
    # Tareas dinámicas
    # Itera sobre cada año definido en SEASON_AVERAGES_YEAR para procesar las estadísticas de temporada
    # También se define el task_id dinámicamente para cada temporada a procesar 
    # Defino los argumentos que recibe execute_season_averages para su ejecución 
    for year in SEASON_AVERAGES_YEAR:
        
        execute_season_averages_task = PythonOperator(
            task_id=f'execute_season_averages_{year}',
            python_callable=execute_season_averages,
            op_kwargs={'season': year, 'player_ids': SEASON_AVERAGES_PLAYERS_IDS}
        )
