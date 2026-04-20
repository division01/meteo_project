from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from datetime import datetime, timedelta
import requests
import json
import pendulum
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

# region VARIABLES 
BUCKET_NAME = "engie-weather-data-vincent"
CITIES_PATH = "static/cities/cities.csv"
AWS_CONN_ID = "aws_default"

# endregion VARIABLES


# region EXTRACT

@task
def get_cities_from_s3() -> List[Dict[str, Any]]:
    """
    Récupère le référentiel des villes depuis S3 et le convertit en dictionnaire.

    Cette fonction extrait un fichier CSV statique servant de table de dimension. 
    Le format attendu est : city_id, city_name, lat, lon.

    Returns:
        List[Dict[str, Any]]: Une liste de dictionnaires, où chaque dictionnaire 
        contient les paramètres d'une ville pour l'ingestion (name, lat, lon).
    """
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    content = s3.read_key(key=CITIES_PATH, bucket_name=BUCKET_NAME)
    
    # TODO : Rendre le parsing plus robuste via Pandas
    lines = content.splitlines()[1:] # On ignore le header
    cities_list = []
    for line in lines:
        c_id, name, lat, lon = line.split(',')
        cities_list.append({
            'city_name': name,
            'lat': float(lat),
            'lon': float(lon)
        })
    logger.info(f"Référentiel chargé : {len(cities_list)} villes trouvées.")
    return cities_list


def fetch_weather_data(lat: float, lon: float, date_str: str) -> Dict[str, Any]:
    """
    Appelle l'API Open-Meteo pour récupérer la météo à la date et heure d'une ville spécifique.

    Args:
        lat: Latitude de la ville.
        lon: Longitude de la ville.
        date_str: La logical date d'airflow au format YYYY-MM-DD.

    Returns:
        Un dictionnaire contenant les données météo brutes (JSON).

    Raises:
        requests.exceptions.HTTPError: Si l'appel API échoue.
    """

    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}&"
        f"start_date={date_str}&end_date={date_str}&hourly=temperature_2m,windspeed_10m,weathercode"
    )
    logger.debug(f"Appel API : {url}")
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# endregion EXTRACT

# region TRANSFORM
# endregion TRANSFORM   

# region LOAD
def save_to_s3(data: Dict[str, Any], city_name: str, l_date:pendulum.DateTime, quality:str, env:str="dev") -> str:
    """
    Sauvegarde les données sur S3 avec partitionnement Year/Month/Day/Hour.

    Args:
        data: Données météo brutes (JSON).
        city_name: Nom de la ville.
        l_date: La date logique d'airflow.
        quality: Archive/Bronze/Silver/Gold
        env: dev/test/prod

    Returns:
        S3_key:Chemin S3 où les données ont été stockées.
    """
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    # Création du chemin partitionné
    year = l_date.strftime('%Y')
    month = l_date.strftime('%m')
    day = l_date.strftime('%d')
    hour = l_date.strftime('%H')
    
    s3_key = f"weather/{env}/{quality}/year={year}/month={month}/day={day}/hour={hour}/{city_name.lower()}.json"
    
    s3.load_string(
        string_data=json.dumps(data),
        key=s3_key,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    return s3_key

# endregion LOAD

# region MAIN
@task(map_index_template="{{ task.op_kwargs['city_name'] }}")
def weather_pipeline_task(city_name: str, lat: float, lon: float, date_str: str, l_date: str) -> None:
    """
    Coordonne les étapes pour chaque instance de tâche.

    Args:
        city_name: Nom de la ville.
        lat: Latitude de la ville.
        lon: Longitude de la ville.
        date_str: La date logique d'airflow ( YYYY-MM-DD ).
        l_date: Le string de datetime pour le partitionnement S3.
    """
    logger.info(f"Démarrage de l'ingestion pour : {city_name}")

    # On transforme la string reçue d'Airflow en objet Pendulum (datetime)
    l_date_obj = pendulum.parse(l_date)

    full_day_data = fetch_weather_data(lat, lon, date_str)    

    path_archive = save_to_s3(full_day_data, city_name, l_date_obj, "archive")
    logger.info(f"Succès : {city_name} sauvegardé dans {path_archive}")
    path_bronze = save_to_s3(full_day_data, city_name, l_date_obj, "bronze")
    logger.info(f"Succès : {city_name} sauvegardé dans {path_bronze}")
    return path_bronze

# endregion MAIN


# region DAG
with DAG(
    dag_id='weather_belgium_backfill_partitioned',
    start_date=datetime(2026, 4, 1),
    schedule=30 23 * * *',
    catchup=True,
    default_args={
        'owner': 'Vincent',
        'retries': 2,
        'retry_delay': timedelta(seconds=30)
    }
) as dag:

    cities = get_cities_from_s3()

    # Dynamic Task Mapping 
    # Partial() permet de définir les valeurs communes a toutes les subtasks
    # Expand() permet de définir créer chaque sub task avec ses valeurs propres
    ingest_tasks = weather_pipeline_task.partial(
        date_str="{{ ds }}",                        # String YYYY-MM-DD pour l'appel API 
        l_date="{{ logical_date }}"                 # String datetime pour l'heure pour le partitionnement S3
    ).expand_kwargs(cities)


    athena_partition_query = """
    ALTER TABLE weather_db.weather_data 
    ADD IF NOT EXISTS PARTITION (
        year='{{ data_interval_start.year }}', 
        month='{{ data_interval_start.strftime("%m") }}', 
        day='{{ data_interval_start.strftime("%d") }}', 
        hour='{{ data_interval_start.strftime("%H") }}'
    ) 
    LOCATION 's3://engie-weather-data-vincent/weather/year={{ data_interval_start.year }}/month={{ data_interval_start.strftime("%m") }}/day={{ data_interval_start.strftime("%d") }}/hour={{ data_interval_start.strftime("%H") }}/';
    """

    # Tâche de synchronisation du catalogue Glue
    # TODO : Ajouter un AWS GLUE Crawler, la tâche est trop longue en case de backfill
    repair_athena_table = AthenaOperator(
        task_id='repair_athena_table',
        query=athena_partition_query,
        database='weather_db',
        aws_conn_id=AWS_CONN_ID,
        output_location=f's3://{BUCKET_NAME}/athena/' 
    )

    # On s'assure que toutes les tâches d'ingestion sont finies avant de repair
    ingest_tasks >> repair_athena_table

# endregion DAG 