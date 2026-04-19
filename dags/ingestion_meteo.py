from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from datetime import datetime, timedelta
import requests
import json
import pendulum
from typing import Dict, Any

### VARIABLES ###
BUCKET_NAME = "engie-weather-data-vincent"
CITIES = {
    "Bruxelles": {"lat": 50.8503, "lon": 4.3517},
    "Liege": {"lat": 50.6337, "lon": 5.5675},
    "Charleroi": {"lat": 50.4108, "lon": 4.4446},
    "Namur": {"lat": 50.4674, "lon": 4.8719},
    "La_Panne": {"lat": 51.1058, "lon": 2.5891},
    "Anvers": {"lat": 51.2194, "lon": 4.4025}
}

### FONCTIONS ###
# --- 1. EXTRACTION ---
def fetch_weather_data(lat: float, lon: float, date_str: str) -> Dict[str, Any]:
    """
    Appelle l'API Open-Meteo pour récupérer la météo à la date et heure d'une ville spécifique.

    Args:
        lat: Latitude de la ville.
        lon: Longitude de la ville.
        date_str: La logical date d'airflow.

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
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# --- 2. CHARGEMENT ---
def save_to_s3(data: Dict[str, Any], city_name: str, l_date:pendulum.DateTime) -> str:
    """
    Sauvegarde les données sur S3 avec partitionnement Year/Month/Day.

    Args:
        data: Données météo brutes (JSON).
        city_name: Nom de la ville.
        l_date: La date logique d'airflow.

    Returns:
        S3_key:Chemin S3 où les données ont été stockées.
    """
    s3 = S3Hook(aws_conn_id='aws_default')
    
    # Création du chemin partitionné
    year = l_date.strftime('%Y')
    month = l_date.strftime('%m')
    day = l_date.strftime('%d')
    hour = l_date.strftime('%H')
    
    s3_key = f"weather/year={year}/month={month}/day={day}/hour={hour}/{city_name.lower()}.json"
    
    s3.load_string(
        string_data=json.dumps(data),
        key=s3_key,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    return s3_key

# --- 3. ORCHESTRATION ---
def weather_pipeline_task(city_name: str, lat: float, lon: float, date_str: str, l_date: pendulum.DateTime) -> None:
    """
    Coordonne les étapes pour chaque instance de tâche.

    Args:
        city_name: Nom de la ville.
        lat: Latitude de la ville.
        lon: Longitude de la ville.
        date_str: La date logique d'airflow ( YYYY-MM-DD ).
        l_date: L'objet datetime pour le partitionnement S3.
    """
    data = fetch_weather_data(lat, lon, date_str)
    path = save_to_s3(data, city_name, l_date)
    print(f"Données pour {city_name} stockées dans : {path}")



### DAG ###
with DAG(
    dag_id='weather_belgium_v4_partitioned',
    start_date=datetime(2026, 4, 1),
    schedule='0 * * * *',
    catchup=True,
    default_args={
        'owner': 'Vincent',
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:

    for city, coords in CITIES.items():
        PythonOperator(
            task_id=f'ingest_{city.lower()}',
            python_callable=weather_pipeline_task,
            op_kwargs={
                'city_name': city,
                'lat': coords['lat'],
                'lon': coords['lon'],
                'date_str':"{{ ds }}",          # String YYYY-MM-DD pour l'appel API
                'l_date': "{{ logical_date }}"  # Objet datetime pour le partitionnement S3
            }
        )

    # Tâche de synchronisation du catalogue Glue
    repair_athena_table = AthenaOperator(
        task_id='repair_athena_table',
        query='MSCK REPAIR TABLE weather_db.weather_data;',
        database='weather_db',
        aws_conn_id='aws_default',
        output_location=f's3://{BUCKET_NAME}/athena/' 
    )

    # On s'assure que toutes les tâches d'ingestion sont finies avant de repair
    # La syntaxe [liste_de_taches] >> repair signifie "attendre tout le monde"
    dag.tasks[:-1] >> repair_athena_table