from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from typing import Dict, Any


# Configuration des villes
CITIES = {
    "Bruxelles": {"lat": 50.8503, "lon": 4.3517},
    "Liege": {"lat": 50.6337, "lon": 5.5675},
    "Charleroi": {"lat": 50.4108, "lon": 4.4446},
    "Namur": {"lat": 50.4674, "lon": 4.8719},
    "La_Panne": {"lat": 51.1058, "lon": 2.5891},
    "Anvers": {"lat": 51.2194, "lon": 4.4025}
}

# Fonctions
def fetch_city_weather(city_name: str, lat: float, lon: float) -> Dict[str, Any]:
    """
    Appelle l'API Open-Meteo pour récupérer la météo actuelle d'une ville spécifique.

    Args:
        city_name: Nom de la ville pour le logging.
        lat: Latitude de la ville.
        lon: Longitude de la ville.

    Returns:
        Un dictionnaire contenant les données météo brutes (JSON).

    Raises:
        requests.exceptions.HTTPError: Si l'appel API échoue.
    """

    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
    response = requests.get(url)
    response.raise_for_status() # Erreur si l'API est down
    data = response.json()
    
    print(f"Météo à {city_name} : {data['current_weather']['temperature']}°C")
    return data




# DAG 
with DAG(
    dag_id='weather_ingestion_v1',
    start_date=datetime(2026, 4, 1),
    schedule='0 * * * *',              # @hourly en somme
    catchup=False,                              # Remonter les données n'a pas de sens
    default_args={
        'owner':'Vincent',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    doc_md="""
    ### DAG d'Ingestion Météo Belgique
    Ce DAG récupère les données météo en temps réel pour 6 villes belges majeures.
    - **Source**: Open-Meteo API
    - **Fréquence**: Horaire (minute 0)
    - **Villes**: Bruxelles, Liège, Charleroi, Namur, La Panne, Anvers.
    """
) as dag:


    for city, coords in CITIES.items():
        PythonOperator(
            task_id=f'fetch_{city.lower()}',
            python_callable=fetch_city_weather,
            op_kwargs={
                'city_name': city,
                'lat': coords['lat'],
                'lon': coords['lon']
            }
        )