# 🌦️ Pipeline ETL Météo Belgique

Ce projet implémente un pipeline de données complet (**ETL**) pour collecter, stocker et analyser les données météo en temps réel pour 6 villes belges.

**Architecture :** `Airflow (WSL)` ➔ `Amazon S3` ➔ `AWS Glue/Athena` ➔ `DBeaver`

---

## Architecture Technique

**Extraction (Python/Airflow) :** Récupération des données via l'API Open-Meteo.  
**Stockage (AWS S3) :** Persistance des données au format JSON avec un partitionnement optimisé.  
**Catalogue (AWS Glue) :** Définition du schéma et gestion des partitions.  
**Analyse (AWS Athena) :** Requêtage SQL direct sur le Data Lake.  
**IDE SQL (DBeaver) :** Requêtage SQL direct sur le Data Lake depuis un IDE SQL open source.  
**Visualisation (PowerBI) :** Mise en place de visuels pour mieux comprendre la donnée.   



---

## Prérequis Système
Pour répliquer cet environnement, les composants suivants sont nécessaires :

- Système d'exploitation : WSL2 (Ubuntu 22.04+) ou Linux Natif.
- Langage : Python 3.10 ou supérieur.
- Infrastructure : Un compte AWS avec les droits AdministratorAccess (pour le POC) ou des politiques restreintes sur S3, Glue et Athena.
- Visualisation : Power BI Desktop (Windows) et le driver Simba Athena ODBC installé.

---

## Installation et Configuration

### 1. Environnement Local (WSL / Ubuntu)

Il est important de noter, avant d'aller plus loin, que le choix du local vise à limiter la difficulté technique pour un temps réduit. 
Pour ce POC, l'installation native sur WSL a été privilégiée pour maximiser la vitesse de déploiement. Une version conteneurisée (Docker) est prévue dans la roadmap pour faciliter la portabilité 
  

Le projet utilise Airflow 2.10.5 pour garantir la stabilité des composants AWS.

```bash
# Initialisation de l'environnement
mkdir meteo_project && cd meteo_project
python3 -m venv airflow_env
source airflow_env/bin/activate

# Installation d'Airflow
pip install "apache-airflow[amazon,pandas,requests]==2.10.5" \
  --constraint "[https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.10.txt](https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.10.txt)"

# Configuration Airflow
export AIRFLOW_HOME=~/airflow
airflow db init

# Créer l'utilisateur admin
airflow users create --username admin --firstname Firstname --lastname Lastname --role Admin --email admin@example.com
```


Nous utilisons des liens symboliques pour séparer le code source du dossier technique d'Airflow.

```bash
mkdir -p ~/meteo_project/dags
ln -s ~/meteo_project/dags/ingestion_meteo.py ~/airflow/dags/ingestion_meteo.py
```

Notre dossier dags pourra donc être sauvegardé sur git, sans que les autres fichiers du repo ne soient dans dans le dossier dags d'Airflow.
Cette méthode de liens symboliques permet de maintenir le code source sous versioning Git sans polluer le répertoire d'installation d'Airflow.
  


### 2. Configuration Cloud (AWS)

1. **Création d'un user IAM :**
   - Créez un compte AWS si vous ne l'avez pas déjà.
   - Créez un user IAM avec les permissions nécessaires pour accéder à S3, Glue et Athena.
   - Notez les credentials (access key et secret key).

2. **Connexion Airflow vers AWS :**
    - Dans l'interface Airflow (Admin > Connections), configurer aws_default :
    - Conn Type : Amazon Web Services
    - Login : VOTRE_ACCESS_KEY
    - Password : VOTRE_SECRET_KEY
    - Extra : {"region_name": "eu-central-1"}

3. **Configuration du bucket S3 :**
    - Créer un bucket S3 sur AWS

### 3. Analyse des données (SQL)  

#### Configuration Athena

Exécutez ces commandes dans l'éditeur de requêtes AWS Athena :

```SQL
CREATE DATABASE IF NOT EXISTS weather_db;
```
Cette première entrée permet de créer la base de données si elle n'existe pas encore.

```SQL
CREATE EXTERNAL TABLE IF NOT EXISTS weather_db.weather_data (
    latitude double,
    longitude double,
    current_weather struct<
        temperature: double,
        windspeed: double,
        time: string
    >
)
PARTITIONED BY (year string, month string, day string, hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://VOTRE-NOM-DE-BUCKET/weather/';
```
Cette seconde entrée permet de créer la table externe si elle n'existe pas encore. 
La partition à la fin permet d'expliquer la structure de nos données dans l'espace de stockage (qu'on sauvegarde partionné par Années/Mois/Jours.)  
Il faut remplacer la LOCATION par votre bucket S3 créé précédemment.  
Si c'est un nouveau compte AWS ou qu'Athena n'a jamais été utilisé avant, il faut aussi définir un bucket pour les opérations d'Athena, une banderole bleue devrait vous amener à l'endroit où le faire, vous pouvez créer un S3 spécifique, ou créer un dossier dans le S3 précédemment créé. 
  
La ligne ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' est spécifique a Hive et Amazon Athena pour lire et écrire des données au format JSON. Nos données étant sauvegardées sous ce format dans le S3.

```SQL
MSCK REPAIR TABLE weather_db.weather_data;
```
Cette entrée permet a Athena qui a maintenant la database et la table, de retourner sur les données et ajouter celles qu'elle peut dedans.

```SQL
SELECT 
    year, month, day, hour, 
    current_weather.temperature AS temp,
    current_weather.windspeed AS vent
FROM weather_db.weather_data
ORDER BY hour DESC
LIMIT 10;
```
Et cette dernière entrée permet de vérifier si vos données sont dedans une fois tout mis en place. 

### Configuration DBeaver

Cette section décrit la procédure pour requêter les données du bucket S3 directement depuis une interface SQL classique (DBeaver).  
Nous avons 3 prérequis à considérer, mais qui sont déjà en place :  
- Access Key & Secret Key : Tes identifiants IAM (les mêmes que pour Airflow).
- Région AWS : Celle de ton bucket (ex: eu-central-1 pour Francfort).
- S3 Output Location : Athena a besoin d'un dossier S3 pour stocker les résultats de tes requêtes SQL.
    - Format attendu : s3://ton-nom-de-bucket/athena-results/ (Crée ce dossier vide dans S3 si besoin).
  
#### Dans DBeaver

1. **Créer une nouvelle connexion :**
Files > New > Dbeaver > Database Connection > Athena
2. **Paramètres principaux :**  
    - Region: Entre le code de ta région (ex: eu-central-1).
    - S3 Output Location: Colle l'URL de ton dossier de résultats (ex: s3://athena-bucket-vincent/results/).
    - Username: Ton Access Key
    - Password: Ta Secret Key
3. **Tester la connexion :**  
Dans mon cas je n'avais pas donné les permissions Athena a mon user. J'ai dû aller modifier les permissions de mon utilisateur dans l'onglet IAM.  
Soit vous aurez une erreur, soit vous allez voir votre base de données apparaitre.  
Une petite requête SQL peut vous permettre de voir vos données et vous assurer que tout est en ordre : 
```SQL 
SELECT * FROM weather_db.weather_data 
ORDER BY year DESC, month DESC, day DESC, hour DESC 
LIMIT 10;
```

Side note intéressante. Ici j'ai compté le nombre de lignes et j'en avais 12. J'ai laissé airflow tourner toute la nuit, et je devais en avoir plus. J'ai relancé le MSCK REPAIR, et j'en avais 84.  
C'était donc la confirmation que Athena étant un moteur Schema-on-Read, il ne détecte pas automatiquement les nouvelles partitions ajoutées directement sur S3. L'ajout du MSCK REPAIR en fin de DAG garantit la mise à jour du catalogue Glue.. J'ai donc ajouté le MSCK REPAIR en fin du dag. C'est une solution provisoire puisque l'utilisation d'un AWS Glue Crawler planifié est préférable à une exécution manuelle. A voir laquelle est la plus économique au long terme.

### Création d'une vue 

Une fois nos données disponibles et en place, pour une meilleur visualisation de nos données j'ai créé une vue : 

```SQL 

CREATE OR REPLACE VIEW weather_db.view_weather_metrics AS 
SELECT 
    CAST(year AS INTEGER) as year,
    CAST(month AS INTEGER) as month,
    CAST(day AS INTEGER) as day,
    CAST(hour AS INTEGER) as hour,
    CAST(year || '-' || month || '-' || day || ' ' || hour || ':00:00' AS TIMESTAMP) as full_timestamp,
    current_weather.temperature as temperature_c,
    current_weather.windspeed as wind_speed_kmh,
    current_weather.weathercode as weather_condition_code,
    latitude,
    longitude
FROM weather_db.weather_data;
```
Le résultat obtenu de cette vue est le suivant.  


![alt text](images_readme/vue_weather_metrics.png)

On voit déjà que les coordonnées sont un plus mais on n'a pas le nom des villes qu'on pourra intuitivement rajouter.  
A noter cette requête SQL pourrait régler le problème dans la création de la vue : 
```SQL
SELECT 
    -- Mapping des coordonnées vers les noms de villes
	CASE 
        WHEN ROUND(latitude, 2) = 50.85 AND ROUND(longitude, 2) = 4.35 THEN 'Bruxelles'
        WHEN ROUND(latitude, 2) = 51.21 AND ROUND(longitude, 2) = 4.41 THEN 'Anvers'
        WHEN ROUND(latitude, 2) = 50.40 AND ROUND(longitude, 2) = 4.44 THEN 'Charleroi'
        WHEN ROUND(latitude, 2) = 51.05 AND ROUND(longitude, 2) = 3.73 THEN 'Gand'
        WHEN ROUND(latitude, 2) = 50.64 AND ROUND(longitude, 2) = 5.57 THEN 'Liège'
        WHEN ROUND(latitude, 2) = 50.48 AND ROUND(longitude, 2) = 4.87 THEN 'Namur'
        WHEN ROUND(latitude, 2) = 51.09 AND ROUND(longitude, 2) = 2.58 THEN 'La Panne'
        ELSE 'Inconnue (' || CAST(latitude AS VARCHAR) || ',' || CAST(longitude AS VARCHAR) || ')'
    END as ville,
    CAST(year AS INTEGER) as year,
    ...
```

Mettre cette query en place impliquerait un changement de la vue à chaque fois qu'on change une ville, que ce soit l'ajouter ou l'enlever. Minimiser le nombre d'endroits où on va devoir modifier du code me semble plus adéquat.  
Ce POC est de plus à visée éducative, par conséquent il y a un intérêt le faire dans PowerBI.  



## Power BI

### Importer les données

> **Note** : On va parler d'une première utilisation ici. Si vous avez déjà utilisé PowerBI passez à la suite.

Il faudra télécharger un driver JDBC/ODBC pour communiquer avec Athena. [Driver Athena ODBC](https://docs.aws.amazon.com/athena/latest/ug/connect-with-odbc.html )   

Une fois installé, tu dois déclarer ta connexion au niveau de Windows :
- Appuie sur la touche Windows et tape "Sources de données ODBC (64 bits)". Ouvre-le.
- Sous l'onglet DSN utilisateur, clique sur Ajouter.
- Choisis Simba Athena ODBC Driver et clique sur Terminer.
- Dans la fenêtre qui s'ouvre :
    - Data Source Name : Tape Simba Athena (c'est ce nom que tu mettras dans Power BI).
    - AwsRegion : eu-central-1.
    - S3 Output Location : Ton bucket de résultats (ex: s3://ton-bucket-athena-results/).
    - Authentication Type : Choisis IAM Credentials.
    - User / Password : Mets ton Access Key et ta Secret Key.
- Clique sur Test pour valider.

> **Note** : Parenthèse installation terminée

Dans Power BI, on importe les données via Amazon Athena.  
Dans le champ DSN, rentrer Simba Athena (ou votre DSN si vous n'avez pas suivi l'étape précédente).  
Pour le choix importer vs DirectQuery c'est propre au cas d'utilisation, le DirectQuery va passer pas Athena a chaque clic et 
donc amener des coûts supplémentaires. C'est donc une décision à faire avec le business.

### Traitement des données

Le traitement via l'éditeur Power Query (bouton 'Transformer les données') a été préféré aux colonnes calculées DAX.  
Cela permet d'effectuer les transformations lors de l'ingestion (couche de transformation amont), optimisant ainsi la compression du moteur VertiPaq.  

Dans le cadre de ce projet, je dois créer une table cities, et créer une clé (que j'utilise en concaténant les coordonnées) pour lier chaque entrée à un nom de ville selon ses coordonnées. J'aurais pu le faire en Pandas en amont, ou en SQL.  
Je peux aussi le faire en DaX derrière mais ça alourdit le rapport PowerBI et c'est moins optimal au niveau calcul.

### Visuels 

Il n'y a pas vraiment de documentation à donner ici pour les visuels.  
Suivre un cours sur les différents visuels, ou juste passer du temps avec permet de mieux les comprendre.  
De plus, ce genre de rapport se fait main dans la main avec le business. Ici ceux choisi ne l'ont été que pour un POC.


###  Roadmap Technique (Évolutions)
Cette section présente les axes d'amélioration identifiés pour passer du POC à une solution de production résiliente.



| Phase | Objectif | Description |
| :--- | :--- | :--- |
| **P1** | **Résilience** | Migration vers `logical_date` (Idempotence) & Stratégie de Backfill. |
| **P2** | **Optimisation** | Passage au format **Parquet** (Silver) & Partitionnement dynamique. |
| **P3** | **Qualité** | Tests des données tout au long du process. |
| **P4** | **DevOps** | Déploiement via **Docker** & CI/CD GitHub Actions. |