# -*- coding: utf-8 -*-
import os
import time
from pyflink.table import EnvironmentSettings, TableEnvironment
from redis import Redis

print("Démarrage du moteur temps réel Apache Flink (Mode hybride Python-Redis)")

# Connexion directe à Redis
try:
    r = Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.ping()
    print("Connexion directe Python <-> Redis réussie !")
except Exception as e:
    print(f"Impossible de se connecter à Redis : {e}")
    exit(1)

# Initialisation de l'environnement Flink en mode Streaming
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(settings)

# Configuration du JAR kafka
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
kafka_jar_local = os.path.join(CURRENT_DIR, "flink-sql-connector-kafka-3.0.1-1.18.jar")
clean_path = kafka_jar_local.replace('\\', '/')
kafka_jar_url = f"file:///{clean_path}"

t_env.get_config().get_configuration().set_string(
    "pipeline.jars", kafka_jar_url
)
print(f"JAR Kafka chargé depuis : {kafka_jar_url}")

# Définition de la table Source
source_ddl = """
    CREATE TABLE kafka_src (
        user_id STRING,
        `timestamp` STRING,
        ip_address STRING,
        action STRING,
        ts AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'user-clickstream',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'flink-fraud-group',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
"""
t_env.execute_sql(source_ddl)
print("Connexion Flink / Kafka établie.")

# Requete de détection : basé sur une fenêtres de 10 secondes
fraud_detection_query = """
    SELECT 
        user_id,
        CAST(COUNT(action) AS STRING) as click_count
    FROM kafka_src
    GROUP BY 
        user_id, 
        TUMBLE(ts, INTERVAL '10' SECOND)
    HAVING COUNT(action) > 10
"""

print("Moteur de règles Flink activé. Surveillance du Feature Store en cours")

# Exécution et interception du flux
table_result = t_env.execute_sql(fraud_detection_query)

# collect permet de lire le flux d'alertes généré par flink en temps réel
with table_result.collect() as results:
    for result in results:
        user_id = result[0]
        click_count = result[1]
        timestamp = time.strftime('%H:%M:%S', time.localtime())
        
        # format de clé attendu par Feast pour Feature View
        redis_key = f"fraud_detection_platform:user_fraud_features:{user_id}"
        
        try:
            # Mise à jour Feast (permettant au modèles ML d'interroger Redis)
            r.hset(redis_key, mapping={"click_count": click_count})
            
            # Envoie du message d'alerte dans la liste globale lue par Dashboard Streamlit
            alert_payload = f"{timestamp}|{user_id}|{click_count}"
            r.lpush("live_alerts", alert_payload)
            r.ltrim("live_alerts", 0, 10000)  # Affichage de 10000 alertes les plus fraîches
            
            print(f"ALERTE ENVOYÉE -> {user_id} : {click_count} clics")
            
        except Exception as redis_err:
            print(f"Erreur d'écriture Redis : {redis_err}")