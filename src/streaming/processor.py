# -*- coding: utf-8 -*-
import os
import sys
import time
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment

def main():
    os.environ["PYFLINK_CLIENT_EXECUTABLE"] = sys.executable
    os.environ["PYFLINK_PYTHON"] = sys.executable
    os.environ["AUTO_LOOPBACK_MODE"] = "true"

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)

    current_dir = os.path.dirname(os.path.abspath(__file__))
    jars = [
        "flink-sql-connector-kafka-3.0.1-1.18.jar",
        "flink-connector-jdbc-3.1.2-1.18.jar",
        "postgresql-42.7.3.jar"
    ]
    
    jar_urls = []
    for jar in jars:
        path = os.path.join(current_dir, jar)
        url = "file:///" + path.replace("\\", "/").lstrip("/")
        jar_urls.append(url)
        
    t_env.get_config().set("pipeline.jars", ";".join(jar_urls))

    print("Démarrage du moteur de détection des fraudes Flink SQL...")

    # SOURCE : Flux de transactions provenant de Kafka
    t_env.execute_sql("""
        CREATE TABLE kafka_transactions (
            transaction_id STRING,
            user_id STRING,
            product_id INT,
            amount DOUBLE,
            `timestamp` BIGINT,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'payment-transactions',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'fraud-detection-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # LOOKUP : Table des produits dans PostgreSQL (avec cache)
    t_env.execute_sql("""
        CREATE TABLE postgres_products (
            product_id INT,
            name STRING,
            category STRING,
            price DOUBLE,
            risk_score DOUBLE,
            PRIMARY KEY (product_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://localhost:5432/fraud_lakehouse',
            'table-name' = 'products',
            'username' = 'data_engineer',
            'password' = 'password123',
            'lookup.cache.max-rows' = '10000',
            'lookup.cache.ttl' = '30min'
        )
    """)

    # SINK : Destination des alertes historiques dans PostgreSQL
    t_env.execute_sql("""
        CREATE TABLE postgres_fraud_alerts (
            transaction_id STRING,
            user_id STRING,
            amount DOUBLE,
            product_name STRING,
            risk_score DOUBLE,
            fraud_reason STRING,
            PRIMARY KEY (transaction_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://localhost:5432/fraud_lakehouse',
            'table-name' = 'fraud_alerts',
            'username' = 'data_engineer',
            'password' = 'password123'
        )
    """)

    print("Pipeline de détection actif. Envoi des alertes vers PostgreSQL...")
    
    statement_set = t_env.create_statement_set()
    
    statement_set.add_insert_sql("""
        INSERT INTO postgres_fraud_alerts
        SELECT 
            t.transaction_id,
            t.user_id,
            t.amount,
            p.name AS product_name,
            p.risk_score,
            CASE 
                WHEN t.amount > 5000.0 THEN 'MONTANT_EXTREME'
                WHEN t.amount > 1000.0 AND p.risk_score > 0.5 THEN 'SCORE_RISQUE_ELEVE'
                ELSE 'SUSPECT'
            END AS fraud_reason
        FROM kafka_transactions AS t
        JOIN postgres_products FOR SYSTEM_TIME AS OF t.proc_time AS p
            ON t.product_id = p.product_id
        WHERE t.amount > 5000.0 
           OR (t.amount > 1000.0 AND p.risk_score > 0.5)
    """)
    
    job_client = statement_set.execute().get_job_client()
    
    if job_client is not None:
        print(f"Job Flink démarré avec succès sous l'ID : {job_client.get_job_id()}")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nArrêt du processeur demandé.")
            try:
                job_client.cancel().result()
                print("Job Flink arrêté proprement.")
            except Exception:
                print("L'environnement d'exécution s'est arrêté.")

if __name__ == "__main__":
    main()