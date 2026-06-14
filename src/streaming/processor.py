# -*- coding: utf-8 -*-
import os
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

def build_kafka_source() -> KafkaSource:
    """Configure la source Kafka pour lire les transactions en temps réel."""
    # Définition du schéma des données attendues depuis Kafka
    row_type_info = Types.ROW_NAMED(
        ["transaction_id", "user_id", "amount", "country", "timestamp"],
        [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.STRING(), Types.LONG()]
    )
    
    json_deserializer = JsonRowDeserializationSchema.builder() \
        .type_info(row_type_info) \
        .build()

    # Configuration de la source Kafka (Pointant vers futur cluster Docker)
    return KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("payment-transactions") \
        .set_group_id("fraud-detection-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .setValueOnlyDeserializer(json_deserializer) \
        .build()

def main():
    # Initialisation de l'environnement d'exécution Flink
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Récupération des JARs nécessaires pour la connexion Kafka
    kafka_jar = "file://" + os.path.abspath("backends/flink/flink-sql-connector-kafka-3.1.0-src.jar")
    env.add_jars(kafka_jar)

    print("Démarrage du moteur de streaming Flink...")

    # Création du flux à partir de Kafka
    kafka_source = build_kafka_source()
    stream = env.from_source(
        kafka_source, 
        WatermarkStrategy.no_watermarks(), 
        "Kafka-Payment-Source"
    )

    # Affichage temporaire dans les logs pour le développement
    stream.print()

    # Lancement de l'exécution continue
    env.execute("Fraud-Detection-Streaming-Job")

if __name__ == "__main__":
    main() 