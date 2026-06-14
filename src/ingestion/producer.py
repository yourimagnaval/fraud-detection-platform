# -*- coding: utf-8 -*-
import json
import random
import time
from kafka import KafkaProducer

print("Démarrage du générateur de clics")

# Connexion à Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print(f"Impossible de se connecter à Kafka : {e}")
    exit(1)

topic_name = 'user-clickstream'

# Liste d'utilisateurs normaux
normal_users = [f"user_{i}" for i in range(1000, 1100)]

while True:
    # SIMULATION TRAFIC NORMAL
    for _ in range(random.randint(5, 15)):
        user = random.choice(normal_users)
        data = {
            "user_id": user,
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "ip_address": f"192.168.1.{random.randint(10, 250)}",
            "action": "click"
        }
        producer.send(topic_name, value=data)
    
    # SIMULATION D'ATTAQUES DE BOTS DYNAMIQUES (évenment aléatoire)
    if random.random() < 0.3:  # 30% de chance d'avoir une vague de fraude à chaque seconde
        bot_user = f"user_{random.randint(5000, 9999)}" 
        nombre_de_clics_du_bot = random.randint(11, 45) 
        
        for _ in range(nombre_de_clics_du_bot):
            data = {
                "user_id": bot_user,
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "ip_address": f"45.22.99.{random.randint(1, 254)}",
                "action": "click"
            }
            producer.send(topic_name, value=data)
            
        print(f"Attaque simulée pour {bot_user} : {nombre_de_clics_du_bot} clics envoyés.")

    producer.flush()
    time.sleep(1)  # Pause d'une seconde avant la prochaine vague