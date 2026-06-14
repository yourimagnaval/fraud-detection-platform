# -*- coding: utf-8 -*-
import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from collections import defaultdict
from datetime import datetime

print("Connexion a la base de donnees MongoDB (Port 27018)")
try:
    # Changement du port : 27018 
    mongo_client = MongoClient('mongodb://127.0.0.1:27018/', serverSelectionTimeoutMS=5000)
    db = mongo_client['fraud_platform']
    alerts_collection = db['alerts']
    mongo_client.server_info()
    print("Connection a MongoDB avec succes sur le port 27018.")
except Exception as e:
    print(f"Impossible de se connecter a MongoDB : {e}")
    exit(1)

print("Connexion au flux Kafka user-clickstream")
try:
    CONSUMER = KafkaConsumer(
        'user-clickstream',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
except Exception as e:
    print(f"Erreur de connexion a Kafka : {e}")
    exit(1)

user_clicks = defaultdict(list)
print("Moteur de detection actif (sauvegarde MongoDB active)")
print("------------------------------------------------------------")

try:
    for message in CONSUMER:
        event = message.value
        user_id = event['user_id']
        
        try:
            evt_time = datetime.fromisoformat(event['timestamp'].split('.')[0])
        except:
            evt_time = datetime.utcnow()
            
        now = datetime.utcnow()
        user_clicks[user_id].append(evt_time)
        user_clicks[user_id] = [t for t in user_clicks[user_id] if (now - t).total_seconds() <= 10]
        click_count = len(user_clicks[user_id])

        if click_count > 10:
            print(f"ALERTE FRAUDE : {user_id} ({click_count} clics/10s)")
            
            fraud_document = {
                "user_id": user_id,
                "click_count_10s": click_count,
                "detected_at": datetime.utcnow().isoformat(),
                "last_ip": event['ip_address'],
                "status": "A Traiter"
            }
            
            try:
                res = alerts_collection.insert_one(fraud_document)
                print(f"=> OK. ID MongoDB genere : {res.inserted_id}")
            except Exception as mongo_err:
                print(f"=> [ERREUR MONGO] Impossible d'ecrire : {mongo_err}")
        else:
            print(f"Activite normale : {user_id} ({click_count} clics/10s)")

except KeyboardInterrupt:
    print("\nMoteur de detection arrete proprement.")
