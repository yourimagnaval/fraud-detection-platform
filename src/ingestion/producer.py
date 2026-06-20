# -*- coding: utf-8 -*-
import json
import random
import time
import psycopg2
from kafka import KafkaProducer

print("Démarrage du générateur hybride (Clics + Vraies Transactions)")

#Connexion PostgreSQL pour récupérer le catalogue de produits
try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="data_engineer",
        password="password123",
        database="fraud_lakehouse"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT product_id, name, price, risk_score FROM products;")
    products_catalog = cursor.fetchall()
    cursor.close()
    conn.close()
    print(f"Catalogue chargé avec succès : {len(products_catalog)} produits disponibles.")
except Exception as e:
    print(f"Impossible de charger le catalogue produits depuis Postgres : {e}")
    exit(1)

# Connexion à Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print(f"Impossible de se connecter à Kafka : {e}")
    exit(1)

topic_clicks = 'user-clickstream'
topic_payments = 'payment-transactions'

# Génération des utilisateurs
normal_users = [f"user_{i}" for i in range(1000, 1100)]

while True:
    current_ts = time.strftime('%Y-%m-%d %H:%M:%S')
    
    # --- SIMULATION DU TRAFIC NORMAL ---
    for _ in range(random.randint(5, 12)):
        user = random.choice(normal_users)
        
        # Flux 1 : Comportement de clic (Signal faible)
        click_data = {
            "user_id": user,
            "timestamp": current_ts,
            "ip_address": f"192.168.1.{random.randint(10, 250)}",
            "action": "click"
        }
        producer.send(topic_clicks, value=click_data)
        
        # Flux 2 : Transaction d'achat (Signal fort - 20% de chance qu'un clic mène à un achat)
        if random.random() < 0.2:
            prod = random.choice(products_catalog)
            payment_data = {
                "transaction_id": f"tx_{random.randint(100000, 999999)}",
                "user_id": user,
                "product_id": prod[0],
                "amount": prod[2],  # Le vrai prix issu de la DB
                "timestamp": int(time.time() * 1000)  # Timestamp epoch millisecondes requis par Flink
            }
            producer.send(topic_payments, value=payment_data)

    # --- SIMULATION DES ATTAQUES DE BOTS ---
    if random.random() < 0.25:  # 25% de chance d'attaque par seconde
        bot_user = f"user_{random.randint(5000, 9999)}"
        bot_ip = f"45.22.99.{random.randint(1, 254)}"
        
        # Un bot fait énormément de clics très vite
        nombre_de_clics = random.randint(15, 40)
        for _ in range(nombre_de_clics):
            click_data = {
                "user_id": bot_user,
                "timestamp": current_ts,
                "ip_address": bot_ip,
                "action": "click"
            }
            producer.send(topic_clicks, value=click_data)
            
        # Le bot passe immédiatement une commande suspecte (ex: montant énorme ou répétitif)
        prod = random.choice(products_catalog)
        payment_data = {
            "transaction_id": f"tx_{random.randint(100000, 999999)}",
            "user_id": bot_user,
            "product_id": prod[0],
            "amount": round(prod[2] * random.randint(5, 10), 2),  # Achats en grosse quantité
            "timestamp": int(time.time() * 1000)
        }
        producer.send(topic_payments, value=payment_data)
        print(f"ATTAQUE SOUDAINE : {bot_user} ({nombre_de_clics} clics + 1 achat groupé)")

    producer.flush()
    time.sleep(1)