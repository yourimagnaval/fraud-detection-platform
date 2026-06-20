# -*- coding: utf-8 -*-
import random
import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="fraud_lakehouse",
        user="data_engineer",
        password="password123",
        port="5432"
    )

def init_table():
    """Crée la table products si elle n'existe pas."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INT PRIMARY KEY,
            name VARCHAR(100),
            category VARCHAR(50),
            price DOUBLE PRECISION,
            risk_score DOUBLE PRECISION
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("Table 'products' vérifiée/créée avec succès.")

def init_fraud_table():
    """Crée la table fraud_alerts si elle n'existe pas."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fraud_alerts (
            transaction_id VARCHAR(50) PRIMARY KEY,
            user_id VARCHAR(50),
            amount DOUBLE PRECISION,
            product_name VARCHAR(100),
            risk_score DOUBLE PRECISION,
            fraud_reason VARCHAR(50),
            alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("Table 'fraud_alerts' vérifiée/créée avec succès.")

def generate_automated_catalog(num_products=200):
    """Génère dynamiquement et automatiquement un catalogue de produits réaliste."""
    categories = {
        "Electronique": {
            "items": ["PC Portable", "Smartphone", "Montre Connectee", "Casque Bluetooth", "Tablette", "Ecran 4K"],
            "price_range": (150.0, 1800.0),
            "risk_range": (0.15, 0.70)
        },
        "Maison": {
            "items": ["Aspirateur Robot", "Miroir LED", "Machine a Cafe", "Lampe Connectee", "Enceinte Intelligente"],
            "price_range": (40.0, 600.0),
            "risk_range": (0.05, 0.30)
        },
        "Mode": {
            "items": ["Sweat a Capuche", "Baskets de Course", "Veste Impermeable", "Sac a Dos Pro"],
            "price_range": (30.0, 250.0),
            "risk_range": (0.05, 0.20)
        },
        "Vouchers": {
            "items": ["Carte Cadeau Virtuelle", "Code Promo Premium", "Ticket Recharchement"],
            "price_range": (10.0, 200.0),
            "risk_range": (0.75, 0.95)
        }
    }

    suffixes = ["Pro", "X", "v2", "v4", "Elite", "Ultra", "Max", "Plus"]
    catalog = []

    for i in range(1, num_products + 1):
        cat_name = random.choice(list(categories.keys()))
        cat_data = categories[cat_name]
        
        base_item = random.choice(cat_data["items"])
        suffix = random.choice(suffixes)
        product_name = f"{base_item} {suffix} #{random.randint(10, 99)}"
        
        price = round(random.uniform(*cat_data["price_range"]), 2)
        risk_score = round(random.uniform(*cat_data["risk_range"]), 2)
        
        catalog.append({
            "product_id": i,
            "name": product_name,
            "category": cat_name,
            "price": price,
            "risk_score": risk_score
        })
        
    return catalog

def seed_catalog(products_list):
    """Remplit ou met à jour le catalogue dans PostgreSQL."""
    init_table()
    conn = get_connection()
    cursor = conn.cursor()
    
    for prod in products_list:
        cursor.execute("""
            INSERT INTO products (product_id, name, category, price, risk_score)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE 
            SET name = EXCLUDED.name, 
                category = EXCLUDED.category, 
                price = EXCLUDED.price, 
                risk_score = EXCLUDED.risk_score;
        """, (prod["product_id"], prod["name"], prod["category"], prod["price"], prod["risk_score"]))
        
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Catalogue enrichi et synchronisé avec {len(products_list)} produits dans PostgreSQL.")

if __name__ == "__main__":
    print("Exécution du générateur automatique de catalogue...")
    
    # Génération de 150 produits uniques
    AUTOMATED_LIST = generate_automated_catalog(num_products=150)
    seed_catalog(AUTOMATED_LIST)
    
    # Création de la table pour stocker les alertes de fraude
    init_fraud_table()