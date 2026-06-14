# -*- coding: utf-8 -*-
from feast import FeatureStore
import os

print("🔍 Initialisation du client Feast Feature Store...")

# 1. Connexion au registre de Feast (il lit le fichier feature_store.yaml automatiquement)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
store = FeatureStore(repo_path=CURRENT_DIR)

# 2. On définit l'utilisateur dont on veut analyser le profil en temps réel
# (Mettons user_6778 ou un autre apparu dans ton terminal Flink)
target_user = "user_6778"

entity_rows = [
    {
        "user_id": target_user,
    }
]

# 3. Les variables (features) qu'on souhaite récupérer depuis Redis
features_to_fetch = [
    "user_fraud_features:click_count",
]

print(f"⚡ Interrogation de Redis pour l'utilisateur : {target_user}...")

try:
    # Récupération des données en ligne (Online Store) en moins de 10ms
    response = store.get_online_features(
        features=features_to_fetch,
        entity_rows=entity_rows
    ).to_dict()

    # 4. Affichage du résultat propre
    user_id_res = response["user_id"][0]
    click_count_res = response["click_count"][0]

    print("\n--- 📊 PROFIL UTILISATEUR FEAST ---")
    print(f"👤 ID Utilisateur : {user_id_res}")
    print(f"📈 Nombre de clics (10s) : {click_count_res}")
    
    if click_count_res and click_count_res > 10:
        print("🚨 STATUT : SUSPECT DE FRAUDE (Bloquer les transactions !)")
    else:
        print("✅ STATUT : Utilisateur Sain")
    print("-----------------------------------\n")

except Exception as e:
    print(f"❌ Erreur lors de la lecture des features : {e}")