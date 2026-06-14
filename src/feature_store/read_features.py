# -*- coding: utf-8 -*-
from feast import FeatureStore
import os

print("Initialisation du client Feast Feature Store")

# Connexion au registre de Feast (lecture du fichier feature_store.yaml de manière automatique)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
store = FeatureStore(repo_path=CURRENT_DIR)

# Définition de l'utilisateur dont avec analyse du profil en temps réel
# (Test avec user_6778)
target_user = "user_6778"

entity_rows = [
    {
        "user_id": target_user,
    }
]

# Variables  récupérées depuis Redis
features_to_fetch = [
    "user_fraud_features:click_count",
]

print(f"Interrogation de Redis pour l'utilisateur : {target_user}...")

try:
    # Récupération des données en ligne 
    response = store.get_online_features(
        features=features_to_fetch,
        entity_rows=entity_rows
    ).to_dict()

    # Affichage du résultat 
    user_id_res = response["user_id"][0]
    click_count_res = response["click_count"][0]

    print("\n---PROFIL UTILISATEUR FEAST ---")
    print(f"ID Utilisateur : {user_id_res}")
    print(f"Nombre de clics (10s) : {click_count_res}")
    
    if click_count_res and click_count_res > 10:
        print("STATUT : SUSPECT DE FRAUDE (Bloquer les transactions !)")
    else:
        print("STATUT : Utilisateur Sain")
    print("-----------------------------------\n")

except Exception as e:
    print(f"Erreur lors de la lecture des features : {e}")