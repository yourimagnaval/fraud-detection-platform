# -*- coding: utf-8 -*-
import os
import subprocess
import pandas as pd
from datetime import datetime, timezone
from feast import FeatureStore

def initialize_infrastructure(current_dir):
    # Vérification et création du fichier Parquet historique pour Feast
    parquet_dir = os.path.abspath(os.path.join(current_dir, "../../data/offline_lakehouse"))
    parquet_path = os.path.join(parquet_dir, "dummy.parquet")

    if not os.path.exists(parquet_path):
        print(f"Création de la structure historique manquante dans : {parquet_dir}")
        os.makedirs(parquet_dir, exist_ok=True)
        
        initial_data = {
            "user_id": ["user_init"],
            "click_count": [0],
            "ip_address": ["127.0.0.1"],
            "is_blocked": [0],
            "blocking_reason": ["INITIALIZATION"],
            "event_timestamp": [datetime.now(timezone.utc)]
        }
        df = pd.DataFrame(initial_data)
        df.to_parquet(parquet_path, index=False)
        print("Fichier de référence dummy.parquet créé avec succès.")

def main():
    print("Initialisation du client Feast Feature Store")
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(current_dir)
    
    # Initialisation de l'infrastructure de fichiers
    initialize_infrastructure(current_dir)

    # Synchronisation du registre Feast
    print("Enregistrement du schéma via 'feast apply'...")
    try:
        subprocess.run(["feast", "apply"], check=True, shell=True)
        print("Registre Feast mis à jour.")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors du feast apply : {e}")

    # Matérialisation vers le Online Store (Redis)
    print("Déclenchement de la matérialisation Feast...")
    start_time = "2026-01-01T00:00:00"
    end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    try:
        subprocess.run(
            ["feast", "materialize", start_time, end_time],
            check=True,
            shell=True
        )
        print("Matérialisation réussie de PostgreSQL vers Redis.")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de la matérialisation : {e}")

    # INTERROGATION DU STORE EN LIGNE
    store = FeatureStore(repo_path=current_dir)
    target_user = "user_5777"

    entity_rows = [
        {
            "user_id": target_user,
        }
    ]

    features_to_fetch = [
        "user_fraud_features:click_count",
        "user_fraud_features:is_blocked",
        "user_fraud_features:blocking_reason"
    ]

    print(f"Interrogation de Redis pour l'utilisateur : {target_user}...")

    try:
        response = store.get_online_features(
            features=features_to_fetch,
            entity_rows=entity_rows
        ).to_dict()

        user_id_res = response["user_id"][0]
        click_count_res = response["click_count"][0]
        is_blocked_res = response["is_blocked"][0]
        blocking_reason_res = response["blocking_reason"][0]

        print("\n--- PROFIL UTILISATEUR FEAST ---")
        print(f"ID Utilisateur : {user_id_res}")
        print(f"Nombre de clics (10s) : {click_count_res}")
        print(f"Indicateur de blocage : {is_blocked_res}")
        print(f"Raison de la fraude   : {blocking_reason_res}")
        
        if is_blocked_res == 1 or (click_count_res and click_count_res > 10):
            print(f"STATUT : SUSPECT DE FRAUDE ({blocking_reason_res if blocking_reason_res else 'Alerte Clics'})")
        else:
            print("STATUT : Utilisateur Sain")
        print("-----------------------------------\n")

    except Exception as e:
        print(f"Erreur lors de la lecture des features : {e}")

if __name__ == "__main__":
    main()