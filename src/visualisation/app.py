# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from redis import Redis
from feast import FeatureStore
import os
import time

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide", page_icon="🚨")
st.title("Plateforme de Détection de Fraude en Temps Réel")

# Connexions (Redis/Feast)
r = Redis(host='localhost', port=6379, db=0, decode_responses=True)

@st.cache_resource
def get_feast_store():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    repo_path = os.path.join(base_dir, "feature_store")
    return FeatureStore(repo_path=repo_path)

try:
    store = get_feast_store()
    st.success("Connection au Flux d'alertes et au feature store feast")
except Exception as e:
    st.error(f"Erreur de connexion : {e}")
    st.stop()

# Simulation de l'API de scoring ML
st.sidebar.header("Microservice de Scoring ML")
user_to_check = st.sidebar.text_input("ID Utilisateur à contrôler", "").strip()

if st.sidebar.button("Simuler une transaction"):
    if user_to_check:
        try:
            # TEST DIRECT REDIS : analyse d'utilisateur si il existe dans le flux d'alertes
            redis_key = f"fraud_detection_platform:user_fraud_features:{user_to_check}"
            clicks = r.hget(redis_key, "click_count")
            
            st.sidebar.write(f"**Données en temps réel pour {user_to_check} :**")
            
            if clicks is not None:
                st.sidebar.metric("Clics (10s)", f"{clicks} / 10 max")
                if int(clicks) > 10:
                    st.sidebar.error("TRANSACTION REJETÉE (Bot détecté)")
                else:
                    st.sidebar.success("TRANSACTION VALIDÉE")
            else:
                # Si pas dans Redis, requête feast standard
                feature_vector = store.get_online_features(
                    features=["user_fraud_features:click_count"],
                    entity_rows=[{"user_id": user_to_check}]
                ).to_dict()
                
                # Feast stocke les résultats avec le nom complet de la feature
                clicks_feast = feature_vector.get("click_count", [None])[0] or feature_vector.get("user_fraud_features:click_count", [None])[0]
                
                if clicks_feast is not None:
                    st.sidebar.metric("Clics (Feast)", f"{clicks_feast} / 10 max")
                    if int(clicks_feast) > 10:
                        st.sidebar.error("TRANSACTION REJETÉE (Bot détecté)")
                    else:
                        st.sidebar.success("TRANSACTION VALIDÉE")
                else:
                    st.sidebar.info("TRANSACTION VALIDÉE (Aucune activité récente)")
        except Exception as e:
            st.sidebar.error(f"Erreur de vérification : {e}")
    else:
        st.sidebar.warning("Veuillez entrer un ID.")

# AFFICHAGE DES ALERTES GLOBALES 
raw_alerts = r.lrange("live_alerts", 0, -1)

if not raw_alerts:
    st.info("En attente de données suspectes de la part d'Apache Flink...")
else:
    alerts_list = []
    for alert in raw_alerts:
        parts = alert.split("|")
        if len(parts) == 3:
            alerts_list.append({"Heure": parts[0], "user_id": parts[1], "click_count": int(parts[2])})
    
    df = pd.DataFrame(alerts_list)
    
    # Métriques
    col1, col2, col3 = st.columns(3)
    col1.metric("Total des Alertes", len(df))
    col2.metric("Fraudeurs Uniques", df['user_id'].nunique())
    col3.metric("Suspect Principal", f"{df['user_id'].value_counts().idxmax()} ({df['user_id'].value_counts().max()}x)")
    
    st.markdown("---")
    
    # Tableau
    st.write("###Liste détaillée des alertes (Flux Flink)")
    st.dataframe(df.rename(columns={"user_id": "ID Utilisateur", "click_count": "Nombre de Clics"}), use_container_width=True)

# Boucle de rafraîchissement 2s
time.sleep(2)
st.rerun()