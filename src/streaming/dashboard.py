# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from pymongo import MongoClient
import time

# Configuration de la page web
st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")

st.title("Plateforme de Détection de Fraude en Temps Réel")
st.subheader("Suivi des alertes stockées dans MongoDB")

# Connexion à MongoDB (Port 27018 comme configuré ensemble)
@st.cache_resource
def init_connection():
    return MongoClient('mongodb://127.0.0.1:27018/')

try:
    client = init_connection()
    db = client['fraud_platform']
    collection = db['alerts']
except Exception as e:
    st.error(f"Impossible de se connecter à MongoDB : {e}")
    st.stop()

# Système de rafraîchissement automatique toutes les 2 secondes
placeholder = st.empty()

while True:
    # Récupération des données depuis MongoDB
    data = list(collection.find())
    
    with placeholder.container():
        if len(data) == 0:
            st.info("Aucune alerte détectée pour le moment. En attente de données...")
        else:
            # Conversion des données en DataFrame Pandas pour manipulation facile
            df = pd.DataFrame(data)
            # On supprime l'ID interne de Mongo pour l'affichage
            if '_id' in df.columns:
                df = df.drop(columns=['_id'])
            
            # --- SECTION 1 : LES METRIQUES ---
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(label="Total des Alertes", value=len(df))
            with col2:
                # Nombre d'utilisateurs uniques suspectés
                unique_users = df['user_id'].nunique()
                st.metric(label="Fraudeurs Uniques", value=unique_users)
            with col3:
                # Le pire fraudeur
                top_fraudster = df['user_id'].value_counts().idxmax()
                fraud_count = df['user_id'].value_counts().max()
                st.metric(label="Suspect principal", value=f"{top_fraudster} ({fraud_count}x)")
            
            st.markdown("---")
            
            # --- SECTION 2 : LE TABLEAU DE DONNÉES ---
            st.write("### Liste détaillée des alertes de fraude")
            # Inverser l'ordre pour voir les plus récentes en premier
            st.dataframe(df.iloc[::-1], use_container_width=True)
            
    # Pause de 2 secondes avant la prochaine mise à jour
    time.sleep(2)