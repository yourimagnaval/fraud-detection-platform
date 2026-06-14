# -*- coding: utf-8 -*-
from datetime import timedelta
from feast import Entity, Field, FeatureView, ValueType, FileSource
from feast.types import Int64, String

# Définition de l'entité clé
user_entity = Entity(
    name="user_id", 
    value_type=ValueType.STRING, 
    description="L'identifiant unique de l'utilisateur"
)

# Source de données historique (Offline Store)
dummy_source = FileSource(
    path="../../data/offline_lakehouse/dummy.parquet",
    timestamp_field="event_timestamp",
)

# Vue de fonctionnalités
user_fraud_features_view = FeatureView(
    name="user_fraud_features",
    entities=[user_entity],
    ttl=timedelta(minutes=10), # Les données expirent après 10 minutes d'inactivité
    schema=[
        Field(name="click_count", dtype=Int64),
        Field(name="ip_address", dtype=String),
    ],
    online=True, # Activation de la synchronisation avec Redis
    source=dummy_source,
)