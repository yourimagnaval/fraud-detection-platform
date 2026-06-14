# -*- coding: utf-8 -*-
from datetime import timedelta
from feast import Entity, Field, FeatureView, ValueType, FileSource
from feast.types import Int64, String

# 1. Définition de l'entité clé (notre table Redis sera indexée par utilisateur)
user_entity = Entity(
    name="user_id", 
    value_type=ValueType.STRING, 
    description="L'identifiant unique de l'utilisateur"
)

# 2. Source de données historique (Offline Store)
# Version mise à jour pour Feast 0.63+
dummy_source = FileSource(
    path="D:/fraud-detection-platform/data/offline_lakehouse/dummy.parquet",
    timestamp_field="event_timestamp",
)

# 3. La vue de fonctionnalités (Feature View)
# C'est ici qu'on déclare à Feast ce qu'on va stocker dans Redis
user_fraud_features_view = FeatureView(
    name="user_fraud_features",
    entities=[user_entity],
    ttl=timedelta(minutes=10), # Les données expirent après 10 minutes d'inactivité
    schema=[
        Field(name="click_count", dtype=Int64),
        Field(name="ip_address", dtype=String),
    ],
    online=True, # On active explicitement la synchronisation avec Redis
    source=dummy_source,
)