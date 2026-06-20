# -*- coding: utf-8 -*-
from datetime import timedelta
from feast import Entity, Field, FeatureView, ValueType
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource
from feast.types import Int64, String

user_entity = Entity(
    name="user_id", 
    value_type=ValueType.STRING, 
    description="L'identifiant unique de l'utilisateur"
)

# La requête fournit user_id, is_blocked, blocking_reason, click_count et event_timestamp
postgres_source = PostgreSQLSource(
    name="postgres_fraud_source",
    query="SELECT user_id, 1 as is_blocked, fraud_reason as blocking_reason, 12 as click_count, CURRENT_TIMESTAMP as event_timestamp FROM fraud_alerts",
    timestamp_field="event_timestamp"
)

user_fraud_features_view = FeatureView(
    name="user_fraud_features",
    entities=[user_entity],
    ttl=timedelta(days=1),
    schema=[
        Field(name="click_count", dtype=Int64),
        Field(name="is_blocked", dtype=Int64),
        Field(name="blocking_reason", dtype=String),
    ],
    online=True,
    source=postgres_source,
)