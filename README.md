# Real-Time Fraud Detection Platform

Une plateforme industrielle et scalable de détection de fraude aux cartes bancaires en temps réel. Ce projet combine le traitement de flux d'événements à haute vélocité, une infrastructure MLOps moderne pour la gestion des caractéristiques (features), et des pratiques DevOps rigoureuses.

---

## Architecture Technique

Le système est conçu autour de quatre piliers techniques majeurs :

1. **Ingestion & In-Flight Processing (Vrai Temps Réel) :** Captation des flux d'événements continus avec **Apache Kafka** et traitement événement par événement (Stateful Stream Processing) via **Apache Flink** à l'aide de fenêtres glissantes.
2. **Infrastructure MLOps (Feature Store) :** Centralisation, versioning et service des caractéristiques à l'aide de **Feast**, avec une synchronisation automatique vers un stockage basse latence (**Redis**).
3. **Stockage Lakehouse & Gouvernance :** Archivage des données froides au format ouvert **Apache Iceberg**, couplé à des pipelines de validation de la qualité et du schéma des données avec **Great Expectations**.
4. **CI/CD & Chaos Engineering :** Automatisation complète des tests et déploiements via **GitHub Actions** et conception d'une architecture hautement résiliente aux pannes critiques (*Exactly-Once Processing*).

---

## Structure du Projet

```text
fraud-detection-platform/
├── .github/
│   └── workflows/
│       └── ci-cd.yml         # Pipeline d'Intégration Continue (GitHub Actions)
├── src/
│   ├── feature_store/        # Configuration et définitions Feast (MLOps)
│   │   ├── feature_store.yaml
│   │   └── features.py
│   ├── streaming/            # Jobs de traitement de flux Apache Flink
│   │   └── processor.py
│   └── visualisation/        # Dashboard de monitoring des fraudes (Streamlit)
│       └── app.py
├── tests/                    # Tests unitaires et d'intégration
│   └── test_streaming.py     # Tests de la logique Flink avec PyTest
├── data/                     # Données locales et registres (ignoré par Git)
├── requirements.txt          # Dépendances Python du projet
└── README.md                 # Documentation du projet
