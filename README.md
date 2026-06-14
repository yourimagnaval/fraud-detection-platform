# Real-Time Fraud & Bot Detection Platform

Une plateforme industrielle et scalable de détection de comportements frauduleux (activités de bots, fermes à clics) en temps réel. Ce projet combine le traitement de flux d'événements à haute vélocité, une infrastructure MLOps moderne pour la gestion des caractéristiques (features) à basse latence, et des pratiques DevOps rigoureuses.

---

## Architecture Technique

Le système est conçu autour de trois piliers techniques majeurs :

* **Ingestion & In-Flight Processing (Temps Réel) :** Simulation d'un trafic utilisateur à haute vélocité (mélange de trafic légitime et d'attaques de bots) distribué via **Apache Kafka**. Traitement événement par événement (*Stateful Stream Processing*) via **Apache Flink** à l'aide de fenêtres glissantes (*Hopping Windows*) pour détecter les anomalies de comportement en moins d'une seconde.
* **Infrastructure MLOps (Feature Store) :** Centralisation et service des caractéristiques à l'aide de **Feast**. Le calcul à chaud issu de Flink alimente directement le Feature Store en ligne hébergé sur **Redis** pour permettre un scoring à l'échelle de la milliseconde.
* **CI/CD & Automatisation :** Automatisation complète de l'intégration (tests unitaires et validation structurelle) via **GitHub Actions** (fichiers d'init, gestion des paths relatifs). Gestion et orchestration de l'infrastructure locale via **Docker Compose**.

---

## Structure du Projet

```text
fraud-detection-platform/
├── .github/
│   └── workflows/
│       └── ci-cd.yml          # Pipeline d'Intégration Continue (GitHub Actions)
├── config/
│   └── docker-compose.yaml    # Orchestration des services (Kafka, Redis, Mongo, Feast)
├── src/
│   ├── ingestion/             # Générateur de trafic et ingestion de données
│   │   └── producer.py        # Simulateur Kafka (Trafic normal vs Attaques de Bots)
│   ├── streaming/             # Traitement de flux distribué
│   │   ├── flink_job.py       # Moteur temps réel Flink SQL (Fenêtrage 10s & Injection Redis)
│   │   ├── consumer.py        # Consumer Python alternatif avec stockage des alertes dans MongoDB
│   │   └── processor.py       # Pipeline d'extension pour l'analyse des transactions de paiement
│   ├── feature_store/         # Configuration et définitions Feast (MLOps)
│   │   ├── feature_store.yaml # Configuration des stores (Offline vs Online Redis)
│   │   ├── features.py        # Définition des entités et des Feature Views
│   │   └── read_features.py   # Client de lecture basse latence / Simulation de scoring ML
│   └── visualisation/         # Interface utilisateur et supervision
│       └── app.py             # Dashboard de monitoring des fraudes en temps réel (Streamlit)
├── data/                      # Registres Feast locaux et stockage offline (Ignoré par Git)
├── requirements.txt           # Dépendances Python du projet
└── README.md                  # Documentation de la plateforme
