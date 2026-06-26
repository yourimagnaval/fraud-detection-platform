# Real-Time Fraud Detection Platform

Une architecture événementielle distribuée dédiée à l'ingestion, la validation, l'enrichissement en continu et le serving à faible latence de fonctionnalités pour la détection de fraudes et d'attaques de bots en temps réel.

## Architecture Globale de la Plateforme

La plateforme s'appuie sur une infrastructure orientée Stream Processing et MLOps entièrement conteneurisée :

1. Ingestion (Streaming multi-flux) : Flux asynchrones de clics utilisateur (user-clickstream) et de transactions (payment-transactions) gérés par Apache Kafka.
2. Data Quality : Validation des schémas et conformité des types à la volée via Great Expectations.
3. Moteur de Streaming : Traitement de flux, règles de classification de fraude et jointures temporelles avec cache optimisé gérés par Apache Flink.
4. Feature Store (MLOps) : Gestion et matérialisation duale du catalogue de fonctionnalités avec Feast (PostgreSQL en Offline Store pour l'entraînement et Redis en Online Store pour le serving).
5. Dashboard et Serving : Interface analytique interactive et microservice de décision temps réel développés avec Streamlit.

---

## Stack Technique

* Langages et Moteurs : Python, SQL, Apache Flink (PyFlink 1.18)
* Courtier d'événements : Apache Kafka / Zookeeper (Écosystème Confluent)
* Bases de Données et Stockage : Redis (In-memory, Low-latency), PostgreSQL (Relationnel, Lakehouse analytique)
* Gouvernance et MLOps : Feast (Feature Store framework), Great Expectations (Data Quality)
* Interface et Restitution : Streamlit, Pandas

---

## Structure du Projet

* docker-compose.yml : Orchestration de l'infrastructure (Kafka, Redis, Postgres).
* cataloh_manager.py : Initialisation des schémas PostgreSQL et générateur automatique de données catalogue.
* producer.py : Simulateur hybride de trafic (comportements utilisateurs normaux vs bursts d'attaques de bots).
* validate_data.py : Moteur éphémère de validation de la qualité et structure de données.
* processor.py : Application maîtresse PyFlink SQL gérant les fenêtres temporelles, le cache et la détection des fraudes.
* feature_store/ : Dossier de configuration Feast (feature_store.yaml, définitions des vues de features dans features.py).
* read_features.py : Script d'automatisation des commandes CLI Feast (apply, materialize) et test de serving unitaire.
* app.py : Dashboard de monitoring temps réel et API de simulation de transactions.

---

## Fonctionnalités Clés et Implémentations Techniques

### Jointures Temporelles Optimisées (Lookup Join)
Le processeur Apache Flink enrichit le flux transactionnel Kafka avec les données du catalogue de produits stocké dans PostgreSQL. Pour garantir des performances élevées et une faible latence, le connecteur JDBC intègre un mécanisme de cache LRU adaptatif :
* lookup.cache.max-rows = 10000
* lookup.cache.ttl = 30min

### Logique de Détection Métier Intégrée
Le moteur Flink évalue à la volée les transactions selon des patterns de risques prédéfinis :
* MONTANT_EXTREME : Toute transaction unitaire supérieure à 5 000 euros.
* SCORE_RISQUE_ELEVE : Toute transaction supérieure à 1 000 euros ciblant un produit à haut risque intrinsèque (risk_score > 0.5).

### MLOps et Feature Store Dual-Store
L'intégration de Feast permet de standardiser l'utilisation des caractéristiques comportementales de l'utilisateur (click_count, is_blocked) :
* Stockage Hors-ligne (PostgreSQL) : Conserve l'historique brut des alertes pour l'entraînement futur de modèles de Machine Learning.
* Stockage En-ligne (Redis) : Matérialisation incrémentale à la volée pour offrir une latence de lecture millisecondique requise par l'interface lors de l'évaluation d'un profil.

---

## Instructions de Démarrage

### 1. Lancer l'infrastructure
```bash
docker-compose up -d


```
### 2. Initialiser la Base de Données
Génère le catalogue de produits simulé dans PostgreSQL :

```bash
python cataloh_manager.py
```


### 3. Orchestrer et Synchroniser le Feature Store

```bash
python cataloh_manager.py
```


### 4. Démarrer le Processeur d'Événements (Apache Flink)

```bash
python processor.py
```

### 5. Lancer le Producteur de Flux Kafka

```bash
python producer.py
```


### 6. Lancer le Dashboard Streamlit
```bash
streamlit run app.py
```
