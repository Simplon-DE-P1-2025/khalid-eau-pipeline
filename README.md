# Pipeline Data Engineering - Qualité de l'Eau

Pipeline de données complète pour l'ingestion, transformation et analyse des données de qualité de l'eau potable en France via l'API Hub'Eau. 

## Architecture

Ce projet implémente l'architecture Médaillon avec trois couches de traitement :

- **Bronze** : Ingestion brute des données via l'API Hub'Eau, stockage Delta sans transformation
- **Silver** : Nettoyage, typage des données, gestion des doublons et valeurs manquantes
- **Gold** : Tables analytiques avec KPIs de conformité par commune et indicateurs de qualité

## Structure du Projet

```
notebooks/
├── 01_ingestion_bronze.py      # Extraction API Hub'Eau, parallélisation avec ThreadPool
├── 02_nettoyage_silver.py      # Nettoyage, typage, merge incrémental
└── 03_analyse_gold.py          # Agrégation KPIs, taux de conformité

scripts/
└── deploy_jobs.py              # Déploiement automatique des jobs Databricks

.github/workflows/
└── databricks_ci.yml           # Pipeline CI/CD avec linting et déploiement
```

## Installation et Configuration

### Prérequis

- Databricks Workspace (Community Edition ou Enterprise)
- Python 3.11+
- GitHub avec secrets configurés

## Configuration

Pour déployer automatiquement sur Databricks via GitHub Actions :

1. Créer un Personal Access Token Databricks (Settings > User Settings > Developer)
2. Configurer les secrets GitHub (Settings > Secrets and variables > Actions) :
   - `DATABRICKS_HOST` : URL du workspace
   - `DATABRICKS_TOKEN` : Token d'authentification
   - `DATABRICKS_WORKSPACE_ID` : ID du workspace

3. À chaque push sur `main`, le CI/CD déploie automatiquement

Voir [docs/DATABRICKS_SETUP.md](docs/DATABRICKS_SETUP.md) pour les détails complets.

## Pipeline CI/CD

Le workflow GitHub Actions automatise :

1. Linting du code Python (flake8)
2. Déploiement des notebooks dans le workspace Databricks
3. Création et mise à jour des jobs orchestrés

Déclenché automatiquement sur chaque push vers `main`.

## Modèle de Données

### Bronze (Données Brutes)

Colonnes principales de l'API Hub'Eau :
- `code_prelevement` : Identifiant unique du prélèvement
- `code_parametre` : Type de paramètre mesuré
- `date_prelevement` : Timestamp du prélèvement
- `resultat_numerique` : Valeur mesurée
- `nom_commune` : Localisation géographique

### Silver (Données Nettoyées)

Transformations appliquées :
- Suppression des doublons (clé composite : code_prelevement + code_parametre)
- Typage des colonnes dates en timestamp
- Normalisation des codes commune et département
- Filtrage des lignes sans identifiant de prélèvement

### Gold (Données Analytiques)

KPIs par commune :
- `total_prelevements` : Nombre de prélèvements uniques
- `conformes_bacterio` : Comptage des conformités bactériologiques
- `conformes_chimique` : Comptage des conformités physico-chimiques
- `taux_conformite_bacterio_pct` : Taux de conformité bactériologique
- `taux_conformite_chimique_pct` : Taux de conformité chimique

## Optimisations

- Parallélisation des appels API (ThreadPoolExecutor, 5 workers par défaut)
- Pagination des requêtes pour gérer de gros volumes
- Merge incrémental (UPSERT) pour les mises à jour sans suppression
- Compaction Delta sur Azure Databricks (OPTIMIZE)

## Ressources API

API Hub'Eau (Portail d'information sur l'eau) :
- Endpoint : `https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis`
- Format : JSON avec pagination et filtrage géographique
- Gestion des erreurs : codes 200, 206 acceptés

## Exécution

Les trois notebooks peuvent être exécutés indépendamment ou orchestrés via un Databricks Job qui appelle les trois séquentiellement. Le CI/CD les déploie automatiquement.
