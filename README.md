# Projet Data Engineering - Pipeline Qualité de l'Eau (Hub'Eau)

Ce projet implémente une pipeline de données moderne avec **Databricks**, utilisant l'**Architecture Medaillon** (Bronze, Silver, Gold) pour ingérer, transformer et exploiter les données de qualité de l'eau potable en France (via l'API Hub'Eau).

## 🗂️ Structure du Projet

```
.
├── notebooks/                  # Dossier contenant les scripts Databricks (PySpark)
│   ├── 01_ingestion_bronze.py  # Ingestion des données API vers la couche Bronze
│   ├── 02_nettoyage_silver.py  # Nettoyage et standardisation (Bronze -> Silver) (À venir)
│   └── 03_analyse_gold.py      # Modélisation et agrégats métier (Silver -> Gold) (À venir)
├── data/                       # (Optionnel) pour des données de test locales
├── requirements.txt            # Dépendances Python si exécution locale
└── README.md
```

## 🚀 Comment commencer ?

1. **Databricks :** Liez votre Workspace Databricks à ce dépôt GitHub (via "Repos" ou "Git folders" dans Databricks).
2. **Cluster :** Assurez-vous d'avoir un cluster avec *Databricks Runtime 13.3 LTS (ou supérieur)*.
3. **Exécution :** Ouvrez le fichier `notebooks/01_ingestion_bronze.py` depuis Databricks (il s'ouvrira comme un Notebook) et lancez l'exécution pour importer les premières données.

## 🏗️ Architecture Medallion

* **Couche Bronze :** Données brutes issues de l'API (format JSON converti en Delta). Historisation sans modification.
* **Couche Silver :** Données nettoyées, typées, avec gestion des valeurs manquantes, doublons et normalisation des unités.
* **Couche Gold :** Tables prêtes pour l'analyse et la BI (indicateurs de conformité, agrégats géographiques et temporels).

## 📡 API Hub'Eau

Nous utilisons le point d'accès pour les résultats du contrôle sanitaire de l'eau distribuée :
`https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis`
