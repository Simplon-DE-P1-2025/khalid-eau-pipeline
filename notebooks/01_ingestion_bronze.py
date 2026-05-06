# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion Bronze - Qualité de l'eau potable (Hub'Eau)
# MAGIC Ce notebook extrait les données depuis l'API Hub'Eau et les stocke dans la couche Bronze au format Delta.

# COMMAND ----------

import os
import sys
import requests
import pandas as pd
from pyspark.sql.types import *

# Fix pour l'environnement local Windows/VS Code
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Définition des fonctions

# COMMAND ----------


def fetch_hubeau_data(size=100, code_commune=None):
    """Récupère les données depuis l'API Hub'Eau."""
    API_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis"
    params = {"size": size}
    if code_commune:
        params["code_commune"] = code_commune

    print(f"Appel de l'API : {API_URL}")
    response = requests.get(API_URL, params=params)

    # CORRECTION : Prise en compte du code 206 (Partial Content)
    if response.status_code in [200, 206]:
        records = response.json().get("data", [])
        print(f"{len(records)} enregistrements récupérés.")
        return records
    else:
        raise Exception(f"Erreur API: {response.status_code} - {response.text}")


# COMMAND ----------


def process_and_save_data(spark_session, records, is_local=False):
    """Convertit en Spark et sauvegarde."""
    if not records:
        print("Aucune donnée à traiter.")
        return

    # Convertit tout en String pour la couche Bronze (évite les erreurs de schéma Spark)
    pdf = pd.DataFrame(records).astype(str)

    df_bronze = spark_session.createDataFrame(pdf)

    # Affichage adapté selon l'environnement
    if is_local:
        df_bronze.show(5)
        bronze_path = "./tmp/data/bronze/hubeau_qualite_eau"  # Chemin local
    else:
        display(df_bronze)  # Fonction magique Databricks
        bronze_path = "/tmp/data/bronze/hubeau_qualite_eau"  # Chemin Databricks

    # Sauvegarde en mode Delta
    df_bronze.write.format("delta").mode("append").option("mergeSchema", "true").save(
        bronze_path
    )
    print(f"Données sauvegardées avec succès dans {bronze_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exécution

# COMMAND ----------

# On vérifie si on est en local ou sur Databricks.
try:
    spark_session = spark
    is_local_env = False
except NameError:
    # Environnement local (VS Code)
    print("Environnement local détecté. Démarrage de SparkSession local...")
    from pyspark.sql import SparkSession

    spark_session = (
        SparkSession.builder.appName("TestLocalHubEau")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    is_local_env = True

# Lancement du processus
donnees_brutes = fetch_hubeau_data(size=100)
process_and_save_data(spark_session, donnees_brutes, is_local_env)
