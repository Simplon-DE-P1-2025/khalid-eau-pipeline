# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion Bronze - Qualité de l'eau potable (Hub'Eau)
# MAGIC Ce notebook extrait les données depuis l'API Hub'Eau et les stocke dans la couche Bronze au format Delta.

# COMMAND ----------

import requests
import json
from pyspark.sql.types import *
import pandas as pd

# COMMAND ----------

# Configuration de l'API Hub'Eau
# On limite ici à quelques résultats pour le test (ex: code_commune=75056 pour Paris, ou un autre code)
API_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis"

params = {
    "size": 1000 # Nombre de résultats par page (max 20000)
    # Vous pouvez ajouter d'autres filtres géographiques ou temporels ici:
    # "code_commune": "75056",
    # "date_min_prelevement": "2023-01-01"
}

print(f"Appel de l'API : {API_URL}")
response = requests.get(API_URL, params=params)

if response.status_code == 200:
    data = response.json()
    records = data.get("data", [])
    print(f"{len(records)} enregistrements récupérés.")
else:
    raise Exception(f"Erreur API: {response.status_code} - {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conversion en DataFrame Spark et Sauvegarde

# COMMAND ----------

if records:
    # Création d'un DataFrame Pandas puis conversion en Spark DataFrame (plus simple pour infer_schema sur du JSON complexe)
    pdf = pd.DataFrame(records)
    
    # Conversion du type dict/list en string pour éviter les problèmes de schéma Spark si présents
    for col in pdf.columns:
        if pdf[col].apply(lambda x: isinstance(x, (dict, list))).any():
            pdf[col] = pdf[col].astype(str)
            
    df_bronze = spark.createDataFrame(pdf)
    
    display(df_bronze)
else:
    print("Aucune donnée à traiter.")

# COMMAND ----------

# Définition du chemin de stockage pour la couche Bronze
# À adapter selon votre configuration de stockage cloud (S3, ADLS) ou DBFS local
bronze_path = "/tmp/data/bronze/hubeau_qualite_eau"

if records:
    # Sauvegarde au format Delta
    df_bronze.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(bronze_path)
        
    print(f"Données sauvegardées avec succès dans {bronze_path}")
