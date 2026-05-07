# Configuration de Databricks pour le Déploiement CI/CD

## Étapes de Configuration

### 1. Créer un Personal Access Token Databricks

1. Aller dans Databricks Workspace
2. Settings > User Settings > Developer
3. Cliquer sur "Generate new token"
4. Copier le token généré (format : `dapi...`)

### 2. Récupérer les informations du Workspace

- **DATABRICKS_HOST** : URL de base du workspace (ex: https://adb-123456789.azuredatabricks.net)
- **DATABRICKS_WORKSPACE_ID** : Visible dans l'URL du workspace

### 3. Configurer les Secrets GitHub (Automatisation)

Aller à Settings > Secrets and variables > Actions > New repository secret

Ajouter les trois secrets :

```
DATABRICKS_HOST=https://adb-xxxxx.azuredatabricks.net
DATABRICKS_TOKEN=dapi1234567890abcdef
DATABRICKS_WORKSPACE_ID=123456789
```

À chaque push sur `main`, le workflow GitHub Actions utilisera ces secrets pour :
- Déployer automatiquement les notebooks dans le workspace
- Créer/mettre à jour les jobs Databricks
- Sauvegarder les IDs de job dans `job_ids.json`

### 4. Configuration Locale (Optionnel)

Pour tester le déploiement localement :

1. Copier `.env.example` en `.env`
2. Remplir les valeurs :
   ```
   DATABRICKS_HOST=https://adb-xxxxx.azuredatabricks.net
   DATABRICKS_TOKEN=dapi1234567890abcdef
   DATABRICKS_WORKSPACE_ID=123456789
   ```

3. Exécuter le script :
   ```bash
   pip install requests
   python scripts/deploy_jobs.py
   ```

Le script créera automatiquement les jobs et sauvegardera leurs IDs dans `job_ids.json`.

### 5. Configurer Git Integration sur Databricks

1. Admin Console > Git Integration
2. Configurer GitHub (OAuth)
3. Créer un dossier "Repos" et lier ce repository
4. Les notebooks seront synchronisés automatiquement

### 6. Créer un Cluster

1. Compute > Create Cluster
2. Runtime : Databricks Runtime 13.3 LTS ou supérieur
3. Configuration recommandée :
   - Node type : i3.xlarge (ou equivalent)
   - Workers : 2-4

## Déploiement Automatique

Après configuration des secrets GitHub :

1. **Push sur main** :
   ```bash
   git add .
   git commit -m "Deploy pipeline"
   git push origin main
   ```

2. GitHub Actions déclenche automatiquement :
   - Linting du code
   - Upload des notebooks dans `/Repos/khalid-eau-pipeline/notebooks`
   - Création/mise à jour des 3 jobs Databricks
   - Sauvegarde des IDs de job dans `job_ids.json`

3. Les IDs de job sont stockés localement pour les exécutions futures

## Récupération des IDs de Job

Les IDs de job sont sauvegardés automatiquement dans `job_ids.json` :

```json
{
  "Ingestion Bronze": 12345678,
  "Nettoyage Silver": 12345679,
  "Analyse Gold": 12345680
}
```

Ces IDs permettent au script de mettre à jour les jobs existants au lieu d'en créer des nouveaux.

## Vérification

Après déploiement :
1. Vérifier que les notebooks sont visibles sous Workspace > Repos
2. Vérifier que les 3 jobs sont créés sous Workflows > Jobs
3. Tester manuellement une exécution
4. Consulter `job_ids.json` pour les IDs de job
