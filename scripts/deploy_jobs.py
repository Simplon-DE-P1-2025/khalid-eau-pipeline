import os
import json
import requests
import base64
from pathlib import Path

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
JOB_IDS_FILE = Path(__file__).parent.parent / "job_ids.json"

if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
    print("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN environment variables required")
    exit(1)

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json",
}


def load_job_ids():
    """Load existing job IDs from file."""
    if JOB_IDS_FILE.exists():
        with open(JOB_IDS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_job_ids(job_ids):
    """Save job IDs to file for future reference."""
    JOB_IDS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(JOB_IDS_FILE, "w") as f:
        json.dump(job_ids, f, indent=2)
    print(f"Job IDs saved to {JOB_IDS_FILE}")


def create_folder(folder_path):
    """Create a folder in Databricks workspace."""
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/mkdirs"
    payload = {"path": folder_path}
    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200:
        print(f"  Created folder {folder_path}")
        return True
    elif "already exists" in response.text:
        print(f"  Folder {folder_path} already exists")
        return True
    else:
        print(f"  ERROR creating folder {folder_path}: {response.text}")
        return False


def upload_notebooks():
    """Upload notebooks to Databricks workspace via REST API."""
    notebooks_dir = Path(__file__).parent.parent / "notebooks"
    target_path = "/Repos/khalid-eau-pipeline/notebooks"

    print(f"\nCreating folders in Databricks...")
    if not create_folder("/Repos"):
        return False
    if not create_folder("/Repos/khalid-eau-pipeline"):
        return False
    if not create_folder(target_path):
        return False

    print(f"\nUploading notebooks from {notebooks_dir} to {target_path}...")

    for notebook_file in notebooks_dir.glob("*.py"):
        notebook_name = notebook_file.stem
        notebook_content = notebook_file.read_bytes()

        content_base64 = base64.b64encode(notebook_content).decode("utf-8")

        url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"

        payload = {
            "path": f"{target_path}/{notebook_name}",
            "format": "SOURCE",
            "language": "PYTHON",
            "overwrite": True,
            "content": content_base64,
        }

        response = requests.post(url, json=payload, headers=headers)

        if response.status_code == 200:
            print(f"  Uploaded {notebook_name}")
        else:
            print(f"  ERROR uploading {notebook_name}: {response.text}")
            return False

    print("All notebooks uploaded successfully")
    return True


def create_or_update_job(job_name, notebook_path, timeout_seconds=3600):
    """Create or update a Databricks job."""

    job_ids = load_job_ids()

    # Check if job exists in Databricks
    response = requests.get(f"{DATABRICKS_HOST}/api/2.1/jobs/list", headers=headers)
    if response.status_code != 200:
        print(f"ERROR: Failed to list jobs - {response.text}")
        return None

    jobs = response.json().get("jobs", [])
    existing_job = next((j for j in jobs if j["settings"]["name"] == job_name), None)

    job_config = {
        "name": job_name,
        "tasks": [
            {
                "task_key": job_name.replace(" ", "_").replace("-", "_"),
                "notebook_task": {"notebook_path": notebook_path},
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 2,
                },
                "timeout_seconds": timeout_seconds,
            }
        ],
    }

    if existing_job:
        job_id = existing_job["job_id"]
        print(f"Updating job '{job_name}' (ID: {job_id})")
        response = requests.post(
            f"{DATABRICKS_HOST}/api/2.1/jobs/reset",
            json={"job_id": job_id, "new_settings": job_config},
            headers=headers,
        )
        if response.status_code == 200:
            print(f"  Updated successfully")
            job_ids[job_name] = job_id
        else:
            print(f"  ERROR: {response.text}")
    else:
        print(f"Creating new job '{job_name}'")
        response = requests.post(
            f"{DATABRICKS_HOST}/api/2.1/jobs/create",
            json=job_config,
            headers=headers,
        )
        if response.status_code == 200:
            job_id = response.json().get("job_id")
            print(f"  Created with ID: {job_id}")
            job_ids[job_name] = job_id
        else:
            print(f"  ERROR: {response.text}")
            return None

    save_job_ids(job_ids)
    return job_ids.get(job_name)


if __name__ == "__main__":
    print("Starting Databricks deployment...\n")

    # Step 1: Upload notebooks
    if not upload_notebooks():
        print("Failed to upload notebooks. Aborting.")
        exit(1)

    # Step 2: Create/update jobs
    notebooks = [
        (
            "Ingestion Bronze",
            "/Repos/khalid-eau-pipeline/notebooks/01_ingestion_bronze",
        ),
        (
            "Nettoyage Silver",
            "/Repos/khalid-eau-pipeline/notebooks/02_nettoyage_silver",
        ),
        (
            "Analyse Gold",
            "/Repos/khalid-eau-pipeline/notebooks/03_analyse_gold",
        ),
    ]

    print("\nDeploying jobs to Databricks...")
    for job_name, notebook_path in notebooks:
        create_or_update_job(job_name, notebook_path)

    print("\nDeployment complete!")
