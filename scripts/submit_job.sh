#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
# set -e

# --- Argument Check ---
if [ -z "$1" ]; then
  echo "Usage: $0 <cluster-name>"
  echo "Error: Please provide the name of the target Dataproc cluster as the first argument."
  exit 1
fi
TARGET_CLUSTER_NAME=$1

# --- Configuration Variables (EDIT THESE) ---
export GCP_PROJECT="upheld-booking-456522-s3"                 # Your GCP Project ID
export GCP_REGION="asia-south1"               # e.g., us-central1, asia-south1
export CODE_BUCKET="ev-code"                # Bucket where code is uploaded

# --- List of auxiliary Python files (EDIT THIS LIST) ---
# List all .py files UPLOADED to CODE_BUCKET that main.py depends on.
# DO NOT include main.py itself here.
AUX_PY_FILES=(
    "data_loader.py"
    "parser.py"
    "transformer.py"
    "config.py"
    "iceberg_writer.py"  # Assuming you will have this - adjust as needed
    # Add any other .py dependency files here
)

# --- GCS Path Variables (Derived) ---
MAIN_PYTHON_FILE_GCS="gs://${CODE_BUCKET}/main.py"
PY_FILES_GCS_LIST="" # Will store the comma-separated list for --py-files

echo "--- Job Submission Configuration ---"
echo "Project:         ${GCP_PROJECT}"
echo "Region:          ${GCP_REGION}"
echo "Code Bucket:     ${CODE_BUCKET}"
echo "Target Cluster:  ${TARGET_CLUSTER_NAME}"
echo "Main Script:     ${MAIN_PYTHON_FILE_GCS}"
echo "Auxiliary Files: ${AUX_PY_FILES[*]}"
echo "------------------------------------"
echo

# --- Build the --py-files argument list ---
# This assumes the auxiliary files exist directly inside the CODE_BUCKET
echo "Building GCS path list for auxiliary files..."
for py_file in "${AUX_PY_FILES[@]}"; do
    # Construct the GCS path for the auxiliary file
    gcs_path="gs://${CODE_BUCKET}/${py_file}"
    # Add the GCS path to comma-separated list
    if [ -z "$PY_FILES_GCS_LIST" ]; then
        PY_FILES_GCS_LIST="${gcs_path}"
    else
        PY_FILES_GCS_LIST="${PY_FILES_GCS_LIST},${gcs_path}"
    fi
done

# Construct the actual flag to pass to gcloud, only if the list is not empty
PY_FILES_FLAG=""
if [ -n "$PY_FILES_GCS_LIST" ]; then
    PY_FILES_FLAG="--py-files=${PY_FILES_GCS_LIST}"
    echo "Using --py-files flag: ${PY_FILES_FLAG}"
else
    echo "No auxiliary Python files specified for --py-files flag."
fi
echo

# --- Submit Job ---
echo "Submitting PySpark job to cluster '${TARGET_CLUSTER_NAME}'..."

gcloud dataproc jobs submit pyspark ${MAIN_PYTHON_FILE_GCS} \
    --project=${GCP_PROJECT} \
    --region=${GCP_REGION} \
    --cluster=${TARGET_CLUSTER_NAME} \
    ${PY_FILES_FLAG} \
    # --jars=gs://path/to/extra.jar \ # Optional: Add if needed and not handled by image/packages
    # --properties=spark.driver.memory=4g \ # Optional: Add Spark properties

echo
echo "Job submitted successfully to cluster '${TARGET_CLUSTER_NAME}'."
echo "Monitor progress in the GCP Console (Dataproc > Jobs)."

echo
read -p "Press Enter to exit..."
echo
echo "*** Script finished. ***"