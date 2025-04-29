#!/bin/bash

Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration Variables (EDIT THESE) ---
export GCP_PROJECT="upheld-booking-456522-s3"                         # Your GCP Project ID
export GCP_REGION="asia-south1"                       # e.g., us-central1, europe-west2
export CLUSTER_NAME="ev-iceberg-cluster"         # Unique cluster name with timestamp
export STAGING_BUCKET="ev-code"
export CURATED_BUCKET="ev-data-curated"     # Bucket for Iceberg warehouse
export CUSTOM_SERVICE_ACCOUNT="gcp-ev-sa@upheld-booking-456522-s3.iam.gserviceaccount.com"                             # Optional: your-dataproc-sa@${GCP_PROJECT}.iam.gserviceaccount.com (Leave empty to use default Compute Engine SA)
export IMAGE_VERSION="2.2-ubuntu22"

echo "--- Configuration ---"
echo "Project:            ${GCP_PROJECT}"
echo "Region:             ${GCP_REGION}"
echo "Cluster Name:       ${CLUSTER_NAME}"
echo "Code Bucket:        ${CODE_BUCKET}"
echo "Curated Bucket:     ${CURATED_BUCKET}"
echo "Image Version:      ${IMAGE_VERSION}"
echo "Staging Bucket:     ${STAGING_BUCKET}"
[ -n "${CUSTOM_SERVICE_ACCOUNT}" ] && echo "Service Account:    ${CUSTOM_SERVICE_ACCOUNT}" || echo "Service Account:    Default Compute Engine SA"

SERVICE_ACCOUNT_FLAG=""
if [ -n "${CUSTOM_SERVICE_ACCOUNT}" ]; then
    SERVICE_ACCOUNT_FLAG="--service-account=${CUSTOM_SERVICE_ACCOUNT}"
fi
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --project=${GCP_PROJECT} \
    --region=${GCP_REGION} \
    --image-version=${IMAGE_VERSION} \
    --master-machine-type=n1-standard-4 \
    --master-boot-disk-size=100 \
    --num-workers=2 \
    --worker-machine-type=n1-standard-4 \
    --worker-boot-disk-size=100 \
    --bucket=${STAGING_BUCKET} \
    --optional-components=ICEBERG \
    ${SERVICE_ACCOUNT_FLAG} \
    --scopes=https://www.googleapis.com/auth/cloud-platform
echo "Cluster creation initiated."
echo
