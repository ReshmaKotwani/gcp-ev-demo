#!/bin/bash
# Uploads Python code files to the specified GCS bucket.
# Assumes a flat file structure in the project root.

set -e # Exit on error

# --- Configuration Variables (EDIT THESE) ---
export CODE_BUCKET="ev-code" # Bucket where code should be uploaded

# List all .py files in the root directory needed for the job
PYTHON_FILES=(
    "main.py"
    "data_loader.py"
    "parser.py"
    "transformer.py"
    "config.py"
    "iceberg_writer.py"
    # Add any other required .py files
)
# --- End Configuration ---

echo "--- Uploading Code ---"
echo "Target Bucket: gs://${CODE_BUCKET}/"

# Upload files
for py_file in "${PYTHON_FILES[@]}"; do
    if [ -f "../$py_file" ]; then # Assuming script is run from scripts/ dir
        echo "Uploading $py_file..."
        gsutil cp "../$py_file" "gs://${CODE_BUCKET}/$py_file"
    elif [ -f "$py_file" ]; then # Assuming script is run from project root
         echo "Uploading $py_file..."
         gsutil cp "$py_file" "gs://${CODE_BUCKET}/$py_file"
    else
        echo "Warning: File '$py_file' not found. Skipping upload."
        # Decide if this is critical: exit 1
    fi
done

echo
read -p "Press Enter to exit..."

echo "---------------------"
echo "Code upload complete."