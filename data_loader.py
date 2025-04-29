from google.cloud import storage
import json

def get_json_data_from_gcs(bucket_name, file_path):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)

    json_str = blob.download_as_text()
    full_json = json.loads(json_str)
    
    data_list = full_json.get('data')
    meta = full_json.get('meta', {})
    view = meta.get('view', {})
    meta_columns = view.get('columns')

    return data_list, meta_columns 