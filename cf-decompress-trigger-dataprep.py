import os
import requests
import json
import io
from google.cloud import storage
from zipfile import ZipFile
from zipfile import is_zipfile

def zipextract(bucketname, zippath, zipfilename):

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucketname)

    print('Extract: {}'.format(zippath+"/"+zipfilename))

    destination_blob_pathname = zippath+"/"+zipfilename

    blob = bucket.blob(destination_blob_pathname)
    zipbytes = io.BytesIO(blob.download_as_string())

    contentfilename=""
    if is_zipfile(zipbytes):
        with ZipFile(zipbytes, 'r') as myzip:
            for contentfilename in myzip.namelist():
                contentfile = myzip.read(contentfilename)
                blob = bucket.blob(zippath + "/dataprepfiles/" + contentfilename)
                blob.upload_from_string(contentfile)

    return(contentfilename)

def dataprep_job_gcs_trigger(event, context):

    """Background Cloud Function to be triggered by Cloud Storage.
    Args:
        event (dict): The Cloud Functions event payload.
        context (google.cloud.functions.Context): Metadata of triggering event."""

    head_tail = os.path.split(event['name'])
    newfilename = head_tail[1]
    newfilepath = head_tail[0]

    datataprep_auth_token = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbklkIjoiNDZiOWY2YWUtYTg0Zi00ZWQyLTgxNTMtZDA0MjBjNzIyZTk2IiwiaWF0IjoxNjA2MjI1NTU4LCJhdWQiOiJ0cmlmYWN0YSIsImlzcyI6ImRhdGFwcmVwLWFwaS1hY2Nlc3MtdG9rZW5AdHJpZmFjdGEtZ2Nsb3VkLXByb2QuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLCJzdWIiOiJkYXRhcHJlcC1hcGktYWNjZXNzLXRva2VuQHRyaWZhY3RhLWdjbG91ZC1wcm9kLmlhbS5nc2VydmljZWFjY291bnQuY29tIn0.QKr_IfTOw_7SfK2sjMBAy_GDwdGNRuIP3f5PgUyqfj3rPdpr_4CAqNVDh11_q-NfpxoCkQEscgWs_jBrqXaOR0n4gVXzv0Yc8JAw2Qy5C0ZopbKHYvv0Pvdnd1ELjQTmzPCTzu1DdWEPLlby6feJdzeBDEA2nQvG9LkR9394V7nJK6Ly5XwdcAqWtcyPDaOZn06Ul1wRNcTgRuESVvcG-BfkKsRFCEVMZUJYr9Wc7ngVWCq80pEfksQYa2BQ-T4T6W89x2VfwHadGmXyruqWuvEv59WRxmaZ0CUjtmiocXOzzuJHTSyp9kPN9HlJ5a7wcR7Lz_53-lLKsUpHU0re0g'
    dataprep_jobid = 2011662

    if context.event_type == 'google.storage.object.finalize' and newfilepath == 'DESIGN_PATTER_SOLUTION_P1':

        file_uncompressed = zipextract("dataprep-staging-67278b43-7b2d-4e79-8d2f-5d8c470e77eb", newfilepath, newfilename)
        
        print('Run Dataprep job on new file: {}'.format(file_uncompressed))

        dataprep_runjob_endpoint = 'https://api.clouddataprep.com/v4/jobGroups'
        datataprep_job_param = {
            "wrangledDataset": {"id": dataprep_jobid},
            "runParameters": {"overrides": {"data": [{"key": "filename","value": file_uncompressed}]}}
        }
        print('Run Dataprep job param: {}'.format(datataprep_job_param))
        dataprep_headers = {
            "Content-Type":"application/json",
            "Authorization": "Bearer "+datataprep_auth_token
        }        

        resp = requests.post(
            url=dataprep_runjob_endpoint,
            headers=dataprep_headers,
            data=json.dumps(datataprep_job_param)
        )

        print('Status Code : {}'.format(resp.status_code))      
        print('Result : {}'.format(resp.json()))

    return 'End File event'.format(newfilename)
