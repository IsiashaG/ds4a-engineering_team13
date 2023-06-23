import json
import boto3
import csv
import io
import urllib.request
from botocore.vendored import requests

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = 'extracted-data-data-swan'
    
    file_name = '2020_general_paymentdata.csv'
 
    # URL of the CSV file
    csv_url = "https://openpaymentsdata.cms.gov/api/1/datastore/query/a08c4b30-5cf3-4948-ad40-36f404619019/0/download?&format=csv"

    with requests.get(csv_url, stream=True) as response:
        if response.status_code == 200:
            # Open a temporary file for writing
            with open('/tmp/' + file_name, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)

            # Upload the file to S3 bucket
            s3.upload_file('/tmp/' + file_name, bucket_name, file_name)

            return {
                'statusCode': 200,
                'body': 'CSV file uploaded successfully'
            }
        else:
            return {
                'statusCode': response.status_code,
                'body': 'Failed to download CSV file'
            }
