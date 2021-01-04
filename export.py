import boto3
import os
import time

dynamodb_client = boto3.client('dynamodb')

def export_to_s3(table_arn, bucket_name, bucket_owner, prefix):
    try:
        return dynamodb_client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket_name,
            S3BucketOwner=bucket_owner,
            S3Prefix=prefix,
            ExportFormat="DYNAMODB_JSON")
    except Exception as e:
        print(e)

def lambda_handler(event):
    print(f"Event: {event}")
    if event['pitr_status'] == 'ENABLED':
        table_arn = event['table_arn']
        table_name = event['table_name']
        bucket_name = os.environ['BUCKET_NAME']
        bucket_owner = os.environ['BUCKET_OWNER']
        prefix = f"ddb_migrations/{table_name}/"
        response = export_to_s3(table_arn, bucket_name, bucket_owner, prefix)
    retrun { 'table_name': table_name, 'export_operation': response }