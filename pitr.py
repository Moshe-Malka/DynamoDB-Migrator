import boto3
import time

dynamodb_client = boto3.client('dynamodb')

def enable_disable_pitr(table_name, val):
    try:
        return dynamodb_client.update_continuous_backups(
            TableName=table_name,
            PointInTimeRecoverySpecification={
                'PointInTimeRecoveryEnabled': val
            })
    except Exception as e:
        print(e)

def get_table_arn(table_name):
    try:
        return dynamodb_client.describe_table(TableName=table_name)['Table']['TableArn']
    except Exception as e:
        print(e)

def lambda_handler(event):
    print(f"Event: {event}")
    table_name = event.get('table_name')
    # if we have a key 'enable_pitr' - enable pitr on the table, otherwise disable it.
    response = enable_disable_pitr(table_name, event.get('enable_pitr', False))
    time.sleep(5)
    print(f"PITR Response: {response}")
    return {
        'pitr_response': response['ContinuousBackupsDescription'],
        'pitr_status': response['ContinuousBackupsDescription']['PointInTimeRecoveryDescription']['PointInTimeRecoveryStatus'],
        'table_arn': get_table_arn(table_name),
        'table_name': table_name
        }

