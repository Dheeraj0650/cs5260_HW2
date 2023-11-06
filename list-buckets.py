import boto3
import json
import argparse
import unittest
import unit_test

from unittest.mock import Mock, patch
from io import BytesIO

unit_test = unit_test.UnitTestCode()

parser = argparse.ArgumentParser()

parser.add_argument('--storage', choices=['S3', 'Dynamodb', 'SQS'])
parser.add_argument('--resource', type=str)

args = parser.parse_args()

storage = args.storage
resource = args.resource


session = boto3.Session()
s3_client = session.client('s3')
sqs = session.client('sqs', region_name='us-east-1')
dynamodb_client = session.client('dynamodb')

bucket_name_2 = 'usu-cs5250-global-requests'
bucket_name_3 = 'usu-cs5250-global-web'
table_name = resource

print(storage)
print(resource)

objects = s3_client.list_objects_v2(Bucket=bucket_name_2)

if 'Contents' in objects and storage != 'SQS':
    for obj in objects['Contents']:
        response = s3_client.get_object(Bucket=bucket_name_2, Key=obj['Key'])
        response = response['Body'].read()
        try:
            
            print(obj['Key'])
            object_content = json.loads(response)
            print(storage)
            print(object_content['type'])
            if(object_content['type'] == "create"):
                if(storage == 'S3'):
                    
                    key  = f'widgets/{object_content["owner"].lower().replace(" ", "-")}/{object_content["widgetId"]}'
                    response = s3_client.put_object(Bucket=bucket_name_3, Key=key, Body=json.dumps(object_content))
                    unit_test.unit_test_s3(object_content, obj['Key'], bucket_name_2, bucket_name_3, s3_client)
                    s3_client.delete_object(Bucket=bucket_name_2, Key=obj['Key'])
                    
                elif(storage == 'Dynamodb'):
                    
                    data = {
                        'widget_id': {'S': object_content['widgetId']},
                        'id': {'S': object_content['requestId']},
                        'owner': {'S': object_content['owner']},
                        'label': {'S': object_content['label']},
                        'description': {'S': object_content['description']},
                    }
                    
                    for key in object_content['otherAttributes']:
                        data[key['name']]=  {'S': key['value']}
                    
                    response = dynamodb_client.put_item(TableName=table_name, Item=data)
                    unit_test.unit_test_dynamodb(data, obj['Key'], table_name, bucket_name_3, dynamodb_client, s3_client)
                    s3_client.delete_object(Bucket=bucket_name_2, Key= obj['Key'])
                
        except Exception as e:
            print(f'Error: {e}')
            
else:
    response = sqs.receive_message(QueueUrl=resource, AttributeNames=['All'], MaxNumberOfMessages=1, MessageAttributeNames=['All'], VisibilityTimeout=0, WaitTimeSeconds=0)

    if 'Messages' in response:
        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']

        print(f"received message: {message['Body']}")
        key = json.loads(message['Body'])['Records'][0]['s3']['object']['key']
        
        response = s3_client.get_object(Bucket=bucket_name_2, Key=key)
        response = response['Body'].read()
        object_content = json.loads(response)
        
        print("processed message")
        response = s3_client.put_object(Bucket=bucket_name_3, Key=f'widgets/{key}', Body=json.dumps(object_content))
        unit_test.unit_test_s3(object_content, key, bucket_name_2, bucket_name_3, s3_client)

        sqs.delete_message(
            QueueUrl=resource,
            ReceiptHandle=receipt_handle
        )
        
        print(f"deleted message: {message['Body']}")
        
    else:
        print("no messages in the queue")
            