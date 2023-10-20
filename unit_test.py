import unittest
from unittest.mock import patch, Mock
import io
import boto3
import json


class UnitTestCode(unittest.TestCase):

    def unit_test_s3(self, expected_result, key, resource, dist_bucket, s3_client):
        print(resource, key)
        response = s3_client.get_object(Bucket=resource, Key=key)
        response = response['Body'].read()
        try:
            result = json.loads(response)
            self.assertEqual(result, expected_result)
            
            test_report = {
                "type": "create",
                "owner": expected_result['owner'],
                "widgetId": expected_result['widgetId'],
                "status": "successful"
            }
            s3_client.put_object(Bucket=dist_bucket, Key=f"test_report_s3/{key}", Body=json.dumps(test_report))
        except Exception as e:
            print(f'Error: {e}')
            test_report = {
                "type": "create",
                "owner": expected_result['owner'],
                "widgetId": expected_result['widgetId'],
                "status": "failed"
            }
            s3_client.put_object(Bucket=dist_bucket, Key=f"test_report_s3/{key}", Body=json.dumps(test_report))
        
    def unit_test_dynamodb(self, expected_result, key, resource, dist_bucket, dynamodb, s3_client):
        key_condition = '#id = :id' 
        values = {':id': {'S': f"{expected_result['id']['S']}"}}
        names = {'#id': 'id'}
        
        response = dynamodb.query(
            TableName=resource,
            KeyConditionExpression=key_condition,
            ExpressionAttributeValues=values,
            ExpressionAttributeNames=names
        )
        
        result = response['Items']
        
        test_report = {
                "type": "create",
                "status": "successful"
            }
        
        try:
            self.assertEqual(result[0], expected_result)
            test_report = {
                "type": "create",
                "status": "successful"
            }
            s3_client.put_object(Bucket=dist_bucket, Key=f"test_report_dynamodb/{key}", Body=json.dumps(test_report))
        except Exception as e:
            print(f'Error: {e}')
            test_report = {
                "type": "create",
                "status": "failed"
            }
            s3_client.put_object(Bucket=dist_bucket, Key=f"test_report_dynamodb/{key}", Body=json.dumps(test_report))
            
            

