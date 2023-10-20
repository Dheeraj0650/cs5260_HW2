import boto3
import json


session = boto3.Session()
s3_client = session.client('s3')
dynamodb_client = session.client('dynamodb')

bucket_name_2 = 'usu-cs5250-global-requests'
bucket_name_3 = 'usu-cs5250-global-web'
table_name = 'widgets'


objects = s3_client.list_objects_v2(Bucket=bucket_name_2)

if 'Contents' in objects:
    for obj in objects['Contents']:
        response = s3_client.get_object(Bucket=bucket_name_2, Key=obj['Key'])
        response = response['Body'].read()
        try:
            print(obj['Key'])
            object_content = json.loads(response)
            if(object_content['type'] == "create"):
                # key  = f'widgets/{object_content["owner"].lower().replace(" ", "-")}/{object_content["widgetId"]}'
                # response = s3_client.put_object(Bucket=bucket_name_3, Key=key, Body=json.dumps(object_content))
                
                data_to_store = {
                    'widget_id': {'S': object_content['widgetId']},
                    'id': {'S': object_content['requestId']},
                    'owner': {'S': object_content['owner']},
                    'label': {'S': object_content['label']},
                    'description': {'S': object_content['description']},
                }
                

                for key in object_content['otherAttributes']:
                    data_to_store[key['name']]=  {'S': key['value']}
                
                print(data_to_store)
                
                response = dynamodb_client.put_item(TableName=table_name, Item=data_to_store)

                
        except Exception as e:
            print(f'Error: {e}')
            


