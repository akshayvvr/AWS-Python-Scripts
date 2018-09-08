import boto3
import json

s3_client=boto3.client('s3')
dynamodb=boto3.resource('dynamodb')

def csv_reader(event, context):
    bucket=event['Records'][0]['s3']['bucket']['name']
    
    key=event['Records'][0]['s3']['object']['key']
    print((str(event)))
    obj=s3_client.get_object(Bucket=bucket, Key=key)
    
    rows=obj['Body'].read().split('\n')
    
    table=dynamodb.Table('Messages')
    
    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item={
            
            'MessageID':row.split(',')[0],
            'Message':row.split(',')[1]
            })
        
    
    return 'Hello From Lambda'
    
