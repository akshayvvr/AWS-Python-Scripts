import boto3
import json

#Declare S3 bucket and DynamoDB Boto3 Clients
s3_client=boto3.client('s3')
dynamodb=boto3.resource('dynamodb')

#Event is the uploading of a CSV File to a bucket
def csv_reader(event, context):
    
    #Fetch the bucket name and the file 
    bucket=event['Records'][0]['s3']['bucket']['name']
    key=event['Records'][0]['s3']['object']['key']
    print((str(event)))
    
    # Read the Object using get_object API
    obj=s3_client.get_object(Bucket=bucket, Key=key)
    rows=obj['Body'].read().split('\n')
    
    table=dynamodb.Table('Messages')
    
    #Write the CSV file to the DynamoDB Table
    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item={
            
            'MessageID':row.split(',')[0],
            'Message':row.split(',')[1]
            })
        
    
    return 'Hello From Lambda'
    
