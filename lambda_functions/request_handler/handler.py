# request_handler
import os
import boto3
import json

sqs = boto3.client('sqs')

def handler(event, _):
    # Assuming the file URL is in event['body']
    body = json.loads(event['body'])
    file_url = body.get('fileUrl')
    
    if not file_url:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Missing file URL'})
        }

    queue_url = os.environ['QUEUE_URL']
    sqs.send_message(QueueUrl=queue_url, MessageBody=file_url)

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'File upload request received'})
    }