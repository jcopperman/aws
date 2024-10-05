# status_handler
import boto3
import os
import json

s3 = boto3.client('s3')

def handler(event, context):
    file_id = event['pathParameters']['fileId']
    
    # Check if the file exists in the output bucket
    output_bucket_name = os.environ['OUTPUT_BUCKET_NAME']

    try:
        s3.head_object(Bucket=output_bucket_name, Key=file_id)
        return {
            'statusCode': 200,
            'body': json.dumps({'status': 'ready', 'fileId': file_id})
        }
    except Exception as e:
        if e.response['Error']['Code'] == "404":
            return {
                'statusCode': 200,
                'body': json.dumps({'status': 'not_ready', 'fileId': file_id})
            }

    return {
        'statusCode': 500,
        'body': json.dumps({'message': 'Error checking status'})
    }
