import boto3
import os
from faker import Faker
import pandas as pd
import json
import io
import logging
from jsonschema import validate, ValidationError

s3 = boto3.client('s3')
fake = Faker()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Define a basic schema for JSON validation
json_schema = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "email": {"type": "string"},
        "age": {"type": "integer"}
    },
    "required": ["name", "email", "age"]
}

def anonymize_json(data):
    for key, value in data.items():
        if isinstance(value, dict):
            anonymize_json(value)
        elif isinstance(value, list):
            for item in value:
                anonymize_json(item)
        elif isinstance(value, str) and any(char.isalpha() for char in value):
            if len(value.split()) == 1:
                data[key] = fake.first_name()
            else:
                parts = value.split()
                parts[0] = fake.first_name()
                parts[-1] = fake.last_name()
                data[key] = ' '.join(parts)
    return data

def anonymize_csv(data):
    for column in data.columns:
        if any(char.isalpha() for char in data[column].iloc[0]):
            if len(data[column].iloc[0].split()) == 1:
                data[column] = fake.first_name()
            else:
                parts = data[column].iloc[0].split()
                parts[0] = fake.first_name()
                parts[-1] = fake.last_name()
                data[column] = ' '.join(parts)
    return data

def validate_json(data):
    try:
        validate(instance=data, schema=json_schema)
    except ValidationError as e:
        raise ValueError(f'Invalid JSON: {e}')

def handler(event, context):
    file_url = event['Records'][0]['body']
    
    output_bucket_name = os.environ['OUTPUT_BUCKET_NAME']

    try:
        response = s3.get_object(Bucket=file_url.split('/')[0], Key='/'.join(file_url.split('/')[1:]))
        content_type = response['ContentType']
        
        if 'json' in content_type:
            data = json.load(response['Body'])
            validate_json(data)
            anonymized_data = anonymize_json(data)
        elif 'csv' in content_type:
            csv_content = response['Body'].read()
            df = pd.read_csv(io.StringIO(csv_content.decode('utf-8')))
            anonymized_df = anonymize_csv(df)
            anonymized_data = anonymized_df.to_dict(orient='records')
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Unsupported file type'})
            }
        
        s3.put_object(Bucket=output_bucket_name, Key=file_url.split('/')[-1], Body=json.dumps(anonymized_data))
        
        logger.info(f'Processed and saved anonymized data for {file_url}')
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'File processed and saved'})
        }
    except Exception as e:
        logger.error(f'Error processing file: {e}')
        return {
            'statusCode': 500,
            'body': json.dumps({'message': str(e)})
        }
