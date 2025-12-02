import json
import boto3
import requests
from datetime import datetime
from opensearchpy import OpenSearch
import os

# Initialize AWS clients
s3 = boto3.client('s3')
rekognition = boto3.client('rekognition', region_name='us-east-1')

# for elastic search
OPENSEARCH_HOST = os.getenv('OPENSEARCH_HOST')
OPENSEARCH_USER = os.getenv('OPENSEARCH_USER')
OPENSEARCH_PASS = os.getenv('OPENSEARCH_PASS')
INDEX_NAME = "photos"

# creating elastic client
elastic_client = OpenSearch(
    hosts=[{"host": OPENSEARCH_HOST, "port": 443}],
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
    use_ssl=True,
    verify_certs=True,
)

def lambda_handler(event, context):
    """
    Triggered by S3 PUT events. Detects labels using Rekognition,
    retrieves custom labels from S3 metadata and indexes to ElasticSearch
    """
    
    print("received event = ", json.dumps(event))
    
    try:
        # Extract S3 bucket and object key from the event
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            print(f"Processing file: {key} from bucket: {bucket}")

            # 1. Get custom labels from S3 metadata
            custom_labels = get_custom_labels(bucket, key)
            print(f"Custom labels = {custom_labels}")
            
            # 2. Detect labels using Rekognition
            rekognition_labels = detect_labels(bucket, key)
            print(f"Rekognition labels: {rekognition_labels}")

            # 3. Combine all labels
            all_labels = list(set(rekognition_labels + custom_labels))
            print(f"All labels: {all_labels}")

            # 4. Create JSON object for ElasticSearch
            timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            photo_doc = {
                'objectKey': key,
                'bucket': bucket,
                'createdTimestamp': timestamp,
                'labels': all_labels
            }
            index_to_elasticsearch(photo_doc, key)
            
        return {
            'statusCode': 200,
            'body': json.dumps('Successfully processed and indexed photos')
        }
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing photo = {str(e)}')
        }

def detect_labels(bucket, key):
    """
    Use Rekognition to detect labels in the image.
    detect labels doc : https://docs.aws.amazon.com/rekognition/latest/APIReference/API_DetectLabels.html
    """
    try:
        response = rekognition.detect_labels(
            Image={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': key
                }
            },
            MaxLabels=10,
            MinConfidence=70    # default is 55
        )

        print("Response from Rekognition Detect Labels = ", response)
        
        # Extract label names
        labels = [label['Name'].lower() for label in response['Labels']]
        return labels
        
    except Exception as e:
        print(f"Error detecting labels: {str(e)}")
        return []

def get_custom_labels(bucket, key):
    """
    Retrieve custom labels from S3 object metadata.
    """
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        metadata = response.get('Metadata', {})
        
        # Get custom labels from metadata
        custom_labels_str = metadata.get('customlabels', '')
        print("custom_labels_str = ", custom_labels_str)
        
        if custom_labels_str:
            # Split by comma and clean up
            custom_labels = [label.strip().lower() for label in custom_labels_str.split(',')]
            return [label for label in custom_labels if label]
        
        return []
        
    except Exception as e:
        print(f"Error getting custom labels: {str(e)}")
        return []

def index_to_elasticsearch(document, doc_id):
    """
    Index the document to ElasticSearch.
    """
    try:
        response = elastic_client.index(
            index=INDEX_NAME,
            id=doc_id,
            body=document,
            refresh=True  # Make immediately searchable
        )
        print(f"Elasticsearch response = {response}")

        if response['result'] not in ['created', 'updated']:
            raise Exception(f"Unexpected result: {response['result']}")
            
    except Exception as e:
        print(f"Error indexing to ElasticSearch: {str(e)}")
        raise