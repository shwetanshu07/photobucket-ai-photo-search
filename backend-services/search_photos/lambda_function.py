import json
import boto3
import os
from opensearchpy import OpenSearch


# initialize AWS clients
lex_client = boto3.client('lexv2-runtime', region_name='us-east-1')

# Lex Bot configuration
BOT_ID = os.getenv("BOT_ID")
BOT_ALIAS_ID = os.getenv("BOT_ALIAS_ID")
LOCALE_ID = 'en_US'
SESSION_ID = 'test-session'

# for elastic search
OPENSEARCH_HOST = os.getenv('OPENSEARCH_HOST')
OPENSEARCH_USER = os.getenv('OPENSEARCH_USER')
OPENSEARCH_PASS = os.getenv('OPENSEARCH_PASS')
INDEX_NAME = "photos"

elastic_client = OpenSearch(
    hosts=[{"host": OPENSEARCH_HOST, "port": 443}],
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
    use_ssl=True,
    verify_certs=True,
)


def response_handler(status_code, response_payload):

    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "OPTIONS,GET"
    }

    return {
        "statusCode": status_code,
        "headers": headers,
        "body": json.dumps(response_payload)
    }

def lambda_handler(event, context):

    # check the incoming event details and the context
    print("🟡 Event details = ", event)

    try:
        query = None
        if 'queryStringParameters' in event and event['queryStringParameters']:
            query = event['queryStringParameters'].get('q', '')
        
        if not query:
            return  response_handler(400, {'results': []})
        
        print(f"Search query = {query}")

        # disambiguate query using Lex
        lex_response = lex_client.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=SESSION_ID,
            text=query
        )
        print("Lex resp = ", lex_response)
        
        keywords = []
        
        # Extract slots from Lex response
        slots = lex_response.get('sessionState', {}).get('intent', {}).get('slots', {})
        if slots:
            for slot_name, slot_data in slots.items():
                if slot_data and slot_data.get('value'):
                    keywords.append(slot_data['value']['originalValue'])
        
        print("🟡 keywords = ", keywords)
        if not keywords:
            print("🔶 No keywords found")
            return response_handler(200, {'results': []})

        photos = search_elasticsearch(keywords)
        print("Total photos found = ", len(photos))
        if not photos:
            print("🔶 No photos found")
            return response_handler(200, {'results': []})

        # generate presigned urls 
        pressigned_urls = generate_pre_signed_urls(photos)
        return  response_handler(200, {'results': pressigned_urls})
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return response_handler(500, {'error': str(e),'results': []})


def search_elasticsearch(keywords):
    """
    Search OpenSearch for photos with the given labels
    """
    try:
        clause = [
            {"match": {"labels": keyword}}
            for keyword in keywords
        ]
        query_body = {
            "query": {
                "bool": {
                    "should": clause,
                    "minimum_should_match": 1
                }
            },
            "size": 100
        }
        
        response = elastic_client.search(
            index=INDEX_NAME,
            body=query_body
        )

        print("opensearch reponse = ", response)
                
        hits = response.get('hits', {}).get('hits', [])
        return [hit['_source'] for hit in hits]
            
    except Exception as e:
        print(f"Error searching OpenSearch: {str(e)}")
        return []

def generate_pre_signed_urls(photos):
    """
    generates pre-signed urls for the photos that have been found
    """
    results = []
    s3_client = boto3.client('s3')
    
    for photo in photos:
        bucket = photo.get('bucket', '')
        key = photo.get('objectKey', '')
        labels = photo.get('labels', [])
        try:
            url = s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': bucket,
                    'Key': key
                },
                ExpiresIn=3600 
            )
        except Exception as e:
            raise Exception(f"Error generating pre-signed URL: {str(e)}")

        results.append({
            'url': url,
            'labels': labels
        })
    
    return results