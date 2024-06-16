import json
import boto3
from boto3.dynamodb.conditions import Attr
import logging

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DynamoDB 리소스 및 테이블 생성
dynamodb = boto3.resource('dynamodb')
book_table = dynamodb.Table('busan_book')

# S3 클라이언트 생성
s3_client = boto3.client('s3')

def get_books_by_title(query):
    try:
        response = book_table.scan(
            FilterExpression=Attr('TITLE_NM').contains(query) | Attr('AUTHR_NM').contains(query)
        )
        return response['Items']
    except Exception as e:
        logger.error(f"Error getting books by title: {str(e)}")
        return []

def process_busan_book(csv_data):
    for index, data1 in enumerate(csv_data):
        if index == 0:
            continue

        data = data1.split(",")
        if len(data) < 6:
            logger.warning(f"Skipping malformed row: {data1}")
            continue

        logger.info(f"Processing row {index}: {data}")

        try:
            book_table.put_item(
                Item={
                    "AUTHR_NM": data[0],
                    "PBLICTE_YEAR": data[1],
                    "TITLE_NM": data[2],
                    "LBRRY_CD": data[3],
                    "REGIST_NO": data[4],
                    "LBRRY_NM": data[5]
                },
                ConditionExpression='attribute_not_exists(REGIST_NO)'
            )
            logger.info(f"Inserted item {index}: {data}")
        except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            logger.warning(f"Item already exists at row {index}: {data}")
        except Exception as e:
            logger.error(f"Error inserting item at row {index}: {data} - {str(e)}")

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    # S3 이벤트 처리
    if 'Records' in event and 's3' in event['Records'][0]:
        try:
            bucket = event['Records'][0]['s3']['bucket']['name']
            csv_file_name = event['Records'][0]['s3']['object']['key']
            logger.info(f"Processing file: {csv_file_name} from bucket: {bucket}")

            csv_object = s3_client.get_object(Bucket=bucket, Key=csv_file_name)
            csvFileReader = csv_object['Body'].read().decode("utf-8")
            csv_data = csvFileReader.split("\n")

            logger.info("Parsing CSV data")

            if 'Busan_book_random_100' in csv_file_name:
                process_busan_book(csv_data)

            return {
                'statusCode': 200,
                'body': json.dumps('Successfully processed CSV data from S3 and inserted into DynamoDB')
            }

        except Exception as e:
            logger.error(f"Unhandled exception: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps(f"Error processing file: {str(e)}")
            }

    # API Gateway 이벤트 처리
    elif 'queryStringParameters' in event:
        try:
            if 'query' not in event['queryStringParameters']:
                raise ValueError("Missing query parameter")
            
            query = event['queryStringParameters']['query']
            logger.info(f"User query: {query}")
            
            books = get_books_by_title(query)
            logger.info(f"Fetched books: {books}")
            
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps(books)
            }
        except Exception as e:
            logger.error(f"Error in lambda_handler: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': str(e)})
            }
    
    # 처리되지 않은 이벤트
    else:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Unsupported event format'})
        }
