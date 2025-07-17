import boto3
import os
import json
import logging

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
OBJECT_KEY = os.environ.get('S3_OBJECT_KEY')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
  """
  Main Lambda handler function.
  It fetches a JSON file from S3, filters for the top 10 movies and
  sends the filtered list to an SQS queue.
  """
  logger.info(f"Processing file '{OBJECT_KEY}' from bucket '{BUCKET_NAME}'.")

  try:
    # Fetch the movie list from the bucket
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)
    content = response['Body'].read()
    data = json.loads(content)
    all_movies = data.get('items', [])

    if not all_movies:
      logger.warning("No items found in the JSON data.")
      return {'statusCode': 200, 'body': 'No movie items to process.'}

    # Sort the movies by rank and filter the top 10
    ranked_movies = [movie for movie in all_movies if movie.get('rank')]
    ranked_movies.sort(key=lambda movie: int(movie['rank']))
    top_10_movies = ranked_movies[:10]
    
    # Send the result to SQS
    message_body = json.dumps(top_10_movies)
    sqs_client.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=message_body)
    
    logger.info(f"Successfully sent top 10 movies to SQS queue: {SQS_QUEUE_URL}")
    
    return {'statusCode': 200, 'body': 'Successfully processed and sent top 10 movies to SQS.'}
      
  except Exception as e:
    logger.error(f"An unexpected error occurred: {e}")
    return {'statusCode': 500, 'body': json.dumps(f"An unexpected error occurred: {str(e)}")}
