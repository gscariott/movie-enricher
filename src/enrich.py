import boto3
import os
import json
import logging
from urllib import request, error
from datetime import datetime

s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')

OMDB_SECRET_NAME = os.environ.get('OMDB_SECRET_NAME')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_ARN').split(":::")[-1]

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def fetch_movie_details(imdb_id):
  """Fetches detailed movie information from the OMDb API."""
  try:
    secret_response = secrets_client.get_secret_value(SecretId=OMDB_SECRET_NAME)
    secret = json.loads(secret_response['SecretString'])
    omdb_api_key = secret.get('OMDB_API_KEY')
  except Exception as e:
    logger.error(f"Failed to retrieve secret '{OMDB_SECRET_NAME}': {e}")
    raise e
  
  api_url = f"https://www.omdbapi.com/?apikey={omdb_api_key}&i={imdb_id}"
  
  try:
    with request.urlopen(api_url, timeout=10) as response:
      if 200 <= response.status < 300:
        details = json.loads(response.read().decode('utf-8'))
        logger.info(f"Successfully fetched details for {imdb_id}.")
        return details
      else:
        logger.error(f"OMDb API returned status {response.status} for {imdb_id}")
        return {'Response': 'False', 'Error': f'API returned HTTP Status {response.status}'}
  except error.URLError as e:
    logger.error(f"Error calling OMDb API for {imdb_id}: {e}")
    return {'Response': 'False', 'Error': str(e)}

def enrich_movie_data(source_object, details_object):
  """
  Enriches a source movie object with additional data from a details object.
  """
  
  # Process the 'Writer' and 'Actors' strings into lists
  writers = [writer.strip() for writer in details_object.get("Writer", "").split(",")]
  actors = [actor.strip() for actor in details_object.get("Actors", "").split(",")]

  enriched_data = {
    "id": source_object.get("id"),
    "title": source_object.get("title"),
    "year": source_object.get("year"),
    "original_list_data": {
      "rank": source_object.get("rank"),
      "image_url": source_object.get("image"),
      "imdb_rating": source_object.get("imDbRating"),
      "imdb_rating_count": source_object.get("imDbRatingCount")
    },
    "details": {
      "full_title": details_object.get("fullTitle") or source_object.get("fullTitle"),
      "synopsis": details_object.get("Plot"),
      "genre": details_object.get("Genre"),
      "runtime": details_object.get("Runtime"),
      "rating": details_object.get("Rated"),
      "release_date": details_object.get("Released"),
      "language": details_object.get("Language"),
      "country": details_object.get("Country"),
      "awards": details_object.get("Awards"),
      "box_office": details_object.get("BoxOffice"),
      "poster_url": details_object.get("Poster")
    },
    "crew": {
      "director": details_object.get("Director"),
      "writers": writers,
      "actors": actors
    },
    "external_ratings": []
  }

  # Process the ratings array, adding imdbVotes to the IMDb entry
  for rating in details_object.get("Ratings", []):
      processed_rating = {
        "source": rating.get("Source"),
        "value": rating.get("Value")
      }
      if rating.get("Source") == "Internet Movie Database":
        processed_rating["votes"] = details_object.get("imdbVotes")
      
      enriched_data["external_ratings"].append(processed_rating)

  return enriched_data
  
def upload_to_s3(bucket, data):
  """
  Uploads the list of enriched movie data as a single JSON file to S3.
  """
  timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
  file_name = f"enriched-top-10-movies-{timestamp}.json"
  
  try:
    s3_client.put_object(
      Bucket=bucket,
      Key=file_name,
      Body=json.dumps(data, indent=2).encode('utf-8'),
      ContentType='application/json'
    )
    logger.info(f"Successfully uploaded '{file_name}' to bucket '{bucket}'.")
  except Exception as e:
    logger.error(f"Error uploading enriched data to S3: {e}")
    raise

def lambda_handler(event, context):
  """
  Orchestrates the process of enriching movie data from SQS and saving it to S3.
  """
  all_enriched_movies = []
  
  for record in event['Records']:
    try:
      movies_from_sqs = json.loads(record['body'])

      for movie in movies_from_sqs:
        imdb_id = movie.get('id')
        for attempt in range(3):
          omdb_details = fetch_movie_details(imdb_id)
          if omdb_details.get('Response') != 'False': break
          if attempt < 2: time.sleep(2)
        enriched_movie = enrich_movie_data(movie, omdb_details)
        all_enriched_movies.append(enriched_movie)

    except json.JSONDecodeError as e:
      logger.error(f"Error decoding JSON from SQS message body: {e}")
      # Skip to the next message in the batch
      continue
          
  upload_to_s3(bucket=S3_BUCKET_NAME, data=all_enriched_movies)

  return {
    'statusCode': 200,
    'body': 'Successfully processed all movies.'
  }
