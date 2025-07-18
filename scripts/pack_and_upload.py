import argparse
import boto3
import logging
import os
import zipfile

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_NAME = os.environ.get('ARTIFACT_BUCKET_NAME')

def zip_single_file(source_file_path, output_zip_path):
  """
  Creates a .zip archive containing the specified source file.
  """
  if not os.path.exists(source_file_path):
    logging.error(f"Source file '{source_file_path}' not found.")
    raise FileNotFoundError(f"File not found: {source_file_path}")

  file_name = os.path.basename(source_file_path)

  with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
    # 'arcname' ensures the file is stored at the root of the zip archive.
    zipf.write(source_file_path, arcname=file_name)

  logging.info("Zip creation completed.")
  return output_zip_path

def upload_to_s3(file_path, object_key):
  """Uploads a file to the specified S3 bucket."""
  s3_client = boto3.client('s3')
  try:
    s3_client.upload_file(file_path, BUCKET_NAME, object_key)
    logging.info("Upload to S3 completed successfully.")
  except Exception as e:
    logging.error(f"Failed to upload to S3: {e}")
    raise

def main():
  parser = argparse.ArgumentParser(description="Zip a file and upload it to S3.")
  parser.add_argument(
    "--source-file",
    required=True,
    help="Relative path to the file that should be zipped (e.g., src/enrich.py)"
  )
  parser.add_argument(
    "--object-key",
    required=True,
    help="The key (path/name) of the object in the S3 bucket."
  )

  args = parser.parse_args()
  zip_file_path = "lambda_package.zip"

  try:
    # 1. Zip the source file
    zip_single_file(args.source_file, zip_file_path)

    # 2. Upload the zip to S3
    upload_to_s3(zip_file_path, args.object_key)

  finally:
    # 3. Clean up local zip file
    if os.path.exists(zip_file_path):
        logging.info(f"Cleaning up local file '{zip_file_path}'...")
        os.remove(zip_file_path)

if __name__ == "__main__":
    main()
