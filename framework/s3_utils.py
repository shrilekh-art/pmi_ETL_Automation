import boto3
import os


s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)


def download_file(bucket, s3_key, local_path):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3.download_file(bucket, s3_key, local_path)
    print(f"Downloaded: s3://{bucket}/{s3_key} → {local_path}")


def upload_file(local_path, bucket, s3_key):
    s3.upload_file(local_path, bucket, s3_key)
    print(f"Uploaded: {local_path} → s3://{bucket}/{s3_key}")