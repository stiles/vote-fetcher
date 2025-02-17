import pandas as pd
import boto3

def save_to_csv(dataframe, path):
    """
    Save a DataFrame to a CSV file.
    :param dataframe: pd.DataFrame to save
    :param path: Path to save the file
    """
    dataframe.to_csv(path, index=False)
    print(f"Saved data to {path}")


def save_to_s3(local_path, bucket, s3_path, profile_name=None):
    """
    Upload a file to S3 using a specific AWS profile (if provided)
    :param local_path: Path to the local file
    :param bucket: S3 bucket name
    :param s3_key: Key (path) in the S3 bucket
    :param profile_name: AWS profile name (if using named profiles)
    """
    if profile_name:
        session = boto3.Session(profile_name=profile_name)
        s3_client = session.client("s3")
    else:
        s3_client = boto3.client("s3")  # Uses default profile

    try:
        s3_client.upload_file(local_path, bucket, s3_path)
        print(f"✅ Uploaded {local_path} to s3://{bucket}/{s3_path}")
    except Exception as e:
        print(f"❌ Failed to upload {local_path} to S3: {e}")

