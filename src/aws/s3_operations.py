import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import pandas as pd

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
RAW_FOLDER_PATH = os.getenv("RAW_FOLDER_PATH")

s3_client = boto3.client("s3", region_name=AWS_REGION)


def create_s3_bucket(bucket_name):
    """Creates an S3 bucket if it doesn't already exist."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            try:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": AWS_REGION}
                    if AWS_REGION != "us-east-1"
                    else {},
                )
                print(f"Bucket '{bucket_name}' created successfully.")
            except ClientError as err:
                print(f"Failed to create bucket: {err}")
        else:
            print(f"Error checking bucket: {e}")


def upload_to_s3(directory=RAW_FOLDER_PATH, file_paths=None):
    """Uploads all CSV files from the specified directory to S3."""
    if file_paths is None:
        file_paths = get_csv_files(directory)

    if not file_paths:
        print(f"No CSV files found in {directory}. Skipping upload.")
        return

    for file_path in file_paths:
        object_name = f"raw/{os.path.basename(file_path)}"
        try:
            s3_client.upload_file(file_path, BUCKET_NAME, object_name)
            print(f"Uploaded {file_path} to {BUCKET_NAME} as {object_name}")
        except Exception as e:
            print(f"Error uploading {file_path}: {e}")


def download_from_s3(object_name, file_name):
    """Downloads a file from S3."""
    try:
        s3_client.download_file(BUCKET_NAME, object_name, file_name)
        print(f"File {object_name} downloaded as {file_name}")
    except Exception as e:
        print(f"Error downloading file: {e}")


def list_files_in_bucket():
    """Lists files in the S3 bucket."""
    s3_list = []
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
        if 'Contents' in response:
            print(f"Files in bucket '{BUCKET_NAME}':")
            for obj in response['Contents']:
                print(f"- {obj['Key']}")
                s3_list.append(obj['Key'])
        else:
            print(f"No files found in bucket '{BUCKET_NAME}'.")
    except Exception as e:
        print(f"Error listing files: {e}")

    return s3_list


def delete_from_s3(object_name):
    """Deletes a file from S3."""
    try:
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=object_name)
        print(f"File {object_name} deleted from bucket '{BUCKET_NAME}'.")
    except Exception as e:
        print(f"Error deleting file: {e}")


def read_csv_from_s3(bucket_name, object_name):
    """Reads a CSV file directly from S3 and returns a DataFrame."""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
        df = pd.read_csv(response['Body'])
        print(f"Successfully read {object_name} from S3.")
        return df
    except Exception as e:
        print(f"Error reading {object_name} from S3: {e}")
        return None


def get_csv_files(directory):
    """Returns a list of CSV file paths in the given directory."""
    return [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".csv")]

#
# if __name__ == "__main__":

#     create_s3_bucket(BUCKET_NAME)
#
#     # upload_to_s3("../../raw/customers.csv", "raw/customers.csv")
#     # upload_to_s3("../../raw/orders.csv", "raw/orders.csv")
#
#     # df_orders = read_csv_from_s3("raw/orders.csv")
#     # print(df_orders)
