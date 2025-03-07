import os
from dotenv import load_dotenv
from src.aws.s3_operations import create_s3_bucket, upload_to_s3, read_csv_from_s3, list_files_in_bucket
from src.aws.clean_data import clean_data
from src.aws.dynamo_operations import create_dynamodb_table, insert_item
from src.aws.sns_operations import create_sns_topic

# Load environment variables
load_dotenv()
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
RAW_FOLDER_PATH = os.getenv("RAW_FOLDER_PATH", "../../raw")  # Default raw data folder


def process_csv_to_dynamodb(bucket_name, directory=RAW_FOLDER_PATH):
    """Handles the full process: Upload CSVs, read, clean, and insert into DynamoDB."""

    # Upload CSV to S3 from a given directory
    upload_to_s3(directory)

    s3_files = list_files_in_bucket()

    dataframes = {}

    for file in s3_files:
        if file.endswith(".csv"):  # Ensure only CSV files are processed
            df_name = os.path.splitext(os.path.basename(file))[0]  # Extract filename without extension
            df = read_csv_from_s3(bucket_name, file)
            if df is not None:  # Avoid storing None values
                dataframes[df_name] = df

    # Print available DataFrames
    print("\n✅ Loaded DataFrames:")
    for name, df in dataframes.items():
        print(f"- {name}: {df.shape[0]} rows, {df.shape[1]} columns")

    # Check if we have data to process
    if not dataframes:
        print("❌ No valid CSV files found in S3. Exiting.")
        return

    # Clean Data
    cleaned_df = clean_data(bucket_name, list(dataframes.keys()))

    # Initialize DynamoDB
    table = create_dynamodb_table()

    # Create SNS Topic
    sns_topic_arn = create_sns_topic()

    # Insert all cleaned DataFrames into DynamoDB
    for name, df in dataframes.items():
        if df.empty:
            print(f"⚠️ Skipping {name}: Empty DataFrame.")
            continue

        print(f"\n📌 Processing {name} into DynamoDB...")
        for _, row in cleaned_df.iterrows():
            item = row.to_dict()
            insert_item(table, sns_topic_arn, item)

    print("\n✅ Data processing complete.")


if __name__ == "__main__":
    create_s3_bucket(BUCKET_NAME)
    process_csv_to_dynamodb(bucket_name=BUCKET_NAME)
