import pandas as pd
from decimal import Decimal
import numpy as np
from dotenv import load_dotenv
from src.aws.s3_operations import read_csv_from_s3

load_dotenv()


def convert_to_decimal(df):
    """Convert specified numeric columns in the DataFrame to Decimal."""
    for column in df.select_dtypes(include=[np.float64]):
        df[column] = df[column].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else x)
    return df


def clean_data(bucket_name, files):
    """Cleans the CSV files and merges them if possible."""
    cleaned_dataframes = []

    for file_name in files:
        df = read_csv_from_s3(bucket_name, file_name)
        pd.set_option("display.max_columns", None)

        if df is not None:
            if "id" not in df.columns:
                df["id"] = df.index.astype(str)
            else:
                df["id"] = df["id"].fillna(df.index.astype(str))

            df.drop_duplicates(inplace=True)
            df.fillna("", inplace=True)
            cleaned_dataframes.append(df)

    if len(cleaned_dataframes) < 2:
        print("❌ Not enough files to merge. Returning available DataFrame.")
        return cleaned_dataframes[0] if cleaned_dataframes else pd.DataFrame()

    common_keys = set(cleaned_dataframes[0].columns).intersection(cleaned_dataframes[1].columns)
    merge_key = "customer_id" if "customer_id" in common_keys else list(common_keys)[0]

    merged_df = cleaned_dataframes[0].merge(cleaned_dataframes[1], on=merge_key, how="inner")

    if merged_df is not None:
        if "id" not in merged_df.columns:
            merged_df["id"] = merged_df.index.astype(str)
        else:
            merged_df["id"] = merged_df["id"].fillna(merged_df.index.astype(str))

    merged_df = convert_to_decimal(merged_df)

    return merged_df if not merged_df.empty else pd.DataFrame()
