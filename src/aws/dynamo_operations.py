import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)


def create_dynamodb_table():
    """Creates a DynamoDB table if it does not already exist."""
    try:
        table = dynamodb.create_table(
            TableName=DYNAMODB_TABLE_NAME,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
        )
        print(f"Creating table... Waiting for it to be active: {DYNAMODB_TABLE_NAME}")
        table.meta.client.get_waiter("table_exists").wait(TableName=DYNAMODB_TABLE_NAME)
        print(f"Table created successfully: {DYNAMODB_TABLE_NAME}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Table {DYNAMODB_TABLE_NAME} already exists.")
        else:
            print(f"Error creating table: {e.response['Error']['Message']}")

    return dynamodb.Table(DYNAMODB_TABLE_NAME)


def insert_item(table, sns_topic_arn, item):
    """Inserts an item into DynamoDB and sends an SNS notification."""
    if 'id' not in item:
        print("Error: 'id' field is required.")
        return

    table.put_item(Item=item)
    from src.aws.sns_operations import send_sns_notification
    send_sns_notification(sns_topic_arn, "INSERT", item)
    print(f"Inserted item: {item}")


def update_item(table, sns_topic_arn, item_id, update_data):
    """Updates an item in DynamoDB and sends an SNS notification."""
    update_expression = "SET " + ", ".join(f"{key} = :{key}" for key in update_data.keys())
    expression_values = {f":{key}": value for key, value in update_data.items()}

    table.update_item(
        Key={'id': item_id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_values
    )
    from src.aws.sns_operations import send_sns_notification
    send_sns_notification(sns_topic_arn, "UPDATE", {"id": item_id, **update_data})
    print(f"Updated item with id '{item_id}': {update_data}")


def delete_item(table, sns_topic_arn, item_id):
    """Deletes an item from DynamoDB and sends an SNS notification."""
    table.delete_item(Key={'id': item_id})
    from src.aws.sns_operations import send_sns_notification
    send_sns_notification(sns_topic_arn, "DELETE", {"id": item_id})
    print(f"Deleted item with id '{item_id}'")
