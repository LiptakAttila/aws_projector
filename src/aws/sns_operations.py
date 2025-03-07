import boto3
import os
import json
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
SNS_TOPIC_NAME = "DynamoDB_Notifications"
SUBSCRIBER_EMAIL = os.getenv("SUBSCRIBER_EMAIL")

sns_client = boto3.client("sns", region_name=AWS_REGION)


def create_sns_topic():
    """Creates an SNS topic if it doesn't exist and subscribes an email only if not subscribed yet."""

    try:
        topics = sns_client.list_topics()
        existing_topic_arn = None

        for topic in topics.get("Topics", []):
            if SNS_TOPIC_NAME in topic["TopicArn"]:
                existing_topic_arn = topic["TopicArn"]
                print(f"✅ SNS topic already exists: {existing_topic_arn}")
                break

        if not existing_topic_arn:
            response = sns_client.create_topic(Name=SNS_TOPIC_NAME)
            existing_topic_arn = response["TopicArn"]
            print(f"✅ SNS Topic created: {existing_topic_arn}")

        subscriptions = sns_client.list_subscriptions_by_topic(TopicArn=existing_topic_arn)
        subscribed_emails = [sub["Endpoint"] for sub in subscriptions.get("Subscriptions", [])]

        if SUBSCRIBER_EMAIL not in subscribed_emails:
            sns_client.subscribe(
                TopicArn=existing_topic_arn,
                Protocol="email",
                Endpoint=SUBSCRIBER_EMAIL
            )
            print(f"📩 Subscription request sent to {SUBSCRIBER_EMAIL}. Check your email to confirm.")
        else:
            print(f"✅ Email {SUBSCRIBER_EMAIL} is already subscribed.")

        return existing_topic_arn

    except ClientError as e:
        print(f"❌ Error creating SNS topic: {e.response['Error']['Message']}")
        return None


def send_sns_notification(sns_topic_arn, action, item):
    """Sends an SNS notification for a DynamoDB action."""
    message = json.dumps({"action": action, "item": item})
    sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=message,
        Subject=f"DynamoDB {action} Notification"
    )
    print(f"📢 SNS notification sent for {action}.")
