import os
import json
from pydoc import cli
import queue
import boto3
import requests
from dotenv import load_dotenv

#testing my key
url = "https://jsonplaceholder.typicode.com/posts"


def producer(url: str) -> dict:
    data = requests.get(url).json()
    return data


def transformer(data: dict) -> dict:
    data_processed = {}
    for item in data:
        if item["userId"] not in data_processed:
            data_processed[item["userId"]] = {
                "records": [
                    {"id": item["id"], "title": item["title"], "body": item["body"]}
                ]
            }
        if item["userId"] in data_processed:
            record = data_processed[item["userId"]]
            record["records"].append(
                {"id": item["id"], "title": item["title"], "body": item["body"]}
            )
    return data_processed


def publisher(data: dict) -> None:
    try:
        client = boto3.client(
            "sqs",
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY"),
            region_name='us-west-2'
        )
        queue = client.get_queue_url(QueueName=os.getenv("QUEUE"))["QueueUrl"]
        response = client.send_message(QueueUrl=queue, MessageBody=json.dumps(data))
        return response

    except Exception as e:
        return e

if __name__ == "__main__":
    load_dotenv(".env")
    data = producer(url)
    data = transformer(data)
    print(publisher(data))
