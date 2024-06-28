import logging
import os
from typing import Union

from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud import bigquery


READ_BUCKET = os.environ['READ_BUCKET']
WRITE_BUCKET = os.environ['WRITE_BUCKET']
BQ_TABLE_ID = os.environ['BQ_TABLE_ID']
PROJECT_ID = os.environ['PROJECT_ID']
SUBSCRIPTION_ID = os.environ['SUBSCRIPTION_ID']


logging.basicConfig(level=logging.INFO)


def download_blob_into_memory(bucket_name, blob_name):
    """Downloads a blob into memory."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your GCS object
    # blob_name = "storage-object-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(blob_name)
    contents = blob.download_as_bytes()

    logging.info(f"Downloaded storage object {blob_name} from bucket {bucket_name} as the following bytes object: {contents.decode("utf-8")}")

    return contents


def bq_stream_insert(filename, gcs_event_t, pubsub_publish_t, python_start_t, python_after_gcs_write_t):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    rows_to_insert = [
        {"filename": filename, "gcs_event_time": gcs_event_t, "pubsub_publish_time": pubsub_publish_t, "python_start_time": python_start_t, "python_after_gcs_write_time": python_after_gcs_write_t,},
    ]

    errors = client.insert_rows_json(BQ_TABLE_ID, rows_to_insert)  # Make an API request.
    if errors == []:
        logging.info(f"New rows have been added.")
    else:
        logging.info(f"Encountered errors while inserting rows: {errors}")


def convert_timestring_to_epoch(time_str, who):
    if re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3,6}Z$', time_str):
        time_obj = datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        epoch_time_in_ms = round(int(time_obj.timestamp() * 1000))
        print("{} Time STR: {} | Epoch: {}".format(who,time_str, epoch_time_in_ms))
        return epoch_time_in_ms
    elif re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$', time_str):
        time_obj = datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ")
        epoch_time_in_ms = round(int(time_obj.timestamp() * 1000))
        print("{} Time STR: {} | Epoch: {}".format(who,time_str, epoch_time_in_ms))
        return epoch_time_in_ms
    else:
        print("{} Time STR: {} | Epoch: {}".format(who,time_str, "-1"))
        return -1


def read_and_transform_file(pubsub_message):
    python_start_time = str(time.time_ns() // 1000000)
    filename = pubsub_message['message']['attributes']['objectId']
    filecontent = download_blob_into_memory(READ_BUCKET, filename)
    python_after_gcs_read_time = str(time.time_ns() // 1000000)
    gcs_event_time = convert_timestring_to_epoch(jsondata['message']['attributes']['eventTime'], "GCS Event Time")
    pubsub_publish_time = convert_timestring_to_epoch(jsondata['message']['publishTime'], "Pub/Sub Publish Time")
    bq_stream_insert(filename, gcs_event_time, pubsub_publish_time, python_start_time, python_after_gcs_read_time)
    logging.info(f"Log for file: {filename} | gcs_event_time {gcs_event_time} | pubsub_publish_time {pubsub_publish_time} | python_start_time {python_start_time} | python_after_gcs_write_time {python_after_gcs_read_time}")


def pubsub_notification(): 
    subscriber = pubsub_v1.SubscriberClient()
    # The `subscription_path` method creates a fully qualified identifier
    # in the form `projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}`
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message}.")
        logging.info(f"Received {message}.")
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")
    logging.info(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.

    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            pubsub_message = streaming_pull_future.result()
            read_and_transform_file(pubsub_message)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.

### main
pubsub_notification()