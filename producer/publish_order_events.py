"""Publish sample order events to Google Cloud Pub/Sub.

Run this locally after authenticating with:
gcloud auth application-default login

The script is intentionally simple so the Pub/Sub -> Dataflow streaming
concept is easy to understand.
"""

import argparse
import json
import time

from google.cloud import pubsub_v1


def publish_events(project_id: str, topic_id: str, events_file: str):
    """Read JSON Lines and publish each event to Pub/Sub."""

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    with open(events_file, "r", encoding="utf-8") as file:
        for line in file:
            event = json.loads(line)

            # Pub/Sub expects bytes, so convert the JSON object to UTF-8.
            payload = json.dumps(event).encode("utf-8")

            # publish() is asynchronous; result() waits for the server response.
            message_id = publisher.publish(topic_path, payload).result()

            print(f"Published {event['order_id']} -> message_id={message_id}")

            # Small delay makes the streaming demo easier to observe.
            time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--topic_id", required=True)
    parser.add_argument("--events_file", required=True)
    args = parser.parse_args()

    publish_events(args.project_id, args.topic_id, args.events_file)
