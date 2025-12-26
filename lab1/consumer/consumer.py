import os
import json
from google.cloud import pubsub_v1

print("CONSUMER POKRENUT")

#VARIJABLE
PROJECT_ID = os.environ["PROJECT_ID"]
SUBSCRIPTION_ID = os.environ["PUBSUB_SUBSCRIPTION"]

#PUBSUB
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(
    PROJECT_ID, SUBSCRIPTION_ID
)

print(f"PRATIMO PORUKE NA {subscription_path}")

#CALLBACK
def callback(message):
    print("\nDOBIVENA NOVA PORUKA")

    try:
        data = json.loads(message.data.decode("utf-8"))
        print(json.dumps(data, indent=2))
    except Exception as e:
        print("Failed to decode message:", e)
        print("Raw message:", message.data)

    message.ack()
    print("--- MESSAGE ACKNOWLEDGED ---")

#LISTEN
streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=callback
)

print("STEP 3: consumer is running (CTRL+C to stop)")

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("Consumer stopped.")
