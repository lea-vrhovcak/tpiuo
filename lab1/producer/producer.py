import os
import time
import json
import requests
from google.cloud import pubsub_v1

print("PRODUCER POKRENUT")

#VARIJABLE
PROJECT_ID = os.environ["PROJECT_ID"]
TOPIC_ID = os.environ["PUBSUB_TOPIC"]

REDDIT_CLIENT_ID = os.environ["REDDIT_CLIENT_ID"]
REDDIT_CLIENT_SECRET = os.environ["REDDIT_CLIENT_SECRET"]
REDDIT_USERNAME = os.environ["REDDIT_USERNAME"]

#PUBSUB
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

#REDDIT AUTH
auth = requests.auth.HTTPBasicAuth(
    REDDIT_CLIENT_ID,
    REDDIT_CLIENT_SECRET
)

headers = {
    "User-Agent": f"script:{REDDIT_USERNAME}:v1.0"
}

auth_data = {
    "grant_type": "password",
    "username": REDDIT_USERNAME,
    "password": os.environ["REDDIT_PASSWORD"]
}

token_res = requests.post(
    "https://www.reddit.com/api/v1/access_token",
    auth=auth,
    data=auth_data,
    headers=headers
)

token_res.raise_for_status()
access_token = token_res.json()["access_token"]

headers["Authorization"] = f"bearer {access_token}"

print("SPOJENI NA REDDIT")

#POSTOVI
url = "https://oauth.reddit.com/r/dataengineering/top"
params = {
    "t": "all",
    "limit": 10
}

response = requests.get(url, headers=headers, params=params)
response.raise_for_status()

posts = response.json()["data"]["children"]

print(f"FETCHALI {len(posts)} OBJAVA")

#OBJAVI
for post in posts:
    payload = post  # UNFILTERED
    data = json.dumps(payload).encode("utf-8")

    publisher.publish(topic_path, data).result()
    print("OBJAVLJEN POST:", post["data"]["title"])

print("SVE PORUKE OBJAVLJENE")

#BESKONACNA PETLJA
while True:
    time.sleep(60)
