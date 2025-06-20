import boto3
import urllib3
import json
import os
from datetime import datetime

# AWS Clients
s3 = boto3.client('s3')
secretsmanager = boto3.client('secretsmanager')
http = urllib3.PoolManager()

# Env Vars (set in Lambda config)
BUCKET = os.environ['S3_BUCKET']
MEDIA_FOLDER = os.environ['MEDIA_FOLDER']  # e.g. 'raw/media_stats/'
MEDIA_METADATA_FOLDER = os.environ.get('MEDIA_METADATA_FOLDER', 'raw/media_metadata/')
ENGAGEMENT_FOLDER = os.environ.get('ENGAGEMENT_FOLDER', 'raw/media_engagement/')
EVENTS_FOLDER = os.environ.get('EVENTS_FOLDER', 'raw/events/')
SECRET_NAME = os.environ['SECRET_NAME']
MEDIA_IDS = os.environ['MEDIA_IDS'].split(',')

def get_api_token():
    secret = secretsmanager.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(secret['SecretString'])['api_token']

def fetch_media_stats(media_id, token):
    url = f"https://api.wistia.com/v1/stats/medias/{media_id}.json"
    headers = {"Authorization": f"Bearer {token}"}
    response = http.request("GET", url, headers=headers)
    if response.status == 200:
        return json.loads(response.data.decode("utf-8"))
    else:
        print(f"❌ Media stats error {media_id}: {response.status}")
        return None

def fetch_media_metadata(media_id, token):
    url = f"https://api.wistia.com/v1/medias/{media_id}.json"
    headers = {"Authorization": f"Bearer {token}"}
    response = http.request("GET", url, headers=headers)
    if response.status == 200:
        return json.loads(response.data.decode("utf-8"))
    else:
        print(f"❌ Media metadata error {media_id}: {response.status}")
        return None

def fetch_media_engagement(media_id, token):
    url = f"https://api.wistia.com/v1/stats/medias/{media_id}/engagement.json"
    headers = {"Authorization": f"Bearer {token}"}
    response = http.request("GET", url, headers=headers)
    if response.status == 200:
        return json.loads(response.data.decode("utf-8"))
    else:
        print(f"❌ Engagement error {media_id}: {response.status}")
        return None

def save_to_s3(folder, id_or_label, data):
    now = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    filename = f"{folder}{id_or_label}_{now}.json"
    s3.put_object(Bucket=BUCKET, Key=filename, Body=json.dumps(data))

def fetch_all_events(token, max_events=100):
    all_events = []
    page = 1
    base_url = "https://api.wistia.com/v1/stats/events.json"
    headers = {"Authorization": f"Bearer {token}"}
    while len(all_events) < max_events:
        url = f"{base_url}?page={page}"
        response = http.request("GET", url, headers=headers)
        if response.status != 200:
            print(f"❌ Error fetching events page {page}: {response.status}")
            break
        data = json.loads(response.data.decode("utf-8"))
        print(f"📄 Events page {page}: {len(data)} events")
        if not data:
            break
        all_events.extend(data)
        if len(all_events) >= max_events:
            break
        page += 1
    return all_events[:max_events]

def lambda_handler(event, context):
    token = get_api_token()

    # 1. Fetch media stats, metadata, and engagement for each media ID
    for media_id in MEDIA_IDS:
        print(f"🎬 Fetching media stats for: {media_id}")
        media_data = fetch_media_stats(media_id, token)
        if media_data:
            save_to_s3(MEDIA_FOLDER, media_id, media_data)
            print(f"✅ Media stats saved for {media_id}")
        else:
            print(f"❌ Failed to fetch stats for {media_id}")

        print(f"🎬 Fetching media metadata for: {media_id}")
        metadata = fetch_media_metadata(media_id, token)
        if metadata:
            save_to_s3(MEDIA_METADATA_FOLDER, media_id, metadata)
            print(f"✅ Media metadata saved for {media_id}")
        else:
            print(f"❌ Failed to fetch metadata for {media_id}")

        print(f"🎬 Fetching engagement for: {media_id}")
        engagement = fetch_media_engagement(media_id, token)
        if engagement:
            save_to_s3(ENGAGEMENT_FOLDER, media_id, engagement)
            print(f"✅ Engagement saved for {media_id}")
        else:
            print(f"❌ Failed to fetch engagement for {media_id}")

    # 2. Fetch events data (NEW)
    print("📥 Fetching all events...")
    events = fetch_all_events(token, max_events=100)
    if events:
        save_to_s3(EVENTS_FOLDER, "all_events", events)
        print(f"✅ Saved {len(events)} events to S3")
    else:
        print("⚠️ No events retrieved")
