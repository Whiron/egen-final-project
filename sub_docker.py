import base64
import json
import pandas as pd
from google.cloud.storage import Client

class BucketManager:
    def __init__(self,event,context):
        self.event = event
        self.context = context
        self.bucket_name = "raw-coviddata-bucket"

    def get_data(self):
        message = base64.b64decode(self.event['data']).decode('utf-8')
        data=pd.DataFrame(json.loads(message))
        date=str(data['date'][0])[:10]
        return date,data

    def load_to_bucket(self,date,data):
        google_client = Client()
        bucket_obj = google_client.bucket(self.bucket_name)
        blob_obj = bucket_obj.blob(f'covid_data_{date}}.csv')
        blob_obj.upload_from_string(data.to_csv(),content_type="text/csv")
        print("Data added to storage as covid_data.csv")

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    bucket_manager_obj = BucketManager(event,context)
    date,data = bucket_manager_obj.get_data()
    bucket_manager_obj.load_to_bucket(date,data)
