import pandas as pd
import requests
import json
import datetime
import os
import time

from google.cloud import pubsub_v1

class Publisher:
    def __init__(self):
        self.project_id = "covid-data-project-328321"
        self.topic_id = "covid-raw-data"
        self.recheck_time=86400
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "covid-data-project-328321-c37b3c4360fb.json"

    def fetch_data(self):

        # yesterdays_date = str(datetime.date.today() - datetime.timedelta(days=1)) + "T00:00:00.000Z"
        yesterdays_date = "2021-10-10T00:00:00.000Z"
        start_date = end_date = yesterdays_date
        url = "https://webhooks.mongodb-stitch.com/api/client/v2.0/app/covid-19-qppza/service/REST-API/incoming_webhook/us_only?min_date=" + start_date + "&max_date=" + end_date
        response = requests.get(url)
        if response.json() == []:
            print("No Data found")
            self.recheck_time=3600
        else:
            print("Data found")
        string_msg = json.dumps(response.json())
        string_msg = string_msg.encode("utf-8")
        return string_msg

    def publish(self):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(self.project_id, self.topic_id)

        while True:
            data=self.fetch_data()
            if self.recheck_time == 86400:
                future = publisher.publish(topic_path, data)
                print(future.result())
            else:
                print("Data for yesterday is not uploaded yet. Will retry in 1 hour.")
            time.sleep(self.recheck_time)


if __name__ == "__main__":
    try:
        while True:
            pub = Publisher()
            pub.publish()
    except KeyboardInterrupt:
        pass




