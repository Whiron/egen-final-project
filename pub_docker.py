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
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "covid-data-project-328321-c37b3c4360fb.json"

    def fetch_data(self):

        # yesterdays_date = str(datetime.date.today() - datetime.timedelta(days=1)) + "T00:00:00.000Z"
        yesterdays_date = "2021-10-07T00:00:00.000Z"
        start_date = end_date = yesterdays_date
        url = "https://webhooks.mongodb-stitch.com/api/client/v2.0/app/covid-19-qppza/service/REST-API/incoming_webhook/us_only?min_date=" + start_date + "&max_date=" + end_date
        response = requests.get(url)

        string_msg = json.dumps(response.json())
        string_msg = string_msg.encode("utf-8")
        return string_msg

    def publish(self):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(self.project_id, self.topic_id)

        date_flag=True
        data=False
        while date_flag == True:
            data=self.fetch_data()
            if data!=False:
                date_flag=False

        future = publisher.publish(topic_path,data)
        print(future.result())


if __name__ == "__main__":
    try:
        while True:
            pub = Publisher()
            pub.publish()
            time.sleep(86400)
    except KeyboardInterrupt:
        pass




