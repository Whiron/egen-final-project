import pandas as pd
import requests
import json
import datetime
import os
import time
from google.cloud import bigquery
from google.cloud import pubsub_v1

class Publisher:
    def __init__(self):
        self.project_id = "covid-data-project-328321"
        self.topic_id = "covid-raw-data"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "covid-data-project-328321-c37b3c4360fb.json"

    def fetch_data(self,date):

        # yesterdays_date = str(datetime.date.today() - datetime.timedelta(days=1)) + "T00:00:00.000Z"
        yesterdays_date = date+"T00:00:00.000Z"
        start_date = end_date = yesterdays_date
        url = "https://webhooks.mongodb-stitch.com/api/client/v2.0/app/covid-19-qppza/service/REST-API/incoming_webhook/us_only?min_date=" + start_date + "&max_date=" + end_date
        response = requests.get(url)

        string_msg = json.dumps(response.json())
        string_msg = string_msg.encode("utf-8")
        return string_msg

    def publish(self):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(self.project_id, self.topic_id)
        date_list=[]
        last_n_days=18
        start_date = datetime.date.today() - datetime.timedelta(last_n_days)
        for i in range(last_n_days):
            date = start_date + datetime.timedelta(i)
            date_list.append(str(date))


        for date in date_list:
            data=self.fetch_data(date)
            future = publisher.publish(topic_path,data)
            print(future.result())
            time.sleep(30)



if __name__ == "__main__":
    try:
        while True:
            pub = Publisher()
            pub.publish()
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "covid-data-project-328321-c8cad2a05e5f.json"
            bqclient = bigquery.client.Client(project="covid-data-project-328321")
            state_geo = pd.read_csv("state_geo.csv")
            bqclient.load_table_from_dataframe(state_geo, "covid-data-project-328321.covid_data.state_geo_data")
            time.sleep(86400)
    except KeyboardInterrupt:
        pass




