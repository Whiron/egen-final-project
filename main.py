import datetime
import os
import pandas as pd
import requests
import json
from google.cloud import bigquery

if __name__ == '__main__':

    # url = "https://webhooks.mongodb-stitch.com/api/client/v2.0/app/covid-19-qppza/service/REST-API/incoming_webhook/us_only?min_date=2021-09-29T00:00:00.000Z&max_date=2021-10-05T00:00:00.000Z"
    # result = requests.get(url)
    # uids=[]
    # response=result.json()
    # df = pd.DataFrame(response)
    # states=list(df['state'].drop_duplicates())
    # result_df=pd.DataFrame(columns=['uid','county','state','combined_name','population','date','confirmed','deaths','confirmed_daily','deaths_daily'])
    # for state in states:
    #     url = "https://webhooks.mongodb-stitch.com/api/client/v2.0/app/covid-19-qppza/service/REST-API/incoming_webhook/us_only?state="+state+"&min_date=2021-09-21T00:00:00.000Z&max_date=2021-10-05T00:00:00.000Z"
    #     result = requests.get(url)
    #     response_df=pd.DataFrame(result.json())
    #     response_df=response_df.reset_index(drop=True)
    #     result_df = pd.concat([result_df, response_df], axis=0)
    #
    # print(result_df)

    # print(df.columns)
    # new_df=df[['uid','county','state','combined_name','population','date','confirmed','deaths','confirmed_daily','deaths_daily']]
    # print(new_df)
    # start_date="2021-10-04T00:00:00.000Z"
    # end_date="2021-10-04T00:00:00.000Z"
    # yesterdays_date=str(datetime.date.today()-datetime.timedelta(days=1))+"T00:00:00.000Z"
    yesterdays_date = "2021-10-07T00:00:00.000Z"
    start_date=end_date=yesterdays_date
    url = "https://webhooks.mongodb-stitch.com/api/client/v2.0/app/covid-19-qppza/service/REST-API/incoming_webhook/us_only?min_date="+start_date+"&max_date="+end_date
    result = requests.get(url)
    # print(pd.DataFrame(result.json())['date'][0])
    response_df = pd.DataFrame(result.json())
    new_df=response_df.groupby(["date","state"],as_index=False).sum()
    new_df=new_df.drop(columns=['uid','country_code','fips'])
    new_df["confirmed_daily"] = pd.to_numeric(new_df["confirmed_daily"])
    new_df["deaths_daily"] = pd.to_numeric(new_df["deaths_daily"])
    new_df["deaths"] = pd.to_numeric(new_df["deaths"])
    new_df["confirmed"] = pd.to_numeric(new_df["confirmed"])
    new_df["population"] = pd.to_numeric(new_df["population"])
    # print(json.dumps(new_df.to_json()))


    # start_date = datetime.date.today() - datetime.timedelta(16)
    # end_date = datetime.date.today() - datetime.timedelta(1)
    # for i in range(16):
    #     date=start_date+datetime.timedelta(i)
    #     date_list.append(str(date))

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="covid-data-project-328321-c8cad2a05e5f.json"
    bqclient = bigquery.client.Client(project="covid-data-project-328321")

    QUERY = (
        'SELECT * FROM `covid-data-project-328321.covid_data.state_geo_data`')
    response = bqclient.query(QUERY)
    geo_data = response.result().to_dataframe().set_index(["state"])
    # WHERE date IN (SELECT DISTINCT date from `covid-data-project-328321.covid_data.state_case_data` order by date DESC limit 13)
    QUERY = (
        'SELECT * FROM `covid-data-project-328321.covid_data.state_case_data`')
    response = bqclient.query(QUERY)
    data = response.result().to_dataframe()
    date_data = list(data['date'].drop_duplicates().sort_values())[-13:]
    data = data[pd.DataFrame(data.date.tolist()).isin(date_data).any(1).values]
    # print(filtered_data)
    data = data.append(new_df)
    population_data=data[['state','population']].drop_duplicates()
    # print(population_data)
    data = data.drop(columns=["date","population","confirmed","deaths"])
    sum_data = data.groupby(["state"],as_index=False).sum()
    sum_data['date'] = new_df['date']
    sum_data = sum_data.set_index('state').join(population_data.set_index('state'))
    # print(sum_data)
    sum_data = sum_data.join(geo_data,lsuffix="_")
    # print(sum_data)
    sum_data["active_idx"] = (sum_data["confirmed_daily"]*1000)/sum_data['population']
    sum_data["density_idx"] = (sum_data["confirmed_daily"]*1000)/sum_data['area']
    sum_data = sum_data.drop_duplicates()
    print(sum_data)
    # print(new_df)

    # list_var=[('Grand Princess', 0), ('Diamond Princess', 0), ('South Carolina', 32007), ('Rhode Island', 1545), ('Oklahoma', 69903), ('Virginia', 42769), ('New Jersey', 8722), ('Wyoming', 97818), ('Ohio', 44828), ('Alaska', 656425), ('Michigan', 96810), ('Nebraska', 77358), ('North Dakota', 70704), ('Georgia', 59441), ('Mississippi', 48434), ('Texas', 268601), ('Massachusetts', 10555), ('Maine', 35387), ('North Carolina', 53821), ('Illinois', 57918), ('South Dakota', 77121), ('District of Columbia', 68), ('District of Columbia', 68), ('Indiana', 36420), ('Missouri', 69709), ('Oregon', 98386), ('American Samoa', 86), ('Arizona', 114006), ('Montana', 147046), ('Kansas', 82282), ('Idaho', 83574), ('Maryland', 12407), ('California', 163707), ('Louisiana', 51843), ('Washington', 71303), ('New Hampshire', 9351), ('Vermont', 9615), ('Minnesota', 86943), ('Delaware', 1954), ('Tennessee', 42146), ('Colorado', 104100), ('West Virginia', 24231), ('Connecticut', 5544), ('Utah', 84904), ('Northern Mariana Islands  ', 179), ('Hawaii', 10932), ('Puerto Rico', 3515), ('Arkansas', 53182), ('Alabama', 52423), ('New York', 54475), ('Guam', 210), ('Iowa', 56276), ('Kentucky', 40411), ('Florida', 65758), ('Wisconsin', 65503), ('Nevada', 110567), ('Pennsylvania', 46058), ('New Mexico', 121593)]
    # # df=pd.read_csv(list_var)
    # df=pd.DataFrame(list_var,columns=['state','area'])
    # print(df)

    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "covid-data-project-328321-c8cad2a05e5f.json"
    # bqclient = bigquery.client.Client(project="covid-data-project-328321")
    # state_geo = pd.read_csv("state_geo.csv")
    # # bqclient.create_table("state_geo_data")
    # bqclient.load_table_from_dataframe(state_geo, "covid-data-project-328321.covid_data.state_geo_data")

