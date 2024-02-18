import json

# http://api.weatherapi.com/v1/current.json?key=&q=India&aqi=no
from datetime import datetime
import requests  
import boto3
import pandas as pd
import awswrangler as wr
from pytz import timezone


AWS_ACCESS_KEY='<Your Access Key>'
AWS_SECRET_KEY='<Your Secret Key>'
AWS_REGION='<Your AWS Region>'
S3_BUCKET_NAME='<Your S3 bucket name>'


# Create an S3 client
s3_client = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

def get_weather_data(city):  
    api_url = "http://api.weatherapi.com/v1/current.json"
    params = {  
        "q": city,    
        "key": "<Your API Key>"
    }  
    response = requests.get(api_url, params=params)  
    data = response.json()  
    return data  
    
    

def lambda_handler(event, context):
    df=pd.DataFrame()
    
    cities = ["Bangalore","Delhi","Mumbai","Chennai","Kolkata","Hyderabad","Pune","Solapur"]
    for city in cities:
        data = get_weather_data(city)  
        #print(data)
        temp = data['current']['temp_c']
        wind_speed = data['current']['wind_mph']
        wind_dir = data['current']['wind_dir']
        pressure_mb = data['current']['pressure_mb']
        humidity = data['current']['humidity']

        #print(city,temp,wind_speed,wind_dir,pressure_mb,humidity)
        current_timestamp = datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M")

        item = {
                    'city': city,
                    'time': str(current_timestamp),
                    'temprature': temp,
                    'wind_speed': wind_speed,
                    'wind_direction': wind_dir,
                    'pressure_mb': pressure_mb,
                    'humidity': humidity
                }
        tempdf = pd.DataFrame.from_dict([item])
        df = pd.concat([df,tempdf], ignore_index=True)
        
    wr.s3.to_csv(df,"s3://<bucket_name>/weather_data.csv",dataset=True, mode='append')

    

 