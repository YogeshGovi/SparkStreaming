import tweepy
from kafka import KafkaProducer, KafkaConsumer
import time

#Twitter Credentials

API_KEY="3QVG0jSWVEIsnuCdBlvqgGQ8z"
API_KEY_SECRET="FAlh7RqIUZDJPIxEB59f9S1IjbCI45kNyDrqUfRJkAvEkvIXfj"
Access_Token="1481482494168154112-hT2YZNplvVOXQPbatYm4c5gloT7jKa"
Access_Token_Key="vPo4V5R2k1XUnCu8n08NzXDqOjQdFLHSPgRjnjDCGNAk8"

#Creating authentication object
auth=tweepy.OAuthHandler(API_KEY,API_KEY_SECRET)
#Setting your access token and access secret
auth.set_access_token(Access_Token, Access_Token_Key)
#Creating api object by passing in auth information
api=tweepy.API(auth)

producer=KafkaProducer(bootstrap_servers="master:9092")

from datetime import datetime

def normalize_timestamp(time):
    mytime= datetime.strptime(time[:19], "%Y-%m-%d %H:%M:%S")
    print(mytime)
    print(str(mytime.strftime("%Y-%m-%d %H:%M:%S")))
    print("===================================")
    return(mytime.strftime("%Y-%m-%d %H:%M:%S"))

topic="TwiKaf"

def get_twitter_data():
    res=api.search_tweets("Trisha Krishnan")
    for i in res:
        record=''
        record+=str(i.user.id_str)
        record+=';'
        record+=str(normalize_timestamp(str(i.created_at)))
        record += ';'
        record += str(i.user.followers_count)
        record += ';'
        record += str(i.user.location)
        record += ';'
        record += str(i.favorite_count)
        record += ';'
        record += str(i.retweet_count)
        record += ';'
        producer.send(topic, str.encode(record))

def periodic_work(interval):
    while True:
        get_twitter_data()
        time.sleep(interval)

periodic_work(60*0.1) #get data for every couple of mins




























