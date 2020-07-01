#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: dougblizz
"""

import requests
import tweepy
import json
import pandas as pd
from io import StringIO
from timeit import default_timer as timer
import re
import matplotlib.pyplot as plt
import seaborn as sns

plt.style.use('ggplot')

def create_df_images_prediction(url):
    response = requests.get(url)
    result = str(response.content, 'utf-8')
    data = StringIO(result)
    return pd.read_csv(data, sep='\t')

def get_tweets_data(df):
    try:
        consumer_key = input("set consumerkey")
        consumer_secret = input("set consumer secret")
        access_token = input("set access token")
        access_secret = input("set access secret")
        
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)
        
        api = tweepy.API(auth, wait_on_rate_limit=True)
        
        tweet_ids = df.tweet_id.values
        len(tweet_ids)
        
        #Query Twitter's API for JSON data for each tweet ID in the Twitter archive
        count = 0
        fails_dict = {}
        start = timer()
        # Save each tweet's returned JSON as a new line in a .txt file
        with open('../data/tweet_json.txt', 'w') as outfile:
            # This loop will likely take 20-30 minutes to run because of Twitter's rate limit
            for tweet_id in tweet_ids:
                count += 1
                print(str(count) + ": " + str(tweet_id))
                try:
                    tweet = api.get_status(tweet_id, tweet_mode='extended')
                    print("Success")
                    json.dump(tweet._json, outfile)
                    outfile.write('\n')
                except tweepy.TweepError as e:
                    print("Fail")
                    fails_dict[tweet_id] = e
                    pass
                if count == 3:
                    break
        end = timer()
        print(end - start)
        print(fails_dict)
    except Exception as e:
        print(e)
        
def parse_api_result_to_dataframe():
    lines = []
    df = pd.DataFrame()
    keys_to_remove = ['user', 'entities', 'extended_entities',]
    with open('../data/tweet_json.txt') as json_file:
        lines = json_file.readlines()
        json_file.close()
        
    for line in lines:
        data = json.loads(line)
        for keys in keys_to_remove:
            data.pop(keys, None)
        df = df.append(data, ignore_index = True)
    return df

def make_dog_stages(df):

    if(df['doggo'] != 'None'):
        return df['doggo']
    elif(df['floofer'] != 'None'):
        return df['floofer']
    elif(df['pupper'] != 'None'):
        return df['pupper']
    elif(df['puppo'] != 'None'):
        return df['puppo']
    else:
        return None
    
def check_invalid_characters(strg, search=re.compile(r'[^a-z0-9A-Zäëïöüáéíóúàèìòù]').search):
     return bool(search(strg))
    
def weekend_or_weekday(df):
    if(df['day'] == 'Sunday' or df['day'] == 'Saturday'):
        return 'weekend'
    else:
        return 'weekday'
    
def predicted_dog(df):
    if(df['p1_dog'] == True):
        return df['p1']
    elif(df['p2_dog'] == True):
        return df['p2']
    elif(df['p3_dog'] == True):
        return df['p3']
    else:
        return f'the closest prediction was: {df["p1"]}' 
    
def predicted_precision(df):
    if(df['p1_dog'] == True):
        return df['p1_conf']
    elif(df['p2_dog'] == True):
        return df['p2_conf']
    elif(df['p3_dog'] == True):
        return df['p3_conf']
    else:
        return df['p1_conf']
    
def simple_plot(df, column, tittle, x, y):
    plt.subplots(figsize = (12,8)) 
    df[column].value_counts().plot(kind = 'bar')\
                .set_title(tittle, fontsize = 14)
                
    plt.xlabel(x, fontsize=10)
    plt.ylabel(y, fontsize=10)  
    plt.xticks(rotation = 360)
    
def top_plot(df, x, y, tittle, top):
    plt.subplots(figsize = (16,12))
    sns.barplot(x=x, y=y, data = df.sort_values(by=[y], ascending = False).head(top),  palette="Purples_d")\
                .set_title(tittle, fontsize = 20)



        
    