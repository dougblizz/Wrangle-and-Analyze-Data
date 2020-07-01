#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: dougblizz
"""

import pandas as pd
import funct 
import os
import re


#Import data
# ================================================================================================================

df_enhanced = pd.read_csv("../data/twitter-archive-enhanced.csv")

url = "https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv"

df_predictions = funct.create_df_images_prediction(url)

if not os.path.isfile('../data/tweet_json.txt'):
    print("file tweet_json.txt not exist, please send keys")
    funct.get_tweets_data(df_enhanced)
else:
    print("file tweet_json.txt exist")
    
#Parse "tweet_json.txt" to dataframe
df_tweets = funct.parse_api_result_to_dataframe()
# ================================================================================================================

    
#Create copies 
df_enhanced_clean = df_enhanced.copy()
df_predictions_clean = df_predictions.copy()
df_tweets_clean = df_tweets.copy()


#Clean df_enhanced copy
# ================================================================================================================

#Remove retweets in df_enhanced_copy
df_enhanced_clean = df_enhanced_clean[df_enhanced_clean['retweeted_status_id'].isnull()]

#Remove replies in df_enhanced_copy
df_enhanced_clean = df_enhanced_clean[df_enhanced_clean['in_reply_to_status_id'].isnull()]

unnecessary_enhanced_columns = ['in_reply_to_status_id',
                                'in_reply_to_user_id',
                                'retweeted_status_id',
                                'retweeted_status_user_id',
                                'retweeted_status_timestamp']
#Drop unnecessary columns
df_enhanced_clean.drop(columns = unnecessary_enhanced_columns, inplace = True)

#See different values in source
df_enhanced_clean['source'].value_counts()

#Use Regex to clear the source column
pattern = ">(.*)</"
df_enhanced_clean['source'] = df_enhanced_clean.apply(lambda x: str(re.findall(pattern, x['source'])).strip("['']"), axis = 1)

#Drop rows with name = None
df_enhanced_clean = df_enhanced_clean[df_enhanced_clean['name'] != 'None']

#See "rating_numerator" values
df_enhanced_clean['rating_numerator'].value_counts()

#See if "rating_denominator" have values equals to 0
df_enhanced_clean['rating_denominator'].value_counts()

#Create new column ratings
df_enhanced_clean['ratings'] = df_enhanced_clean.apply(lambda x: x['rating_numerator']/x['rating_denominator'], axis = 1)                                

#Now we can remove rating_numerator and denominator columns
df_enhanced_clean.drop(columns = ['rating_numerator', 'rating_denominator'], inplace = True)

#Delete names with 2 string or miniors
df_enhanced_clean = df_enhanced_clean[df_enhanced_clean['name'].str.len() > 2]

#Now we need to make one column for dogs stages
df_enhanced_clean['dog_stages'] = df_enhanced_clean.apply(lambda x: funct.make_dog_stages(x), axis = 1)

#Drop (doggo-floofer-pupper-puppo) columns
df_enhanced_clean.drop(columns = ['doggo', 'floofer', 'pupper', 'puppo'], inplace = True)

#Check if column "Name" have invalid characters
df_enhanced_clean['invalid_characters'] = df_enhanced_clean.apply(lambda x: funct.check_invalid_characters(x['name']), axis = 1)       

# the first time I checked with function (check_invalid_characters) I found these values:
# 915       Devón
# 1172     Ralphé
# 1196     Flávio
# 1465    Oliviér
# 1559      Frönq
# 1597     Flávio
# 2164    Oliviér
# 2195     Amélie
# 2217     Gòrdón
# As we can see they are not invalid names so we modify the pattern for invalid names

#Now we can remove "invalid_characters" column
df_enhanced_clean.drop(columns = 'invalid_characters', inplace = True)

#We want to know the days of the week of the tweets, and if they are the weekends for future viewing, and the hours for another graph out of curiosity
#pass to datetime
df_enhanced_clean['timestamp'] = pd.to_datetime(df_enhanced_clean['timestamp'])
#Get day
df_enhanced_clean['day'] = df_enhanced_clean['timestamp'].dt.day_name()
#Get hour (for see)
df_enhanced_clean['hour'] = df_enhanced_clean['timestamp'].dt.hour
#Now we need to know if it's weekday or weekend
df_enhanced_clean['full_week'] = df_enhanced_clean.apply(lambda x: funct.weekend_or_weekday(x), axis = 1)
#Now we can remove "day" column
df_enhanced_clean.drop(columns = 'day', inplace = True)
# ================================================================================================================

#Clean df_predictions copy
# ================================================================================================================

#Info (As we can see this df are clean)
df_predictions_clean.info()

#Duplicates? Not
sum(df_predictions_clean['tweet_id'].duplicated())

#I think we dont need column "img_num"
df_predictions_clean.drop(columns = 'img_num', inplace = True)
# ================================================================================================================


#Clean df_tweets copy
# ================================================================================================================

#We only need some columns so
df_tweets_clean = df_tweets_clean[['id_str', 'favorite_count', 'retweet_count', 'display_text_range', 'lang']]

#Rename columns
df_tweets_clean.rename(columns={'id_str':'tweet_id', 'lang':'language', 'display_text_range':'tweet_length'}, inplace=True)

#Pass tweet_id float to int
df_tweets_clean['tweet_id'] = df_tweets_clean['tweet_id'].astype(int)

#Fix tweet_length column
# add 1 as the tweet count starts from 0
df_tweets_clean['tweet_length'] = df_tweets_clean.apply(lambda x: x['tweet_length'][1] + 1, axis = 1)

#Fix language column
#First see values in column
df_tweets_clean['language'].value_counts()

#result
# en     2314
# und       7
# nl        3
# in        3
# ro        1
# es        1
# eu        1
# tl        1
# et        1
# Name: language, dtype: int64

#Create dict with lang avatibles in twitter
language = {'en':'English', 
            'nl':'Dutch', 
            'ro':'Romanian', 
            'es':'Spanish', 
            'eu':'Undefined', 
            'et':'Undefined', 
            'tl':'Undefined',
            'und':'Undefined',
            'in':'Undefined'}

# replace dict values into column "language"
df_tweets_clean.replace({"language": language}, inplace = True)
# ================================================================================================================

#Merge dataframes and clean final dataframe
# ================================================================================================================

#Create merged df
twitter_archive_master = pd.merge(pd.merge(df_enhanced_clean, df_predictions_clean, on = 'tweet_id'), df_tweets_clean, on = 'tweet_id')

#Create new column for predicted dogs
twitter_archive_master['predicted_dog'] = twitter_archive_master.apply(lambda x: funct.predicted_dog(x), axis = 1)

#Create new column for predicted precision
twitter_archive_master['predicted_precision'] = twitter_archive_master.apply(lambda x: funct.predicted_precision(x), axis = 1)

#New column 
twitter_archive_master['is_dog'] = twitter_archive_master.apply(lambda x: 'it´s a dog' if x['predicted_dog'].find('the closest prediction was:') else 'it´s not a dog', axis = 1)


#Drop prediction columns
prediction_columns = ['p1', 'p1_dog', 'p1_conf', 'p2', 'p2_dog', 'p2_conf', 'p3', 'p3_dog', 'p3_conf']

twitter_archive_master.drop(columns = prediction_columns, inplace = True)
# ================================================================================================================

#Let's do some visualizations
# =============================================================================

#Days when users send more tweets
funct.simple_plot(twitter_archive_master,
                  'full_week',
                  "Days when users send more tweets",
                  'full_week',
                  'days_count')           
            

#Hours when users send more tweets
funct.simple_plot(twitter_archive_master,
                  'hour',
                  "Hours when users send more tweets",
                  'Hours',
                  'Hours_count')     


#How accurate is the prediction
funct.simple_plot(twitter_archive_master,
                  'is_dog',
                  "Prediction accuracy in dogs",
                  'it´s a dog?',
                  'Count')  

#Most used sources
funct.simple_plot(twitter_archive_master,
                  'source',
                  "Most used sources",
                  'Sources',
                  'Count')

#dog stages with more occurrences
funct.simple_plot(twitter_archive_master,
                  'dog_stages',
                  "dog stages with more occurrences",
                  'stages',
                  'Count')

#Top 10 most favorite dogs
funct.top_plot(twitter_archive_master, 'name', 'favorite_count', 'Top 10 most favorite dogs', 10)
            
#Top 10 most retweets dogs
funct.top_plot(twitter_archive_master, 'name', 'retweet_count', 'Top 10 most retweets dogs', 10)

#Top 20 dogs with highest rating
funct.top_plot(twitter_archive_master, 'name', 'ratings', 'Top 20 dogs with highest rating', 20)                
# =============================================================================

#Save csv
# =============================================================================
twitter_archive_master.to_csv('../data/twitter_archive_master.csv', index = False)
# =============================================================================











