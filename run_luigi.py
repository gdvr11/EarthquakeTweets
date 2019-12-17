#!/usr/bin/env python
#!/usr/bin/python -tt
# encoding: utf-8

import luigi
import tweepy
import csv
import tweepy
import datetime
import sys
import numpy as np
import pandas as pd
import requests


class extractTweets(luigi.Task):
    username = luigi.Parameter()
    # Twitter API credentials
    consumer_key = ""
    consumer_secret = ""
    access_key = ""
    access_secret = ""

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget('{}_2019tweets.csv'.format(self.username))

    def run(self):
        #authorize twitter, initialize tweepy
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_key, self.access_secret)
        api = tweepy.API(auth)

        startDate = datetime.datetime(2018, 12, 31, 23, 59, 59)
        
        #initialize a list to hold all the tweepy Tweets
        alltweets = []
        #make initial request for most recent tweets (200 is the maximum allowed count)
        new_tweets = api.user_timeline(screen_name = self.username,count=200)
        
        #save most recent tweets
        alltweets.extend(new_tweets)
        
        #save the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1
        
        #keep grabbing tweets until there are no tweets left to grab
        while len(new_tweets) > 0:
            print("getting tweets before {}".format(oldest))
            
            #all subsiquent requests use the max_id param to prevent duplicates
            new_tweets = api.user_timeline(screen_name = self.username,count=200,max_id=oldest)
            
            #save most recent tweets
            alltweets.extend(new_tweets)
            
            #update the id of the oldest tweet less one
            oldest = alltweets[-1].id - 1
            
            print("...{} tweets downloaded so far".format(len(alltweets)))
        
        tweets = []
        for tweet in alltweets:
            if tweet.created_at > startDate:
                tweets.append(tweet)

        # pass to 2d array
        outtweets = [[tweet.id_str, tweet.created_at, tweet.text.encode("utf-8")] for tweet in tweets]
        
        # write the csv	
        with self.output().open('w') as f:
            writer = csv.writer(f)
            writer.writerow(["id","created_at","text"])
            writer.writerows(outtweets)
            print('{}_2019tweets.csv was successfully created.'.format(self.username))
        pass

class TransformTweets(luigi.Task):
    username = luigi.Parameter(default="earthquakesSF")

    def requires(self):
        return[extractTweets(username=self.username)]

    def output(self):
        return luigi.LocalTarget('sf_2019Earthquakes.csv')
    
    def unshorten_url(self,url):
        return requests.head(url, allow_redirects=True).url
        
    def run(self):
        filename = 'earthquakesSF_2019tweets.csv'
        df = pd.read_csv(filename)
        df['magnitude'] = df['text'].str[4:7]
        df['distance'] = df['text'].str.split('occurred ',1).str[1].str.split(' of ').str[0].str.replace('"', "")
        df['location'] = df['text'].str.split(' of ',1).str[1].str.split(', C').str[0].str.replace('"', "")
        df['details'] = df['text'].str.split('Details:',1).str[1].str.split('Map:').str[0]

        df['country'] = 'United States'
        df['state'] = 'California'
        df['map'] = df['text'].str.split('Map:',1).str[1].str.slice(0,-1)
        df['map2'] = df['map'].apply(self.unshorten_url)
        df['map3'] = df['map2'].str.split('&ll=',1).str[1].str.split('&spn=').str[0]
        df[['latitude','longtitude']] = df.map3.str.split(",",expand=True,)

        df2 = df[['created_at','magnitude','distance','location','state','country','details','map','latitude','longtitude']]
        
        with self.output().open('w') as f:
            df2.to_csv(f)

if __name__ == '__main__':
	luigi.run(["--local-scheduler"], main_task_cls=TransformTweets)