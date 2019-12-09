import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs


def getCoordinate(url):
    r = requests.get(url)
    soup = bs(r.content, 'lxml')
    title = soup.find("meta",  property="og:title")
    return title["content"]

def unshorten_url(url):
    return requests.head(url, allow_redirects=True).url
        
    
filename = 'sfEbot_tweets.csv'
df = pd.read_csv(filename)
df['magnitude'] = df['text'].str[4:7]
df['distance'] = df['text'].str.split('occurred ',1).str[1].str.split(' of ').str[0].str.replace('"', "")
df['location'] = df['text'].str.split(' of ',1).str[1].str.split(', C').str[0].str.replace('"', "")
df['details'] = df['text'].str.split('Details:',1).str[1].str.split('Map:').str[0]

df['country'] = 'United States'
df['state'] = 'California'
df['map'] = df['text'].str.split('Map:',1).str[1].str.slice(0,-1)
df['map2'] = df['map'].apply(unshorten_url)
df['map3'] = df['map2'].str.split('&ll=',1).str[1].str.split('&spn=').str[0]
df[['latitude','longtitude']] = df.map3.str.split(",",expand=True,)

df2 = df[['created_at','magnitude','distance','location','state','country','details','map','latitude','longtitude']]
df2.to_csv('sf_Earthquakes.csv')




