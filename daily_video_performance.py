import pandas as pd
import numpy as np
import urllib.request
import json
import time
import ast
from datetime import datetime

data_dir = "/home/psicktrick/PycharmProjects/Automated_Reporting/venv/src/" + "Reports/Daily video performance/"

start_date = "24-11-2018" #DD-MM-YYYY
end_date = "27-11-2018"

print("Views api is loading")
urll = 'https://api.womaniya.co/analytics/api/rs/v1/service/get/videoperformance/VIEW'
with urllib.request.urlopen(urll) as url:
    s1 = url.read()
print("\n")

print("Likes api is loading")
urll = 'https://api.womaniya.co/analytics/api/rs/v1/service/get/videoperformance/LIKE'
with urllib.request.urlopen(urll) as url:
    s2 = url.read()
print("\n")

print("Shares api is loading")
urll = 'https://api.womaniya.co/analytics/api/rs/v1/service/get/videoperformance/SHARE'
with urllib.request.urlopen(urll) as url:
    s3 = url.read()
print("\n")

print("Downloads api is loading")
urll = 'https://api.womaniya.co/analytics/api/rs/v1/service/get/videoperformance/DOWNLOAD'
with urllib.request.urlopen(urll) as url:
    s5 = url.read()
print("\n")

print("Watch later api is loading")
urll = 'https://api.womaniya.co/analytics/api/rs/v1/service/get/videoperformance/WATCH_LATER'
with urllib.request.urlopen(urll) as url:
    s6 = url.read()
print("\n")

dataView = json.loads(s1.decode('utf-8'))
dataLike = json.loads(s2.decode('utf-8'))
dataShare = json.loads(s3.decode('utf-8'))
dataDownload = json.loads(s5.decode('utf-8'))
dataWatch_later = json.loads(s6.decode('utf-8'))

data_list = [dataLike, dataShare, dataDownload, dataWatch_later]

weekly_data2 = ['weeklyVideoLike', 'weeklyVideoShare', 'weeklyVideoDownload', 'weeklyVideoWatchLater']

weekly_data3 = ['weeklyVideoView','weeklyVideoLike','weeklyVideoShare','weeklyVideoDownload','weeklyVideoWatchLater']

data = dataView

for (i,j) in zip(weekly_data2,data_list):
    data['data'][i] = j['data'][i]

df = pd.DataFrame()
for i in weekly_data3:
    if  df.empty:
        df = pd.DataFrame(data['data'][i])
        df = df[['videoId','publishDate','title','channelName','tags','count']]
        df.rename(columns={'count':i},inplace=True)
        df.set_index('videoId', inplace=True)
        df.sort_values(i, ascending=False, inplace=True)
    else:
        nf = pd.DataFrame(data['data'][i])
        nf = nf[['videoId','title','channelName','tags','count']]
        nf.rename(columns={'count':i},inplace=True)
        nf.set_index('videoId', inplace=True)
        nf.drop(['title','channelName','tags'],axis=1,inplace=True)
        nf.sort_values(i, ascending=False, inplace=True)
        df = df.merge(nf, left_index=True, right_index=True, how='outer')
df.fillna(0,inplace=True)
df.rename(columns = {'weeklyVideoView':'Views','weeklyVideoLike':'Likes','weeklyVideoShare':'Shares','weeklyVideoComment':'Comments','weeklyVideoDownload':'Downloads','weeklyVideoWatchLater':'Watch Later' }, inplace=True)
df['Engagement'] = (df['Views'] + 5*df['Likes'] + 10*df['Shares']  + 10*df['Downloads'] + 10*df['Watch Later'])
df['publishDate'] = df['publishDate']/1000
df['publishDate'] = pd.to_datetime(df['publishDate'],unit='s')
df.sort_values('Views', ascending = False, inplace=True)

df = df[(df['publishDate'] >= start_date) & (df['publishDate'] <= end_date)]

df['format'] = df.apply(lambda _: '', axis=1)

for i in range(len(df)):
    if df.iloc[i,2] == 'वुमनिया':
        df.iloc[i,10] = 'image'
    else:
        df.iloc[i,10] = 'video'

print(df.head())

df.to_csv(data_dir + start_date  + '.csv')

print("check folder")