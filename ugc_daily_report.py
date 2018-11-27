import pandas as pd
import numpy as np
import urllib.request
import json
import time
from datetime import datetime
import sqlite3
import pickle

report_folder = "/home/psicktrick/PycharmProjects/Automated_Reporting/venv" + "/src/Reports/Daily ugc/"
db = report_folder + "production.db"
tags = report_folder + 'tag_names.csv'
user_names = report_folder + 'select_id_first_name_last_name_from_user.csv'

start_date = '2018-11-26'  # YYYY-MM-DD
end_date = '2018-11-27'

# Uninstalled users
url2 = 'https://api.womaniya.co/analytics/api/rs/v1/service/get/checkuninstall'
with urllib.request.urlopen(url2) as url:
    s2 = url.read()
data2 = json.loads(s2.decode('utf-8'))
df = pd.DataFrame(data2['data']['list'])['id']
un_users = list(df)

# seeded accounts
cnx = sqlite3.connect(db)
tf = pd.read_sql_query("SELECT * FROM seed", cnx)
bots = list(set((tf['user_id']))) + [78] + [20849] + [1]

# post analytics
url1 = 'https://api.womaniya.co/analytics/api/rs/v1/service/get/userpost/analytics'
with urllib.request.urlopen(url1) as url:
    s1 = url.read()
data1 = json.loads(s1.decode('utf-8'))

df = pd.DataFrame(data1['data']['list']).set_index('videoId')

df['publishDate'] = df['publishDate'] / 1000
df['publishDate'] = pd.to_datetime(df['publishDate'], unit='s')
df['tags'] = df['tags'].apply(lambda x: json.loads(x))

uf = df[~(df['userId'].isin(bots))]
vf = uf.reset_index().groupby('userId')['videoId'].count().reset_index()

# Top creators till date
print('\n')
print("Top creators till date")
uf = uf.reset_index()[['userId', 'videoId']]
tc = uf.groupby('userId')['videoId'].count().reset_index().sort_values('videoId', ascending=False)
tc.rename(columns={'videoId': 'No. of posts'}, inplace=True)
tc.set_index('userId', inplace=True)
all_users = pd.read_csv(user_names, header=None)
all_users.set_index(0, inplace=True)
tc = tc.merge(all_users, left_index=True, right_index=True, how='left')
tc = tc.iloc[1:, :]
tc.rename(columns={1: 'first name', 2: 'last name'}, inplace=True)
tc = tc[['No. of posts', 'first name', 'last name']]
# tc.sort_values("No. of posts", ascending=False, inplace = True)
print(tc.head(10))
# tc.to_csv(report_folder + '/top_creators' + start_date + '.csv')
print('\n\n')

# Post wise users till date'
print("\n")
print("Post wise no. of users till date:")
vf = vf['videoId'].value_counts().reset_index().sort_values('index').rename(
    columns={'index': 'No. of posts', 'videoId': 'No. of users'})
print(vf.head())
print("\n")
bins = [5, 10, 1000]
vf["No. of posts range"] = pd.cut(vf['No. of posts'], bins)
vf = vf.groupby('No. of posts range')['No. of users'].sum()
print(vf)
print('\n\n')

df = df[(df['publishDate'] >= start_date) & (df['publishDate'] <= end_date)]

print("Total no. of posts: " + str(len(df)))

uf = df[~(df['userId'].isin(bots))]
sf = df[(df['userId'].isin(bots))]

print("\n\n")
print("Total no. of user posts: " + str(len(uf)))
print("Total no. of seeded posts: " + str(len(sf)))
print("\n\n")

# No of users posting for the first time

d = pd.DataFrame(data1['data']['list'])
d['publishDate'] = d['publishDate'] / 1000
d['publishDate'] = pd.to_datetime(d['publishDate'], unit='s')
d = d[(d['publishDate'] >= '2018-10-28') & (d['publishDate'] <= start_date)]

u = d[~(d['userId'].isin(bots))]
s = d[(d['userId'].isin(bots))]

old_users = list(set(u['userId']))

u_first_time = uf[~(uf['userId'].isin(old_users))]
print("No. of users posting for the first time are: " + str(len(list(set((u_first_time['userId']))))))
print("\n\n")

tf = df.copy()
tf = tf.set_index(['commentCount', 'likeCount', 'publishDate', 'shareCount', 'userId', 'viewCount'])['tags'].apply(
    pd.Series).stack()
tf = tf.reset_index()
tf.columns = ['commentCount', 'likeCount', 'publishDate', 'shareCount', 'userId', 'viewCount', 'tags', 'tag']
tf.drop('tags', axis=1, inplace=True)

# tag wise no of posts
tf = tf[~(tf['userId'].isin(bots))]
twp = tf['tag'].value_counts().reset_index()
twp['index'] = twp['index'].apply(lambda x: int(x))
twp.rename(columns={'index': 'tag_id', 'tag': 'no. of posts'}, inplace=True)
twp.set_index('tag_id', inplace=True)

tag_names = pd.read_csv(report_folder + 'tag_names.csv')
tag_names.set_index('tag_id', inplace=True)

tag_wise_analysis = tag_names.merge(twp, left_index=True, right_index=True,
                                    how='left')  # .rename(columns={'index': 'tag_id'})

# tag wise performance
tg = tf.copy()
tg['tag'] = tg['tag'].apply(lambda x: int(x))
tg = tg[['tag', 'viewCount', 'likeCount', 'shareCount', 'commentCount']]
tg.set_index('tag', inplace=True)
tag_names = pd.read_csv(tags)
tag_names.set_index('tag_id', inplace=True)
t = tag_names.merge(tg, left_index=True, right_index=True, how='left').reset_index().rename(
    columns={'index': 'tag_id'})
t = t.groupby(['tag_id', 'tag_name']).sum()
t['Engagement'] = t['viewCount'] + 5 * t['likeCount'] + 10 * t['shareCount'] + 10 * t['commentCount']
t = t.sort_values('tag_id')
t = t.reset_index()[['tag_name', 'tag_id', 'viewCount', 'likeCount', 'shareCount', 'commentCount', 'Engagement']]
t.set_index("tag_id", inplace=True)

tag_wise_analysis = tag_wise_analysis.merge(t, left_index=True, right_index=True, how='left')
tag_wise_analysis.drop('tag_name_y', axis=1, inplace=True)
tag_wise_analysis.fillna(0, inplace=True)

print(tag_wise_analysis.head(10))
print("\n\n")
tag_wise_analysis.to_csv(report_folder + 'tag_performance_' + start_date + '.csv')

# Post wise no. of users
vf = uf.reset_index().groupby('userId')['videoId'].count().reset_index()
vf = vf.sort_values('videoId', ascending=False)
x = vf['videoId'].value_counts().reset_index().sort_values('index').rename(
    columns={'index': 'No. of posts', 'videoId': 'No. of people'}).set_index('No. of posts')
print(x)
print("\n\n")

# Top creators for the day

uf = uf.reset_index()[['userId', 'videoId']]
tc = uf.groupby('userId')['videoId'].count().reset_index().sort_values('videoId', ascending=False)
tc.rename(columns={'videoId': 'No. of posts'}, inplace=True)
tc.set_index('userId', inplace=True)
all_users = pd.read_csv(user_names, header=None)
all_users.set_index(0, inplace=True)
tc = tc.merge(all_users, left_index=True, right_index=True, how='left')
tc = tc.iloc[1:, :]
tc.rename(columns={1: 'first name', 2: 'last name'}, inplace=True)
tc = tc[['No. of posts', 'first name', 'last name']]
print(tc.head(10))
tc.to_csv(report_folder + '/top_creators' + start_date + '.csv')
print('\n\n')

# User wise followers
url3 = 'https://api.womaniya.co/analytics/api/rs/v1/service/get/userwise/followers'
with urllib.request.urlopen(url3) as url:
    s3 = url.read()
data3 = json.loads(s3.decode('utf-8'))

df = pd.DataFrame(data3['data']['list']).sort_values('followers', ascending=False)

uf = df[~(df['userId'].isin(bots))]
sf = df[(df['userId'].isin(bots))]

uf = uf[~(uf['userId'].isin(un_users))]  # removing uninstalled users

avg_followers = sum(uf['followers']) / len(uf)

print("Average followers for a user: " + str(avg_followers))
print("\n\n")

# count of followers
folr_count = uf['followers'].value_counts().reset_index().rename(
    columns={'index': 'No. of followers', 'followers': 'No. of users'}).sort_values('No. of followers')
print(folr_count.iloc[0, :])
print("\n\n")

folr_count2 = folr_count.copy()
bins = [0, 10, 20, 50, 100, 500, 1000, 2000]
folr_count2['followers'] = pd.cut(folr_count2['No. of followers'], bins)
folr_count2 = folr_count2.groupby('followers')['No. of users'].sum()
print(folr_count2)
print("\n\n")

# count of following
url4 = 'https://api.womaniya.co/analytics/api/rs/v1/service/get/userwise/following'
with urllib.request.urlopen(url4) as url:
    s4 = url.read()
data4 = json.loads(s4.decode('utf-8'))

df = pd.DataFrame(data4['data']['list'])

uf = df[~(df['userId'].isin(bots))]
sf = df[(df['userId'].isin(bots))]

uf = uf[~(uf['userId'].isin(un_users))]

avg_following = sum(uf['following']) / len(uf)

print("Average no. of accounts a user follows: " + str(avg_following))
print("\n\n")

ex_avg_following = sum(uf[uf['following'] > 1].set_index('userId')['following']) / len(uf[uf['following'] > 1])

print("Average no. of accounts a user follows other than default: " + str(ex_avg_following))
print("\n\n")
