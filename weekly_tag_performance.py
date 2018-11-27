import pandas as pd
import numpy as np
import urllib.request
import json
import time
import ast
from datetime import datetime

data_dir = "/home/psicktrick/PycharmProjects/Automated_Reporting/venv" + "/src/Reports/Weekly Tag performance/"
login_users_file = data_dir + 'select_interest_tags_from_login_activity.csv'
nonlogin_users_file = data_dir + 'select_interest_tags_from_non_login_user.csv'
master_tags = [1, 2, 8, 4, 3, 9, 6, 7, 10, 5, 148, 161, 147, 160, 159, 173, 174]

start_date = "21-11-2018"  # DD-MM-YYYY
end_date = "27-11-2018"

print("Views api is loading")
urll = 'https://api.womaniya.co/analytics/api/rs/v1/service/get/videoperformance/VIEW'
with urllib.request.urlopen(urll) as url:
    s = url.read()
print("\n")

data = json.loads(s.decode('utf-8'))
df = pd.DataFrame(data['data']['dailyVideoView'])
df.rename(columns={'count': 'views'}, inplace=True)
df = df[['publishDate', 'videoId', 'tags', 'views']]
df['publishDate'] = df['publishDate'] / 1000
df['publishDate'] = pd.to_datetime(df['publishDate'], unit='s')
df = df[(df['publishDate'] >= start_date) & (df['publishDate'] < end_date)]

lst_col = 'tags'
df = pd.DataFrame({
    col: np.repeat(df[col].values, df[lst_col].str.len())
    for col in df.columns.difference([lst_col])
}).assign(**{lst_col: np.concatenate(df[lst_col].values)})[df.columns.tolist()]

df['tags'] = df['tags'].apply(lambda x: x if x in master_tags else np.nan)
df.dropna(inplace=True)
df['tags'] = df['tags'].apply(lambda x: int(x))
df = df.groupby('tags')['views'].agg({'Views': 'sum', 'No. of videos': 'count'})

l = pd.read_csv(login_users_file, header=None)
l.reset_index(inplace=True)
l.rename(columns={0: 'tags'}, inplace=True)
l.dropna(inplace=True)
l['tags'] = l['tags'].apply(lambda x: list(ast.literal_eval(x)))
lst_col = 'tags'
l = pd.DataFrame({
    col: np.repeat(l[col].values, l[lst_col].str.len())
    for col in l.columns.difference([lst_col])
}).assign(**{lst_col: np.concatenate(l[lst_col].values)})[l.columns.tolist()]
l = l['tags'].value_counts()

n = pd.read_csv(nonlogin_users_file, header=None)
n.reset_index(inplace=True)
n.rename(columns={0: 'tags'}, inplace=True)
n.dropna(inplace=True)
n['tags'] = n['tags'].apply(lambda x: list(ast.literal_eval(x)))
lst_col = 'tags'
n = pd.DataFrame({
    col: np.repeat(n[col].values, n[lst_col].str.len())
    for col in n.columns.difference([lst_col])
}).assign(**{lst_col: np.concatenate(n[lst_col].values)})[n.columns.tolist()]
n = n['tags'].value_counts()

df2 = pd.DataFrame(dict(l=l, n=n))
df2['no. of users'] = df2['l'] + df2['n']

df = df.merge(df2, left_index=True, right_index=True, how='inner')
df['Views per video'] = df['Views'] / df['No. of videos']
df["Views per video per user"] = df['Views per video'] / df['no. of users']
df = df.reset_index().rename(columns={'index': 'tags'})
df = df[['tags', 'Views', 'No. of videos', 'no. of users', 'Views per video', 'Views per video per user']]

df.to_csv(data_dir + start_date + "_To_" + end_date + ".csv")
