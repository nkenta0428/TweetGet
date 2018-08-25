
# coding: utf-8
#Twitterから特定ユーザーのツイートを取得して
#pandas形式で記録、取得した日時のファイルで保存する
#2018.01.29 複数ユーザーのデータを取得できるように設定
#2018.08.25 認証情報は環境変数から取得する

# # ライブラリインポート

# In[1]:

import os 
import tweepy
import pandas as pd
from datetime import datetime
import pytz


# In[2]:

#認証情報

CONSUMER_KEY = os.environ["TWT_CONSUMER_KEY"]
CONSUMER_SECRET = os.environ["TWT_CONSUMER_SECRET"]
ACCESS_TOKEN = os.environ["TWT_ACCESS_TOKEN"]
ACCESS_TOKEN_SECRET = os.environ["TWT_ACCESS_TOKEN_SECRET"]


# In[3]:

#認証情報セット
auth=tweepy.OAuthHandler(CONSUMER_KEY,CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN,ACCESS_TOKEN_SECRET)

api = tweepy.API(auth)
#public_tweets = api.home_timeline()


# In[4]:

#変数定義
# 外部のテキストファイルから読み込みたい
users=["muratamika2020","f_n_s_3807_8892"]
# outdir = "/Users/nkenta/Desktop"
# 外部のテキストファイルから読み込みたい
outdir = "/media/pi/C074-8A40/tweetdata"


# # 関数定義
# ## データフレームリセット

# In[5]:

#データフレームをリセットする
def df_reset():
    global df
    df = [[]]
    df = pd.DataFrame(columns=["tweetid",
                               "userid",
                               "screen_name",
                               "date",
                               "retweetedflg",
                               "retweet_count",
                               "truncatedflg",
                               "text",
                               "rt_id"])
    return df


# ## 前回取得した最新のIDを取得する

# In[6]:

## 前回取得した最新のIDを取得する
## tmpdf がnullなら取得しない
def get_existID(tmpdf):
    if len(tmpdf.index) != 0:
        maxid = max(tmpdf.tweetid)
    else:
        maxid = 1
    return maxid


# ## タイムラインのデータを取得する

# In[7]:

def collectTimeline(tmpdf, tmpusernm, tmpsince):
    for twt in tweepy.Cursor(api.user_timeline, screen_name=tmpusernm, since_id=tmpsince).items():
        if not twt.retweeted and not twt.text.startswith('RT @') :
            retweeted = 0
            rtid = 0
        else:
            retweeted = 1
            rtid = twt.retweeted_status.id_str
    
        row = pd.Series([twt.id_str,
                     twt.user.id_str,
                     twt.user.screen_name, 
                     twt.created_at,
                     retweeted,
                     twt.retweet_count,
                     str(twt.truncated),
                     twt.text,
                     rtid],
                    index=tmpdf.columns)
        tmpdf = tmpdf.append(row,ignore_index = True)
    return tmpdf


# ## ファイル出力関数

# In[13]:

#接頭辞として日時とユーザー名
def createCsvFile(tmpdf,tmpdir,uname):
    outdir=tmpdir
    tmpdate = datetime.now().strftime("%Y%m%d")
    filenm=tmpdate+"_"+str(uname)+".csv"
    tmpdf.to_csv(tmpdir+"/"+filenm, header=True, sep=",")


# # メイン処理

# In[14]:

#対象ユーザーリストをループさせる
for u in users:

    #処理
    df = df_reset()

    #タイムラインデータ取得
    #5くらいループしたら全部取れると想定
    for i in range(5):
        #前回の最後のidから取得する
        lastid = get_existID(df)
        print("loop: " + str(i) + " maxid: " + str(lastid))
        df = collectTimeline(df, u, lastid)

    df.date = pd.DatetimeIndex(df.date,format="%Y/%m/%d %H:%m:%s").tz_localize("GMT").tz_convert('Asia/Tokyo')
    #df.data=df.date = pd.DatetimeIndex(df.date,format="%Y/%m/%d %H:%m:%s").tz_convert('Asia/Tokyo').tz_localize(None) 
    createCsvFile(df,outdir,u)

