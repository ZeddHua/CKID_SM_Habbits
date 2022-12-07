import pandas as pd
import os

directory = '/project/ll_774_951/SMHabits/avax_project_data/avax_user_tweets/'

files = []
for filename in os.listdir(directory):
    f = os.path.join(directory,filename)
    if os.path.isfile(f):
        files.append(f)

batch_size =50
n = len(files)
cols = ['tweetid', 'userid', 'screen_name', 'date', 'tweet_type', 'text', 'reply_userid', 'favourites_count', 
        'rt_fav_count', 'rt_userid',  'qtd_fav_count', 'qtd_userid', 'friends_count', 'followers_count']  

def frames(batch):
    dfs_lst = []
    for file in batch:
        df = pd.read_csv(file,usecols=cols,lineterminator='\n')
        #print(df)
        dfs_lst.append(df)
    return dfs_lst

j=0
for i in range(0,n,batch_size):
    j+=1
    batch = files[i:i+batch_size]
    dfs_lst = frames(batch)
    #print(dfs_lst)
    dfs = pd.concat(dfs_lst)
    dfs.to_csv('/home1/sliu3825/out'+str(j)+'.csv',index=False)
    print('success'+str(j))

def extract_userid(df):
    lst = list(df.userid.unique())
    return lst

master = []
for file in files:
    df = pd.read_csv(file)
    lst = extract_userid(df)
    master.extend(lst)
