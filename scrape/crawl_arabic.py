import pandas as pd
import re

def forsana_jobs():

    _RE = re.compile(r"[\u200e8\xa0\n\s]+")

    df = pd.read_json('forsana.jl',lines=True)
    df['source'] = 'forsana'
    df['job-id'] = df.url.apply(lambda x: x.split('-')[-1])
    return df

def bayt_jobs():
    _RE = re.compile(r"[\u200e8\xa0\n\s]+")
    columns = ['Job Description', 'Job Details','title', 'href', 'job-id']

    read = pd.read_json('jobs.jl',lines=True,chunksize=1000)
    df = []
    for chunk in read:
        if len(df)==0:
            df = chunk[columns]
        else:
            df = pd.concat([df,chunk[columns]],ignore_index=True,axis=0)
    df['source'] = 'bayt'
    df = df.rename(columns={'Job Description': 'description'})
    return df

def all_jobs():
    df1 = bayt_jobs()
    df2 = forsana_jobs()
    columns = ['title','description','source','job-id']
    df = pd.concat([df1[columns],df2[columns]],ignore_index=True,axis=0)
    df['description'] = df.description.apply(lambda x: _RE.sub(" ", x).strip() if isinstance(x,str) else '')
    df['title'] = df.title.apply(lambda x: _RE.sub(" ", x).strip())
    df['ar-chars'] = df['title'].apply(lambda x: 0x0600<=ord(x[0]) and ord(x[0])<= 0x06ff)
    return df


df = all_jobs()
df.loc[df['ar-chars']].to_csv('arabic.csv')
