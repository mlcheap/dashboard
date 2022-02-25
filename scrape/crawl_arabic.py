import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
import tqdm
import sqlite3


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

def crawl_fu1sa():
    try:
        df = pd.read_sql("SELECT * FROM fu1sa",con=con)
    except:
        df = pd.DataFrame(columns=['post','url', 'head_raw', 'body_raw'])
    posts = set(range(10273))-set(df.loc[df.head_raw!="None"].post.astype(int).unique())
    print(f'crawling {len(posts)} posts')
    for post in tqdm.tqdm(posts,total=len(posts)):
        url = f'https://www.fu1sa.com/posts/{post}'
        response = requests.get(url,headers=headers)
        if response.status_code!=200:
            print(f'post {post} returned {response.status_code}')
        soup = BeautifulSoup(response.text, 'html.parser')
        head = soup.select_one('.head_blogDetails')
        body = soup.select_one('.post_blog_details')
        df.loc[len(df),:] = (post, url, str(head), str(body))
    df = df.set_index('post')
    df.to_sql("fu1sa",if_exists="append",con=con)
    
def extract_info(post):
    head = post.head_raw
    soup = BeautifulSoup(head, 'html.parser')
    meta = soup.select('.info_news > li')
    title = clean(soup.find('h2').find(text=True))

    body = post.body_raw
    soup = BeautifulSoup(body, 'html.parser')
    data = [soup.select_one('div > div').text]
    data.extend([d.text for d in soup.select('.post_blog_responsibilities')])
    description = clean('\n'.join(data))
    meta = {
        'post': post.Index,
        'title': title,
        'views': meta[0].text, 
        'date': meta[1].text, 
        'place': meta[2].text,
        'description': description
    }
    return meta

def proc_fu1sa(con):
    df =  pd.read_sql("SELECT * FROM fu1sa",con=con).set_index('post')
    proc = [extract_info(post) 
               for post in tqdm.tqdm(df.itertuples(index=True),total=len(df))]
    proc = pd.DataFrame(proc).set_index('post')
    proc.to_sql("fu1sa_proc",if_exists="replace",con=con)

def clean(text):
    text = re.sub(r'[\xa0 ]+',' ',text)
    text = re.sub(r'[ ]*\n[ ]*', '\n', text)
    text = re.sub(r'[\r\n]+', '\n', text)
    return text.strip()

def extract_adwhit(post):
    soup = BeautifulSoup(post.body_raw, 'html.parser')
    description = soup.find('div').text

    soup = BeautifulSoup(post.head_raw, 'html.parser')
    title = soup.select_one(".listingTitle").text

    soup = BeautifulSoup(post.head_raw, 'html.parser')
    meta = soup.select_one(".listingBody")
    meta = clean(meta.text)
    info = {'url': post.url, 
            'meta': clean(meta), 
            'title': clean(title), 
            'description': clean(description)}
    return info

def proc_adwhit(con):
    df = pd.read_sql("SELECT * FROM adwhit",con=con)
    df = df.loc[df.body_raw!="None"]
    proc = [extract_adwhit(post) 
               for post in tqdm.tqdm(df.itertuples(),total=len(df))]
    proc = pd.DataFrame(proc)
    proc = proc.set_index("url")
    proc.to_sql("adwhit_proc",if_exists="replace",con=con)
    
def crawl_adwhit(con):
    all_jobs = []
    for page in tqdm.trange(1,453):
        html_text = requests.get(f'https://www.adwhit.com/جميع-الوظائف/-1/0/{page}/50').text
        soup = BeautifulSoup(html_text, 'html.parser')
        jobs = soup.select('.jobsListingContainer .listingBlock')
        all_jobs.extend(jobs)

    df = pd.DataFrame(df, columns=['url','summary_raw','head_raw','body_raw'])
    for job in  tqdm.tqdm(all_jobs):
        url = job.find('a').get('href')
        html_text = requests.get(f'https://www.adwhit.com{url}').text
        soup = BeautifulSoup(html_text, 'html.parser')
        head = soup.select_one('.jobHeader')
        body = soup.select_one('.jobBody .description')
        df.loc[len(df),:] = (url, str(job), str(head), str(body))
    df = df.set_index('url')
    df.to_sql("adwhit",if_exists="append",con=con)


df = all_jobs()
df.loc[df['ar-chars']].to_csv('arabic.csv')

