import os
import requests
import json
import ast
import pandas as pd
import psycopg2 as pg
import pandas.io.sql as psql
import tqdm

from MlCheap.Client import Client
from MlCheap.LabelClass import LabelClass
from MlCheap.Task import Task, Data, Label
from MlCheap.Project import Project

SDK_HOST = os.getenv('SDK_HOST') or 'localhost:6221'
ML_HOST = os.getenv('ML_HOST') or 'localhost:5000'
SKILL_HOST = os.getenv('SKILL_HOST') or 'localhost:8765'
VAC_HOST = os.getenv('VAC_HOST') or 'localhost:5678'


ESCO_TEXT_TAGGING_TYPE = 'esco-text-tagging'

def get_stats(client):
    all_projs = client.get_all_projects()['data']['projects']
    name2id = {prj['project_name']:prj['project_id'] for prj in all_projs}
    proj2lang = {proj['project_id']:v 
                 for proj in all_projs 
                 for k,v in client.get_project(proj["project_id"])["data"]["metadata"].items() 
                 if k=="lang"}
    models = pd.DataFrame(get_models()["data"])
    lang2model = {lang:models[models.lang==lang].id.iloc[0] for lang in models.lang.unique()}

    # get labelers 
    labelers = [labeler 
                for pname,pid in name2id.items() 
                for labeler in client.all_labelers(project_id=pid)['data']['active_labelers']]
    labelers = pd.DataFrame(labelers)

    # tasks
    statuses = ['in-progress', 'pending', 'complete']
    tasks = [{**{'project_name':pname,'project_id':pid},**task} 
             for pname,pid in name2id.items() 
             for status in statuses 
             for task in client.get_all_tasks(pid,status=status)['data']['tasks']]
    tasks = pd.DataFrame(tasks).rename(columns={'_id':'task_id'})
    tasks['beta'] = tasks.project_name.apply(lambda x: len(x)==4 or len(x)==3) # designate beta test 
    tasks = tasks[tasks.beta]
    # task data (title & description) 
    task_data = [{**task,**client.get_task(task['project_id'],task['task_id'])["data"]["items"][0]["data"]} 
             for task in tasks.to_dict(orient="record")]
    task_data = pd.DataFrame(task_data)
    task_data['lang'] = task_data.project_id.apply(lambda x:proj2lang[x])
    task_data['model_id'] = task_data.lang.apply(lambda x:lang2model[x])
    tasks = pd.merge(tasks,task_data[['task_id','title','description','lang','model_id']], on='task_id')

    # get labels (groups of tags)
    labels = [{**label,**task}
              for taski,task in tasks[tasks.total_labels>0].iterrows() 
              for label in client.get_task(project_id=task.project_id,task_id=task.task_id)['data']['labels']]
    labels = pd.DataFrame(labels)
    labels = labels.merge(labelers,left_on='labeler_id',right_on='_id') # merge with labelers email 
    labels = labels.loc[~labels[['email','task_id']].duplicated() ] # de-duplicate based on (email,task-id) pair 


    # get tags (individual tags)
    tags = [{**label,**tag} for label in labels.to_dict(orient="records") for tag in label['labels']]
    tags = pd.DataFrame(tags).rename(columns={'name':'occupation_title','_id':'occupation_id'}).drop('labels',axis=1)
    tags.occupation_id = tags.occupation_id.astype(int)
    tags = tags.loc[~tags[['email','task_id','occupation_id']].duplicated()]

    predictions = [{'task_id':task.task_id,
                    'predictions': top_tags(model_id=task.model_id,
                                            title=task.title,
                                            noise=0,
                                            description=task.description).json()} 
                   for task in tqdm.tqdm(task_data.itertuples(),total=len(task_data))]
    predictions = pd.DataFrame(predictions)
    predictions['occ_ind'] = predictions.predictions.apply(lambda preds: {pred['index']:i for i,pred in enumerate(preds)})
    predictions['occ_dist'] = predictions.predictions.apply(lambda preds: {pred['index']:pred['distance'] for pred in preds})
    predictions_exact = predictions.set_index('task_id')

    predictions = [{'task_id':task.task_id,
                    'predictions': top_tags(model_id=task.model_id,
                                            title=task.title,
                                            noise=.3,
                                            description=task.description).json()} 
                   for task in tqdm.tqdm(task_data.itertuples(),total=len(task_data))]
    predictions = pd.DataFrame(predictions)
    predictions['occ_ind'] = predictions.predictions.apply(lambda preds: {pred['index']:i for i,pred in enumerate(preds)})
    predictions['occ_dist'] = predictions.predictions.apply(lambda preds: {pred['index']:pred['distance'] for pred in preds})
    predictions_noisy = predictions.set_index('task_id')
    tags['pred_rank_exact'] = tags.apply(lambda x: predictions_exact.loc[x.task_id].occ_ind[x.occupation_id] 
                                   if x.occupation_id in predictions_exact.loc[x.task_id].occ_ind else 3000,axis=1)
    tags['pred_rank_noisy'] = tags.apply(lambda x: predictions_noisy.loc[x.task_id].occ_ind[x.occupation_id] 
                                   if x.occupation_id in predictions_noisy.loc[x.task_id].occ_ind else 3000,axis=1)
    tops = [0,5,10,20,50]
    for i in range(1,len(tops)):
        tags[f"top_{tops[i]}"] = (tags.pred_rank_exact<tops[i]) & (tags.pred_rank_exact>=tops[i-1])
    tags['search'] = tags.pred_rank_exact>=50
    tags['noisy'] = tags.pred_rank_exact>tags.pred_rank_noisy
    
    pred_stats = tags.groupby('project_name')[['top_5','top_10','top_20','top_50']].mean()
    user_stats = labels.groupby(['email','project_name']).size().reset_index(name='Count').sort_values(by='Count',ascending=False)
    
    tasks.to_csv('data/tasks.csv')
    labels.to_csv('data/labels.csv')
    tags.to_csv('data/tags.csv')
    predictions_noisy.to_csv('data/predictions_noisy.csv')
    predictions_exact.to_csv('data/predictions_exact.csv')
    pred_stats.to_csv('data/pred_stats.csv')
    user_stats.to_csv('data/user_stats.csv')
    
    return tags, labels, pred_stats, user_stats 


def get_token():
    url = f"http://{SDK_HOST}/v1/get-token"

    payload = json.dumps({
      "username": "root",
      "password": "xHe>MARq7yBt-=4*"
    })
    headers = {
      'token': '6be453ab-e857-414c-8876-eeaab3962a77',
      'Content-Type': 'application/json',
      'Cookie': '_xsrf=2|393c593a|c92fe3879eab42aca29580c7ce71b3be|1640337308'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.json())

    API_TOKEN = response.json()['data']['token']
    return API_TOKEN

class EscoOccupationData(Data):
    def __init__(self, title, description):
        name = "esco-occupations-data"
        super(EscoOccupationData, self).__init__(name)
        self.title = title
        self.description = description

    def __to_dic__(self):
        return {'title': self.title, "description": self.description}


class EscoOccupationLabel(Label):
    def __init__(self):
        name = "esco-occupation-label"
        question = ""
        super(EscoOccupationLabel, self).__init__(name)
        self.all_tags = "{{project.metadata.all_tags}}"
        self.question = question
        self.ai_predicts = "{{config:{topN=5}}}"

    def __to_dic__(self):
        return {'all-tags': self.all_tags,
                'question': self.question,
                'ai-predicts': self.ai_predicts}


class EscoOccupationTask(Task):

    def __init__(self, title, desciption):
        super(EscoOccupationTask, self).__init__(ESCO_TEXT_TAGGING_TYPE)
        self.data_text = EscoOccupationData(title, desciption)
        self.label_tagging = EscoOccupationLabel()

    def get_items(self):
        return [self.data_text.to_dic(), self.label_tagging.to_dic()]


def load_skillLab_DB():
    DB_USER = "amir"
    DB_PASS = '8JF9hb!7VD@L'

    VAC_DBNAME = "postgres"

    SKILL_DBNAME = "web_production"

    vac_conn = pg.connect(user=DB_USER,
                    password=DB_PASS,
                    host=VAC_HOST.split(':')[0],
                    port=VAC_HOST.split(':')[1],
                    database=VAC_DBNAME)

    skill_conn = pg.connect(user=DB_USER,
                    password=DB_PASS,
                    host=SKILL_HOST.split(':')[0],
                    port=SKILL_HOST.split(':')[1],
                    database=SKILL_DBNAME)
    return vac_conn, skill_conn

def add_classes(client, data,project_id):
    new_classes = []
    for idx, _class in enumerate(data):
        name = _class["title"]
        metadata = {"index": _class["occupation_id"],
                    "description": str(_class["description"]),
                    "alternates": str(_class["alternates"]),
                    "locale": str(_class["locale"]),
                    }
        unique_id = _class["occupation_id"]
        label_class = LabelClass(name=name, metadata=metadata, unique_id=unique_id)
        new_classes.append(label_class)
    response = client.create_classes(project_id, new_classes)
    # print(response)
    
def sql_all_tags(lang,conn):
    occupation_local = psql.read_sql(f"""
        SELECT * FROM
        (SELECT * FROM occupations WHERE data_set='esco') AS occ 
        LEFT JOIN (SELECT * FROM occupation_translations WHERE locale='{lang}') as occtr
        ON occ.id=occtr.occupation_id
        """, conn)
    return occupation_local


def sql_get_tag(lang, id, conn):
    occupation = psql.read_sql(f"""
        SELECT * FROM
        (SELECT * FROM occupations WHERE data_set='esco' AND id={id}) AS occ 
        LEFT JOIN (SELECT * FROM occupation_translations WHERE locale='{lang}') as occtr
        ON occ.id=occtr.occupation_id
        """, conn).iloc[0]
    return occupation.to_dict()


def sample_vacancy(country, conn, num=300):
    job = psql.read_sql(f"""
        SELECT * FROM jobs 
        WHERE location_country='{country}'
        AND RANDOM() <= .05
        LIMIT {num}
        """, conn)

    return job

def add_samples(client, samples, project_id):
    for i in range(len(samples)):
        text_tagging_task = EscoOccupationTask(samples.iloc[i]['title'],
                                               samples.iloc[i]['description'])
        response = client.create_task(project_id=project_id, task=text_tagging_task)
        # print(response)

        
def train_models(langs):
    for lang in langs:
        train_model(lang)
        
def train_model(lang):
    url = f"http://{ML_HOST}/train"
    payload = json.dumps({
      "model_name": "tfidf_knn",
      "lang": lang,
      "ngram_min": 1,
      "ngram_max": 4,
      "n_neighbors": 50,
      "title_imp": 10,
      "alt_title_imp": 10,
      "case_insensitive": True
    })
    headers = {
      'Content-Type': 'application/json',
      'Cookie': '_xsrf=2|393c593a|c92fe3879eab42aca29580c7ce71b3be|1640337308'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    
    print(response.text)
    return response

def get_models():
    url = f"http://{ML_HOST}/all-models"

    payload={}
    headers = {
      'Cookie': '_xsrf=2|393c593a|c92fe3879eab42aca29580c7ce71b3be|1640337308'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json()


def top_tags(model_id, title, noise, description="", excluded=[]):
    import requests
    import json

    url = f"http://{ML_HOST}/top-tags"

    payload = json.dumps({
      "title": title,
      "description": description,
      "id": model_id,
      "excluded_indices": excluded,
      "noise": noise,
    })
    headers = {
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    
    return response


def get_consensus(project_name):
    labels = pd.read_csv('data/labels.csv')
    labels.labels = labels.labels.apply(lambda x: eval(x))
    if project_name:
        labels = labels.loc[labels.project_name==project_name]
    labels['occ_ids'] = labels.labels.apply(lambda x: [y['_id'] for y in x])
    labels['occ_title'] = labels.labels.apply(lambda x: [y['name'] for y in x])

    labels = pd.merge(labels,labels[['project_name','task_id','email','occ_ids','occ_title']],how='outer',on=['project_name','task_id'])
    labels = labels.loc[labels.email_x<labels.email_y]
    labels = labels.loc[~labels.task_id.duplicated()]

    if len(labels)>0:
        labels['label_diff'] = labels.apply(lambda x: list(set(x.occ_ids_x)-set(x.occ_ids_y))+list(set(x.occ_ids_y)-set(x.occ_ids_x)),axis=1)
        labels['label_int'] = labels.apply(lambda x: list(set(x.occ_ids_x).intersection(set(x.occ_ids_y))),axis=1)
        labels['different'] = labels.label_diff.apply(lambda x: len(x))
        labels['similar'] = labels.label_int.apply(lambda x: len(x))

        stats = labels.groupby(['project_name','email_x','email_y']).sum()[['similar','different']]
    else:
        stats = pd.DataFrame(columns=['project_name','email_x','email_y','similar','different'])
    return labels, stats
  
  
def tag_stats(project_name, email):
    df = pd.read_csv('data/tags.csv')
    df = df.loc[df.email==email] if email else df
    df = df.loc[df.project_name==project_name] if project_name else df
    df['total_tags'] = 1
    df = df.groupby(['email','project_name'])[['total_tags','search','noisy','top_5','top_10','top_10','top_20','top_50']].sum()
    return df
  
  
def label_stats(project_name, email):
    df = pd.read_csv('data/labels.csv')
    df.inserted_at = pd.to_datetime(df.inserted_at)
    df = df.loc[df.email==email] if email else df
    df = df.loc[df.project_name==project_name] if project_name else df

    stats = df.sort_values(['inserted_at']).groupby(['email','project_name'])
    stats = stats.agg({'inserted_at': lambda x: sum([y.total_seconds() for y in x.diff() if y.total_seconds()<60*10]),
                        'labels': lambda x: len(x)})
    stats.labels = stats.labels.astype(int)
    stats = stats.rename(columns={'labels': 'total_tasks'})
    
    return stats

