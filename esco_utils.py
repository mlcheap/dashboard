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
    statuses = ['in-progress', 'pending', 'completed']
    tasks = [{**{'project_name':pname,'project_id':pid},**task} 
             for pname,pid in name2id.items() 
             for status in statuses 
             for task in client.get_all_tasks(pid,status=status)['data']['tasks']]
    tasks = pd.DataFrame(tasks).rename(columns={'_id':'task_id'})
    tasks['beta'] = tasks.project_name.apply(lambda x: len(x)==4 and x[3]=='2') # designate beta test 
    tasks = tasks[tasks.beta]


    # task data (title & description) 
    task_data = [{**task,**client.get_task(task['project_id'],task['task_id'])["data"]["items"][0]["data"]} 
             for task in tasks.to_dict(orient="record")]
    task_data = pd.DataFrame(task_data)
    task_data['model_id'] = task_data.project_id.apply(lambda x:lang2model[proj2lang[x]])

    # get labels (groups of tags)
    labels = [{**label,**{'project_id':task.project_id,'project_name':task.project_name}}
              for task in tasks[tasks.total_labels>0].itertuples() 
              for label in client.get_task(project_id=task.project_id,task_id=task.task_id)['data']['labels']]
    labels = pd.DataFrame(labels)
    labels = labels.merge(labelers,left_on='labeler_id',right_on='_id') # merge with labelers email 


    # get tags (individual tags)
    tags = [{**label,**tag} for label in labels.to_dict(orient="records") for tag in label['labels']]
    tags = pd.DataFrame(tags).rename(columns={'name':'occupation_title','_id':'occupation_id'}).drop('labels',axis=1)
    tags.occupation_id = tags.occupation_id.astype(int)

    if os.path.isfile('data/predictions.csv'): 
        predictions = pd.read_csv('data/predictions.csv')
        predictions = predictions.set_index('task_id')
        predictions.occ_ind = predictions.occ_ind.apply(lambda x: ast.literal_eval(x))
        predictions.occ_dist = predictions.occ_dist.apply(lambda x: ast.literal_eval(x))
    else:
        predictions = [{'task_id':task.task_id,'predictions': top_tags(model_id=task.model_id,
                                        title=task.title,
                                        description=task.description).json()} 
                       for task in task_data.itertuples()]
        predictions = pd.DataFrame(predictions)
        predictions['occ_ind'] = predictions.predictions.apply(lambda preds: {pred['index']:i for i,pred in enumerate(preds)})
        predictions['occ_dist'] = predictions.predictions.apply(lambda preds: {pred['index']:pred['distance'] for pred in preds})
        predictions = predictions.set_index('task_id')
        predictions.to_csv('data/predictions.csv')


    tags['pred_rank'] = tags.apply(lambda x: predictions.loc[x.task_id].occ_ind[x.occupation_id] 
                                   if x.occupation_id in predictions.loc[x.task_id].occ_ind else 3000,axis=1)
    tags['pred_dist'] = tags.apply(lambda x: predictions.loc[x.task_id].occ_dist[x.occupation_id] 
                                   if x.occupation_id in predictions.loc[x.task_id].occ_ind else 2,axis=1)
    for topN in [5,10,20,50]:
        tags[f'top_{topN}'] = tags.pred_rank.apply(lambda x: x<topN)
    pred_stats = tags.groupby('project_name')[['top_5','top_10','top_20','top_50']].mean()

    user_stats = labels.groupby(['email','project_name']).size().reset_index(name='Count').sort_values(by='Count',ascending=False)
    
    return tags, pred_stats, user_stats


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
    return occupation_local.to_dict(orient="row")
    
# def get_all_tags(lang,conn):
#     if lang=='ar':
#         occupation_local = psql.read_sql(f"""
#         SELECT * FROM occupation_translations 
#         WHERE description IS NOT NULL 
#         AND locale='{lang}'
#         """, skill_conn)
#     else:
#         occupation_local = psql.read_sql(f"""
#         SELECT * FROM occupation_translations 
#         WHERE alternates IS NOT NULL 
#         AND locale='{lang}'
#         """, conn)
#     return occupation_local.to_dict(orient="row")

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


def top_tags(model_id, title, description="", excluded=[]):
    import requests
    import json

    url = f"http://{ML_HOST}/top-tags"

    payload = json.dumps({
      "title": title,
      "description": description,
      "id": model_id,
      "excluded": excluded
    })
    headers = {
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    
    return response
