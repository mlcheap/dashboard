import esco_utils as eu
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sqlite3
import tqdm
import sys


def cache_tasks(client,con):
    all_projs = client.get_all_projects()['data']['projects']
    name2id = {prj['project_name']:prj['project_id'] for prj in all_projs}
    statuses = ['in-progress', 'pending', 'complete']
    
    tasks = [{**{'project_name':pname,'project_id':pid},**task} 
             for pname,pid in name2id.items() 
             for status in statuses 
             for task in client.get_all_tasks(pid,status=status)['data']['tasks']]
    df = pd.DataFrame(tasks).drop('labelers',axis=1)
    df.to_sql("tasks",con,if_exists="replace",index=False)
    
    pbar = tqdm.tqdm(tasks,total=len(tasks))
    pbar.set_description("loading tasks" )
    task_data = [{**task,**client.get_task(task['project_id'],task['_id'])["data"]} 
                 for task in pbar]
    df = pd.DataFrame([{**task,**task['items'][0]['data']} for task in task_data])
    df = pd.DataFrame(df).drop(['items','labelers','labels','callbacks'],axis=1)
    df.to_sql("task_data",con,if_exists="replace",index=False)
    
    labels = [{**label,'task_id': task['_id']} 
              for task in task_data 
              for label in task['labels']]
    tags = [{**label,**{'occupation_id': tag['_id'], 'occupation_title': tag['name']}} 
            for label in labels 
            for tag in label['labels']]
    df = pd.DataFrame(tags)
    df = df.drop(['labels'],axis=1)
    df.to_sql("tags",con,if_exists="replace")
    
def cache_projects(client,con):
    all_projs = client.get_all_projects()['data']['projects']
    name2id = {prj['project_name']:prj['project_id'] for prj in all_projs}
    statuses = ['in-progress', 'pending', 'complete']
    
    project_data = [client.get_project(prj['project_id'])['data'] for prj in all_projs]
    project_data = [{**P,**P['metadata'],**P['stat']} for P in project_data]
    df = pd.DataFrame(project_data)
    df = df.drop(['metadata','stat','labelers'],axis=1)
    df.to_sql("projects",con,if_exists="replace",index=False)

    project_labelers = [(P['project_id'],P['stat']['labelers']) for P in project_data]
    project_labelers = [{'project_id': pid, 'labeler_status':status, **labeler} 
                        for pid,P in project_labelers 
                        for status,labelers in P.items() 
                        for labeler in labelers]
    df = pd.DataFrame(project_labelers)
    
    df.to_sql("project_labelers",con,if_exists="replace",index=False)

    
def cache_occupations(skill_con,con):
    for T in ['occupations','skill','ISCOGroups','skillGroups','transversalSkillsCollection']:
        files = glob.glob(f'data/esco/v1.0.3/{T}*.csv')
        dfs = []
        for file in files:
            df = pd.read_csv(file);
            df['lang'] = file.split('_')[-1].split('.')[0]
            dfs.append(df)
        df = pd.concat(dfs)
        df['esco_version'] = 'v1.0.3'
        df.to_sql(f"{T}",con,if_exists="replace",index=False)
    for T in ['skillSkillRelations','occupationSkillRelations','broaderRelationsOccPillar','broaderRelationsSkillPillar']:
        df = pd.read_csv(f'data/esco/v1.0.3/{T}.csv')
        df['esco_version'] = 'v1.0.3'
        df.to_sql(f"{T}",con,if_exists="replace",index=False)

    # insert occupation_id from skillLab DB 
    occ = pd.read_sql("SELECT * FROM occupations",con)  
    skill_occ = psql.read_sql("SELECT * FROM occupations WHERE data_set='esco'", skill_con)
    occ = pd.merge(occ,skill_occ[['id','external_id']],left_on='conceptUri',right_on='external_id',how='left')
    occ = occ.rename(columns={'id': 'occupation_id'})
    occ.to_sql("occupations",con,if_exists="replace",index=False)
    
    
def reload():
    API_TOKEN = eu.get_token()
    client = eu.Client(API_TOKEN)
    client.api.base_api_url = 'http://flask_sdk:6221'
    con = sqlite3.connect('data/cache.db')
    cache_tasks(client,con)
    cache_projects(client,con)
    eu.get_stats(client)
    print('reloaded')


if __name__=='__main__':
    while True:
        try:
            reload()
        except KeyboardInterrupt:
            sys.exit(0)
        except Exception:
             pass
