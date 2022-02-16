import time 
import json
import esco_utils as eu
import pandas as pd
import numpy as np
from flask import request
from flask import Flask
from flask import Response
from flask import make_response
from flask import render_template
from flask import g 
from flask import redirect 

def get_db(db):
    assert(db in ['vac','skill'])
    if db not in g:
        vac, skill = eu.load_skillLab_DB()
        D = {'vac': vac, 'skill': skill}
        g.db = D[db] 
    return g.db

_projects = {
    'humansintheloop': ['SAU3','SAU2'],
    'skilllab': ['GBR', 'GBR2', 'DEU', 'DEU2', 'MEX2', 'NLD', 'NLD2', 'MEX', 'BRA','SAU', 'BRA2', 'SAU2', 'SAU3'],
    'discoverdignify': ['MEX3','BRA3','MEX2','BRA2'],
}

app = Flask(__name__)

@app.route("/")
def index():
    return redirect("https://skilllab.mlcheap.com/dashboard/humansintheloop")

@app.route("/<client_name>")
def client(client_name):
    with open('static/index.html','r') as f:
        return f.read().format(onload=f"load_index('{client_name}', '0.5H')")
      
@app.route("/test")
def test():
    with open('static/index.html','r') as f:
        return f.read().format(onload=f"chart();")
      

@app.route("/src/<name>")
def resource(name):
    with open(f"static/{name}",'r') as f:
        return f.read()


@app.route("/users-activity")
def users_activity():
    email = request.args.get('email')
    freq = request.args.get('freq')
    project_name = request.args.get('project_name')
    df = pd.read_csv('data/labels.csv')
    if email:
        df = df.loc[df.email==email]
    if project_name:
        df = df.loc[df.project_name==project_name]
    df.inserted_at = pd.to_datetime(df.inserted_at)
    df = df[['email','inserted_at','project_name','labels']]
    df = df.groupby(['email','project_name',pd.Grouper(key="inserted_at", freq=freq)]).count()
    df = df.reset_index().groupby(['email','project_name']).agg(list)
    response = df.to_json(orient='table')
    return Response(response, mimetype='application/json')


@app.route("/user-stats")
def user_stats():
    email = request.args.get('email')
    project_name = request.args.get('project_name')
    formatted = request.args.get('formatted')
    df1 = eu.tag_stats(project_name=project_name,email=email)
    df2 = eu.label_stats(project_name=project_name,email=email)
    _, stats = eu.get_consensus(project_name)
    df = stats.reset_index()
    labels = pd.read_csv('data/labels.csv')
    projs = labels.project_name.unique()
    emails = labels.email.unique()
    df = pd.DataFrame([{'email':email, 'project_name': proj, **df.loc[(df.project_name==proj)
                                                                      &((df.email_x==email)
                                                                        |(df.email_y==email))][['similar','different']].sum()} 
                       for email in emails for proj in projs])
    # if len(df)==0:
    #     df = pd.DataFrame([{'email':email, 'project_name': proj,'similar':1,'different': 0} for email in emails for proj in projs])
    
    df['total'] = df.similar + df.different
    df3 = df[['email','project_name','similar','different','total']]
    
    df = pd.merge(df1.reset_index(),df2.reset_index(),on=['email','project_name'])
    df = pd.merge(df,df3.reset_index(),on=['email','project_name'])
    df = df.sort_values('total_tasks',ascending=False)
    if formatted:
        S = df.sum()
        S.iloc[0],S.iloc[1] = 'SUM',''
        df.loc[len(df),:] = S
        df.total_tasks = df.total_tasks.astype(int)
        df.total_tags = df.total_tags.astype(int)
        df['time'] = df.inserted_at.apply(lambda x: f"{x/3600:.2f}")
        df['speed'] = (df.inserted_at / df.total_tasks).apply(lambda x: f"{x:.0f}" if x>0 else "-")
        df.search = (df.search / df.total_tags).apply(lambda x: f"{x*100:.1f}%" if x>0 else "<0.1%")
        df.noisy = (df.noisy / df.total_tags).apply(lambda x: f"{x*100:.2f}%" if x>0 else "<0.1%")
        df.top_5 = (df.top_5 / df.total_tags).apply(lambda x: f"{x*100:.2f}%" if x>0 else "<0.1%")
        df['consensus'] = (df.similar / df.total).apply(lambda x: f"{x*100:.2f}%" if x>0 else "-")
        df = df[['email','total_tasks','time','speed','consensus','noisy', 'search','top_5']]
        df = df.rename(columns={'time': 'active time (h)', 'total_tasks': 'total tasks', 'speed':'time/task (sec)', 'noisy': 'noisy tags'})
    response = df.to_json(orient='table',index=False)
    return Response(response, mimetype='application/json')
  
@app.route("/get-projects")
def get_projects():
    client_name = request.args.get('client_name')
    projects = _projects[client_name]
    df = pd.read_csv('data/labels.csv')[['project_name','inserted_at']]
    df = df.loc[df.project_name.isin(projects)]
    df.inserted_at = pd.to_datetime(df.inserted_at)
    df = df.groupby('project_name')['inserted_at'].max()
    df.sort_values(inplace=True,ascending=False)
    response = list(df.index)
    return Response(json.dumps(response), mimetype='application/json')

@app.route("/project-stats")
def project_stats():
    client_name = request.args.get('client_name')
    projs = _projects[client_name]
    df = pd.read_csv('data/labels.csv')
    df = df.loc[df.project_name.isin(projs)]
    df = df.groupby(['email','project_name']).count()['labels']
    response = df.to_json(orient='table')
    return Response(response, mimetype='application/json')
    
@app.route("/inconsistent-tasks")
def incosisntent_tasks():
    project_name = request.args.get('project_name')
    labels, stats = eu.get_consensus(project_name)
    labels = labels.loc[labels.different>0]
    response = labels[['task_id']].to_json(orient='table')
    return Response(response, mimetype='application/json') 


@app.route("/get-task")
def get_task():
    task_id = request.args.get('task_id')
    df = pd.read_csv('data/tags.csv')
    df = df.loc[df.task_id==task_id]
    task_data = df[['title','description','lang']].iloc[0].to_dict()
    occupations = df.groupby(['occupation_id'])['email'].apply(lambda x:list(set(x))).reset_index().rename(columns={'email':'emails'})
    occupations = [{**eu.sql_get_tag(task_data['lang'],occ.occupation_id, get_db('skill')),'emails':occ.emails} 
                                for _,occ in occupations.iterrows()]
    occupations = pd.DataFrame(occupations)
    response = {'task': task_data, 'tags': occupations.drop(['updated_at','created_at'],axis=1).to_dict(orient='records')}
    return Response(json.dumps(response), mimetype='application/json') 


@app.route("/get-tasks")
def get_tasks():
    project_name = request.args.get('project_name')
    email = request.args.get('email')
    num_tags = request.args.get('num_tags')
    status = request.args.get('status')
    df = pd.read_csv('data/labels.csv')
    df.labels = df.labels.apply(eval)
    df.labelers = df.labelers.apply(eval)
    df['num_tags'] = df.labels.apply(len)
    if email:
        df = df.loc[df.email==email]
    if status:
        df = df.loc[df.status==status]
    if project_name:
        df = df.loc[df.project_name==project_name]
    if status:
        df = df.loc[df.status==status]
    if num_tags:
        df = df.loc[df.num_tags >= int(num_tags)]
    resposne = df.to_json(orient='table')
    return Response(resposne, mimetype='application/json') 
  
