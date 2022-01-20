import time 
import esco_utils as eu
import pandas as pd

from flask import Flask

app = Flask(__name__)



template = """
<style>
th {{
text-align: left !important;
}}

</style>
<head>
  <title>SkillLab Dashboard</title>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
</head>
<body>
<h1>Dashboard</h1>
<h2>Predictions</h2>
{perds}
<h2>User activities</h2>
{users}
</body>
"""
reload_time = time.time()
def reload_data():    
    global reload_time
    if time.time()-reload_time < 60*5: # reload every five minutes
        pred_stats = pd.read_csv('data/pred_stats.csv')
        user_stats = pd.read_csv('data/user_stats.csv')
    else:
        reload_time = time.time()
        API_TOKEN = eu.get_token()
        client = eu.Client(API_TOKEN)    
        client.api.base_api_url = 'http://flask_sdk:6221'
        tags, labels, pred_stats, user_stats = eu.get_stats(client)
        pred_stats.to_csv('data/pred_stats.csv')
        user_stats.to_csv('data/user_stats.csv')
        tags.to_csv('data/tags.csv')
    return user_stats, pred_stats 

@app.route("/")
def show_pred_stats():
    user_stats, pred_stats = reload_data()
    preds = pred_stats.to_html(index=False,classes=["table","table-striped"])
    users = user_stats.to_html(columns=['email','project_name','Count'],index=False,classes=["table","table-striped"])
    return template.format(perds=preds, users=users)

    
