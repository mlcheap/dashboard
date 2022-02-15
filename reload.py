import esco_utils as eu
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def reload():
    API_TOKEN = eu.get_token()
    client = eu.Client(API_TOKEN)
    client.api.base_api_url = 'http://flask_sdk:6221'
    eu.get_stats(client)
    print('reloaded')
    
if __name__=='__main__':
    while True:
        try:
            reload()
        except Exception:
             pass
