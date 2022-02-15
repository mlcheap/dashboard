import requests
from bs4 import BeautifulSoup
import os
import sys
import json
import pandas as pd
import tqdm


def search(start=0):

    url = f"https://forasna.com/job/search_data?start={start}"

    payload={}
    headers = {
      'authority': 'forasna.com',
      'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
      'accept': 'application/json, text/plain, */*',
      'x-requested-with': 'XMLHttpRequest',
      'sec-ch-ua-mobile': '?0',
      'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
      'sec-ch-ua-platform': '"macOS"',
      'sec-fetch-site': 'same-origin',
      'sec-fetch-mode': 'cors',
      'sec-fetch-dest': 'empty',
      'referer': 'https://forasna.com/%D9%88%D8%B8%D8%A7%D8%A6%D9%81-%D8%AE%D8%A7%D9%84%D9%8A%D8%A9?',
      'accept-language': 'en-US,en;q=0.9',
      'cookie': 'forasna_=ec4c1fu0urvkqk9dd483f7lrc77dffb0; _ga=GA1.2.1591321906.1644255072; _gid=GA1.2.1810727906.1644255072; _fbp=fb.1.1644255071754.1158450958; __insp_wid=661323739; __insp_nv=true; __insp_targlpu=aHR0cHM6Ly9mb3Jhc25hLmNvbS8%3D; __insp_targlpt=2YjYuNin2KbZgSDYrtin2YTZitipINmB2Yog2YXYtdixIHwg2YHYsdi12YbYpw%3D%3D; __insp_norec_sess=true; cto_bundle=24Fyh19XVENCejBNMlRsbU9IS1RNbDFwOWVPMyUyQk1JSW16M0p5MlVQZldMSFdWaTdRbU5DYlE4U2UwbmpUOHpTTkVNNHAzUTdqJTJGeklMaU1yeHZYUkVCRnJCSGZwTjkxUVZrRHc4M2Rzb2tsUWJkMHA3dDZVcGNVJTJGNDBRN3hFaSUyQkFlUDhzaUc4Sm43NzdacEdmdUVQb3hhRWtPZ3klMkYyTlZXJTJCdUdlY1ZEQVMwMnNqYzhZR0YxWlFuc2dwc205SE5mUks0c3Q; __insp_slim=1644255138384; user_visit_log=%7B%22landing_url%22%3A%22https%3A%5C%2F%5C%2Fforasna.com%5C%2Fdist%5C%2Fcss%5C%2Ffonts%5C%2Fforasna-icons.ttf%22%2C%22utms%22%3A%5B%5D%2C%22referrer_url%22%3A%22https%3A%5C%2F%5C%2Fforasna.com%5C%2Fdist%5C%2Fcss%5C%2Ffrontend%5C%2Fcritical-homepage.css%3Fv%3D1644245780%22%2C%22referrer_domain%22%3A%22forasna.com%22%2C%22os%22%3A%22mac%22%2C%22device%22%3A%22desktop%22%2C%22browser%22%3A%22chrome%22%2C%22visit_time%22%3A%2222-02-07+07%3A36%3A13%22%2C%22db_stored%22%3Afalse%7D; _gat=1; forasna_=ec4c1fu0urvkqk9dd483f7lrc77dffb0; user_visit_log=%7B%22landing_url%22%3A%22https%3A%5C%2F%5C%2Fforasna.com%5C%2Fdist%5C%2Fcss%5C%2Ffonts%5C%2Fforasna-icons.ttf%22%2C%22utms%22%3A%5B%5D%2C%22referrer_url%22%3A%22https%3A%5C%2F%5C%2Fforasna.com%5C%2Fdist%5C%2Fcss%5C%2Ffrontend%5C%2Fcritical-homepage.css%3Fv%3D1644245780%22%2C%22referrer_domain%22%3A%22forasna.com%22%2C%22os%22%3A%22mac%22%2C%22device%22%3A%22desktop%22%2C%22browser%22%3A%22chrome%22%2C%22visit_time%22%3A%2222-02-07+07%3A49%3A27%22%2C%22db_stored%22%3Afalse%7D'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    jobUrls = [job['_source']['jobCanonicalURL'] for job in response.json()['hits']['hits']]
    
    return response, jobUrls


def fetch(joburl):
    url = f'https://forasna.com/job/p/{joburl}'

    payload={}
    headers = {
      'authority': 'forasna.com',
      'cache-control': 'max-age=0',
      'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"macOS"',
      'upgrade-insecure-requests': '1',
      'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
      'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
      'sec-fetch-site': 'same-origin',
      'sec-fetch-mode': 'navigate',
      'sec-fetch-user': '?1',
      'sec-fetch-dest': 'document',
      'referer': 'https://forasna.com/%D9%88%D8%B8%D8%A7%D8%A6%D9%81-%D8%AE%D8%A7%D9%84%D9%8A%D8%A9?start=20',
      'accept-language': 'en-US,en;q=0.9',
      'cookie': 'forasna_=ec4c1fu0urvkqk9dd483f7lrc77dffb0; _ga=GA1.2.1591321906.1644255072; _gid=GA1.2.1810727906.1644255072; _fbp=fb.1.1644255071754.1158450958; __insp_wid=661323739; __insp_nv=true; __insp_targlpu=aHR0cHM6Ly9mb3Jhc25hLmNvbS8%3D; __insp_targlpt=2YjYuNin2KbZgSDYrtin2YTZitipINmB2Yog2YXYtdixIHwg2YHYsdi12YbYpw%3D%3D; __insp_norec_sess=true; criteo_last_product=239843; _gat=1; user_visit_log=%7B%22landing_url%22%3A%22https%3A%5C%2F%5C%2Fforasna.com%5C%2Fjob%5C%2Fsearch_data%22%2C%22utms%22%3A%5B%5D%2C%22referrer_url%22%3A%22+%22%2C%22referrer_domain%22%3A%22+%22%2C%22os%22%3A%22mac%22%2C%22device%22%3A%22desktop%22%2C%22browser%22%3A%22chrome%22%2C%22visit_time%22%3A%2222-02-07+07%3A58%3A42%22%2C%22db_stored%22%3Afalse%7D; cto_bundle=Ab1dXV9XVENCejBNMlRsbU9IS1RNbDFwOWVIVlRCc0o1UFpJdDlXWnVmQ3FLRlpqelFmcEtLSlJaUTBjbWNvdE56dCUyQks3dnFIM1NLaEloV0owZnZuNzE0a3hOJTJGeVR1cHMxcm9yOEVDaWhQNDVQcjlSOHViRGprSDNpaTAwQnRDdllYRE1yNGM0QklqUVBYWjZ2RGdYVjFSQktkJTJCQXZVQ0JYSFFTUTNtUm03a2xsY0NHR1klMkJFbWY2bkFxQ3Yzd1NtYjR3Sw; __insp_slim=1644256722812; forasna_=ec4c1fu0urvkqk9dd483f7lrc77dffb0; user_visit_log=%7B%22landing_url%22%3A%22https%3A%5C%2F%5C%2Fforasna.com%5C%2Fjob%5C%2Fsearch_data%22%2C%22utms%22%3A%5B%5D%2C%22referrer_url%22%3A%22+%22%2C%22referrer_domain%22%3A%22+%22%2C%22os%22%3A%22mac%22%2C%22device%22%3A%22desktop%22%2C%22browser%22%3A%22chrome%22%2C%22visit_time%22%3A%2222-02-07+08%3A01%3A08%22%2C%22db_stored%22%3Afalse%7D'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    soup = BeautifulSoup(response.text, "html.parser")

    return response, soup


def main(start):
    response, job_urls = search(start=start)
    if os.path.exists('forsana.jl'):
        df = pd.read_json('forsana.jl',lines=True)
        crawled_urls = set(df.url.values)
    else:
        crawled_urls = set()
    with open('forsana.jl', 'a') as f:
        for url in job_urls:
            if url in crawled_urls:
 #               print(f'url {url[:10]}... already crawled')
                continue
            response, soup = fetch(url)    
            title = soup.select_one('[itemprop="title"]').attrs['content']
            desc = soup.select_one('[itemprop="description"]').text
            data = {'url': url, 'raw': response.text, 'title':title, 'description': desc}
            f.write(json.dumps(data,ensure_ascii=False)+'\n')
#            print(title, desc)


# sample: python forsana.py 1-175
if __name__=='__main__':
    pages = sys.argv[1]
    if ',' in pages:
        pages = [int(e) for e in pages.split(',')]
    elif '-' in pages:
        i,j = pages.split('-')[0], pages.split('-')[1]
        pages = list(range(int(i),int(j)+1))
    else:
        pages = [int(sys.argv[1])]
    for page in tqdm.tqdm(pages,total=len(pages)):
        main(page*20)

