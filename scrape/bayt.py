import requests
import sys
import json
import os
import tqdm
from bs4 import BeautifulSoup
import pandas as pd

def fetch_job(page, jobId, country='saudi-arabia'):

    url = f"https://www.bayt.com/en/{country}/jobs/?page={page}&jobId={jobId}"
    #url = "https://www.bayt.com/en/saudi-arabia/jobs/?page=1&jobId=4485639"

    payload={'jobOnly': 'true'}
    files=[

    ]
    headers = {
      'authority': 'www.bayt.com',
      'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
      'x-requested-with': 'XMLHttpRequest',
      'sec-ch-ua-mobile': '?0',
      'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
      'sec-ch-ua-platform': '"macOS"',
      'accept': '*/*',
      'origin': 'https://www.bayt.com',
      'sec-fetch-site': 'same-origin',
      'sec-fetch-mode': 'cors',
      'sec-fetch-dest': 'empty',
      'referer': 'https://www.bayt.com/en/saudi-arabia/jobs/?page=1&jobId=4485639',
      'accept-language': 'en-US,en;q=0.9,fa-IR;q=0.8,fa;q=0.7',
      'cookie': 'brID=3196817984777971951648; ISLOGGED0=0; user-prefs=locale%20xx%20lang%20en%20geo%20ch; _gid=GA1.2.2072758243.1644193798; _ym_uid=1638871593328733712; _ym_d=1644193798; _ym_isad=2; _clck=i2keey|1|eys|0; _gcl_au=1.1.107500405.1644193799; _pk_id.1.c132=794b8dc72ee6f9cf.1644193803.; aff_data={%22qs%22:%22%22%2C%22ref%22:%22https://www.bayt.com/%22}; JB_SRCH_TKN=%2FoPHbiNnW%2F8%3D1644194008; __gsas=ID=decd48a1e02b41aa:T=1644229442:S=ALNI_MaX1nhLv-FNRn72YHGiF2TcEh-8Ng; g_state={"i_p":1644315850628,"i_l":2}; MSESID0=3197182125491119193630%2C0%2C0%2C0%2CZP6H71%2C0%2C5%2C71fcd63ff6c5dea10fba2f20ceed7052; BSESINFO0=98%2CX21V53%2C%2C; NaviPageUrl=https://www.bayt.com/en/saudi-arabia/jobs/legal-secretary-4485575/; __atuvc=10%7C6; _ym_visorc=w; _pk_ses.1.c132=1; _iub_cs-29998284=%7B%22consent%22%3Atrue%2C%22timestamp%22%3A%222022-02-07T00%3A30%3A04.652Z%22%2C%22version%22%3A%221.36.1%22%2C%22id%22%3A%2229998284%22%7D; userJobId=4485662; _ga=GA1.2.772674273.1644193797; _dc_gtm_UA-1644414-1=1; _clsk=mn0etr|1644243271097|186|0|b.clarity.ms/collect; _ga_1NKPLGNKKD=GS1.1.1644242605.4.1.1644243271.0; _ga_ZJ86J4RMT9=GS1.1.1644242605.4.1.1644243271.18; userJobId=4485639'
    }

    response = requests.request("GET", url, headers=headers, data=payload, files=files)
    return response

def search_jobs(page, country):

    url = f"https://www.bayt.com/en/{country}/jobs/?page={page}"

    payload={}
    headers = {
      'authority': 'www.bayt.com',
      'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"macOS"',
      'upgrade-insecure-requests': '1',
      'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
      'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
      'sec-fetch-site': 'none',
      'sec-fetch-mode': 'navigate',
      'sec-fetch-user': '?1',
      'sec-fetch-dest': 'document',
      'accept-language': 'en-US,en;q=0.9,fa-IR;q=0.8,fa;q=0.7',
      'cookie': 'brID=3196817984777971951648; ISLOGGED0=0; user-prefs=locale%20xx%20lang%20en%20geo%20ch; _gid=GA1.2.2072758243.1644193798; _ym_uid=1638871593328733712; _ym_d=1644193798; _ym_isad=2; _clck=i2keey|1|eys|0; _gcl_au=1.1.107500405.1644193799; _pk_id.1.c132=794b8dc72ee6f9cf.1644193803.; aff_data={%22qs%22:%22%22%2C%22ref%22:%22https://www.bayt.com/%22}; JB_SRCH_TKN=%2FoPHbiNnW%2F8%3D1644194008; __gsas=ID=decd48a1e02b41aa:T=1644229442:S=ALNI_MaX1nhLv-FNRn72YHGiF2TcEh-8Ng; g_state={"i_p":1644315850628,"i_l":2}; MSESID0=3197182125491119193630%2C0%2C0%2C0%2CZP6H71%2C0%2C5%2C71fcd63ff6c5dea10fba2f20ceed7052; BSESINFO0=98%2CX21V53%2C%2C; NaviPageUrl=https://www.bayt.com/en/saudi-arabia/jobs/legal-secretary-4485575/; _ga=GA1.1.772674273.1644193797; __atuvc=10%7C6; _iub_cs-29998284=%7B%22consent%22%3Atrue%2C%22timestamp%22%3A%222022-02-07T00%3A30%3A04.652Z%22%2C%22version%22%3A%221.36.1%22%2C%22id%22%3A%2229998284%22%7D; _ga_1NKPLGNKKD=GS1.1.1644238929.3.1.1644240400.0; _ga_ZJ86J4RMT9=GS1.1.1644238929.3.1.1644240400.60; _clsk=mn0etr|1644242593461|177|0|b.clarity.ms/collect'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    soup = BeautifulSoup(response.text, "html.parser")
    nodes = soup.select("#results_inner_card > ul > li > div > h2 > a")

    return response, nodes

def fetch(country, jobId):
    url = f"https://www.bayt.com/en/{country}/jobs/{jobId}"

    payload={}
    headers = {
      'authority': 'www.bayt.com',
      'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"macOS"',
      'upgrade-insecure-requests': '1',
      'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
      'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
      'sec-fetch-site': 'none',
      'sec-fetch-mode': 'navigate',
      'sec-fetch-user': '?1',
      'sec-fetch-dest': 'document',
      'accept-language': 'en-US,en;q=0.9,fa-IR;q=0.8,fa;q=0.7',
      'cookie': 'brID=3196817984777971951648; ISLOGGED0=0; user-prefs=locale%20xx%20lang%20en%20geo%20ch; _gid=GA1.2.2072758243.1644193798; _ym_uid=1638871593328733712; _ym_d=1644193798; _ym_isad=2; _clck=i2keey|1|eys|0; _gcl_au=1.1.107500405.1644193799; _pk_id.1.c132=794b8dc72ee6f9cf.1644193803.; aff_data={%22qs%22:%22%22%2C%22ref%22:%22https://www.bayt.com/%22}; JB_SRCH_TKN=%2FoPHbiNnW%2F8%3D1644194008; __gsas=ID=decd48a1e02b41aa:T=1644229442:S=ALNI_MaX1nhLv-FNRn72YHGiF2TcEh-8Ng; g_state={"i_p":1644315850628,"i_l":2}; MSESID0=3197182125491119193630%2C0%2C0%2C0%2CZP6H71%2C0%2C5%2C71fcd63ff6c5dea10fba2f20ceed7052; BSESINFO0=98%2CX21V53%2C%2C; _ym_visorc=b; _pk_ses.1.c132=1; NaviPageUrl=https://www.bayt.com/en/saudi-arabia/jobs/operations-coordinator-assistant-tabuk-4485600/; userJobId=4485600; _ga_1NKPLGNKKD=GS1.1.1644238929.3.1.1644239769.0; _clsk=mn0etr|1644239770176|171|0|b.clarity.ms/collect; _ga_ZJ86J4RMT9=GS1.1.1644238929.3.1.1644239772.21; _ga=GA1.1.772674273.1644193797; __atuvc=9%7C6; __atuvs=62011858e8a264ae006; _iub_cs-29998284=%7B%22consent%22%3Atrue%2C%22timestamp%22%3A%222022-02-07T00%3A30%3A04.652Z%22%2C%22version%22%3A%221.36.1%22%2C%22id%22%3A%2229998284%22%7D; userJobId=4282526'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    soup = BeautifulSoup(response.text, "html.parser")
    fields = soup.select('.h5')
    descriptions = soup.select('.h5 + * ')
    data = {field.text:desc.text for field,desc in zip(fields,descriptions)}
    data['title'] = soup.select('h1')[0].text.strip()
    return response, data

def main(country, pages):
    if os.path.isfile('jobs.jl'):
        df = pd.read_json('jobs.jl',lines=True)
        df['job-id'] = df['job-id'].astype('str')
        job_ids = set(df['job-id'].values)
    else:
        job_ids = set()
    with open('jobs.jl', 'a', encoding="utf-8") as f:
        for page in tqdm.tqdm(pages,total=len(pages)):
#            print(f'country = {country}, page = {page}, progress: {pagei}/{len(pages)}')
            response, jobs = search_jobs(country=country,page=page)
#            print(response.text)
#            print(jobs)
            for job in jobs: #tqdm.tqdm(jobs,total=len(jobs)): 
                try:
                    jobId = job.attrs['data-job-id']

                    if jobId in job_ids:
                        #print(f'job-id {jobId}: already crawled')
                        continue

                    response, data = fetch(country=country,jobId=jobId)
                    data['href'] = job.attrs['href']
                    data['job-id'] = job.attrs['data-job-id']
                    #print('#'*30)
                    #print(data)
                    data['raw'] = response.text
                    f.write(json.dumps(data,ensure_ascii=False)+'\n')
                    job_ids.add(jobId)
                except Exception: # skip jobs that are skipped 
                    pass


# example run: python bayt.py 1-10 saudi-arabia
if __name__=='__main__':
    pages = sys.argv[1]
    if ',' in pages:
        pages = [int(e) for e in pages.split(',')]
    elif '-' in pages:
        i,j = pages.split('-')[0], pages.split('-')[1]
        pages = list(range(int(i),int(j)+1))
    else:
        pages = [int(sys.argv[1])]
    country = sys.argv[2]

    print(country)
    print(pages)
    main(pages=pages, country=country)
