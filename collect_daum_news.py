import requests
from bs4 import BeautifulSoup
import json
import re
import sys
import time, random
import pandas as pd
import pprint
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
from multiprocessing import Pool
from datetime import date


    
def get_daum_comment(turl):

    req = requests.get(turl)
    cont = req.content
    soup = BeautifulSoup(cont, 'html.parser')

    area = soup.find_all("div", "alex-area")[0]
    client_id = area['data-client-id']
     
    url2 = "https://comment.daum.net/oauth/token?grant_type=alex_credentials&client_id="+client_id
    req = requests.get(url2, headers={"Referer": turl})
    auth = req.text

    header2 = { "Authorization":"Bearer "+json.loads(auth)['access_token']}
    post_id = turl.split('/')[-1]
    turl = "http://comment.daum.net/apis/v1/posts/@"+ post_id
    tar = requests.get(turl, headers = header2)

    limit = json.loads(tar.text)['commentCount']
    commentId = json.loads(tar.text)['id']
    parentId = 0
    sort='favorite'
    durl = "http://comment.daum.net/apis/v1/posts/"+str(commentId)+"/comments?parentId="+str(parentId)+"&offset=0&limit="+str(limit)+"&sort="+sort
    dat = requests.get(durl)
    dat = json.loads(dat.text)
    data = json_normalize(dat)[['content','createdAt', 'likeCount', 'dislikeCount']]


    return data
    

def search_daum_news(query, s_date, e_date):

    s_from = s_date.replace(".","")
    e_to = e_date.replace(".","")
    
    page = 1
    df = pd.DataFrame(columns = ['pdate', 'articleTitle', 'article', 'pcompany', 'url'])


    while True:
       
        print(page)
        
        'https://search.daum.net/search?w=news&q='+query+'&sd='+s_date + '000000&ed='+e_date+'235959'
        'https://search.daum.net/search?nil_suggest=btn&w=news&DA=STC&cluster=y&q='+query+'&sd='+s_date+'000000&ed=20191011'+e_date+'&period=u'
        url = "https://search.naver.com/search.naver?where=news&query=" + query + "&sort=1&ds=" + s_date + "&de=" + e_date + "&nso=so%3Ar%2Cp%3Afrom" + s_from + "to" + e_to + "%2Ca%3A&start=" + str(page)
        #header 추가
        header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        }
        req = requests.get(url,headers=header)
        print(url)
        cont = req.content
        soup = BeautifulSoup(cont, 'html.parser')
            #print(soup)
            
        if len(soup.select("._sp_each_url")) ==0:
            break
        
        news_list = [i["href"] for i in soup.select("._sp_each_url") if i["href"].startswith("https://news.naver.com")]
        with Pool(10) as p:
            get_news_list = p.map(get_news_df, news_list)

        df = df.append(pd.concat(get_news_list))
        
    #     for urls in soup.select("._sp_each_url"):
    #         try :
    #             #print(urls["href"])
    #             if urls["href"].startswith("https://news.naver.com"):
    #                 #print(urls["href"])
    #                 news_detail = get_news(urls["href"])
    #                 #print(news_detail)
    #                 #'pdate', 'articleTitle', 'article', 'pcompany'
    #                 df = df.append(pd.DataFrame([[news_detail[0], news_detail[1], news_detail[2], news_detail[3], news_detail[4]]],                                            columns=['pdate', 'articleTitle', 'article', 'pcompany', 'url']), ignore_index=True)
                    
    #                 # pdate, pcompany, title, btext
    #                 #f.write("{}\t{}\t{}\t{}\n".format(news_detail[0], news_detail[3], news_detail[1], news_detail[2]))  # new style
                    
    #         except Exception as e:
    #             print(e) 
    #             continue

        page += 10  
        
    df = df.drop_duplicates(subset='url')  # 중복 기사 제거
        
    return df
    
#https://search.daum.net/search?w=news&q=%EC%9C%A0%ED%8A%9C%EB%B8%8C

