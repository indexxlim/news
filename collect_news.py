
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



def clean_text(text):
    '''기사 내용 전처리 함수
    Args:
        - text: str 형태의 텍스트
    Return:
        - text: 전처리된 텍스트'''
    # Common
    # E-mail 제거#
    text = re.sub('([\w\d.]+@[\w\d.]+)', '', text)
    text = re.sub('([\w\d.]+@)', '', text)
    # 괄호 안 제거#
    text = re.sub("<[\w\s\d‘’=/·~:&,`]+>", "", text)
    text = re.sub("\([\w\s\d‘’=/·~:&,`]+\)", "", text)
    text = re.sub("\[[\w\s\d‘’=/·~:&,`]+\]", "", text)
    text = re.sub("【[\w\s\d‘’=/·~:&,`]+】", "", text)
    # 전화번호 제거#
    text = re.sub("(\d{2,3})-(\d{3,4}-\d{4})", "", text)  # 전화번호
    text = re.sub("(\d{3,4}-\d{4})", "", text)  # 전화번호
    # 홈페이지 주소 제거#
    text = re.sub('(www.\w.+)', '', text)
    text = re.sub('(.\w+.com)', '', text)
    text = re.sub('(.\w+.co.kr)', '', text)
    text = re.sub('(.\w+.go.kr)', '', text)
    # 기자 이름 제거#
    text = re.sub("/\w+[=·\w@]+\w+\s[=·\w@]+", "", text)
    text = re.sub("\w{2,4}\s기자", "", text)
    # 한자 제거#
    text = re.sub('[\u2E80-\u2EFF\u3400-\u4DBF\u4E00-\u9FBF\uF900]+', '', text)
    # 특수기호 제거#
    text = re.sub("[◇#/▶▲◆■●△①②③★○◎▽=▷☞◀ⓒ□?㈜♠☎]", "", text)
    # 따옴표 제거#
    text = re.sub("[\"\'”“‘’]", "", text)
    # 2안_숫자제거#
    # text = regex.sub('[0-9]+',"",text)
    
    # 날짜 제거    #2017-04-13 16:57:15
    text = re.sub("(\d{4})-(\d{2})-(\d{2})", "", text)  # 전화번호 + 날짜
    text = re.sub("(\d{2}):(\d{2}):(\d{2})","",text) #시간
    
    
    
    return text
    
    
def get_news(n_url):
    news_detail = []
    print('news_url : ', n_url)
    breq = requests.get(n_url)
    bsoup = BeautifulSoup(breq.content, 'html.parser')
    
    # 날짜 파싱
    pdate = bsoup.select('.t11')[0].get_text()[:11]
    news_detail.append(pdate)

    # 기사 제목
    title = bsoup.select('h3#articleTitle')[0].text
    news_detail.append(title)

    
    # 기사 본문 크롤링 
    _text = bsoup.select('#articleBodyContents')[0].get_text().replace('\n', " ")
    btext = _text.replace("// flash 오류를 우회하기 위한 함수 추가 function _flash_removeCallback() {}", "")
    news_detail.append(btext.strip())

    # 신문사 크롤링
    try:
        pcompany = bsoup.select('#footer address')[0].a.get_text()
    except:
        pcompany = ''
    news_detail.append(pcompany)
    
    #url
    news_detail.append(n_url)
    
    return pd.DataFrame([[news_detail[0], news_detail[1], news_detail[2], news_detail[3], news_detail[4]]],
                        columns=['pdate', 'articleTitle', 'article', 'pcompany', 'url'])
                        
def search_naver_news(query, s_date, e_date):

    s_from = s_date.replace(".","")
    e_to = e_date.replace(".","")
    page = 1
    df = pd.DataFrame(columns = ['pdate', 'articleTitle', 'article', 'pcompany', 'url'])


    while True:
       
        print(page)
        
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
            get_news_list = p.map(get_news, news_list)

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



