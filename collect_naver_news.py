
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
    print('news_url : ', n_url)
    breq = requests.get(n_url)
    bsoup = BeautifulSoup(breq.content, 'html.parser')
    
    # 날짜 파싱
    pdate = bsoup.select('.t11')[0].get_text()[:11]
    # 기사 제목
    title = bsoup.select('h3#articleTitle')[0].text
    # 기사 본문 크롤링
    _text = bsoup.select('#articleBodyContents')[0].get_text().replace('\n', " ")
    btext = _text.replace("// flash 오류를 우회하기 위한 함수 추가 function _flash_removeCallback() {}", "")
    btext = btext.strip()
    # 신문사 크롤링
    try:
        pcompany = bsoup.select('#footer address')[0].a.get_text()
    except:
        pcompany = ''
    #url
    #분류명
    aclass = bsoup.find_all('em', {'class':'guide_categorization_item'})[0].get_text()
    
    #'pdate', 'articleTitle', 'article', 'pcompany', 'url'
    return pdate, title, btext, pcompany, n_url, aclass
    
def get_news_df(n_url):
    print('news_url : ', n_url)
    breq = requests.get(n_url)
    bsoup = BeautifulSoup(breq.content, 'html.parser')
    
    # 날짜 파싱
    pdate = bsoup.select('.t11')[0].get_text()[:11]

    # 기사 제목
    title = bsoup.select('h3#articleTitle')[0].text
    
    # 기사 본문 크롤링 
    _text = bsoup.select('#articleBodyContents')[0].get_text().replace('\n', " ")
    btext = _text.replace("// flash 오류를 우회하기 위한 함수 추가 function _flash_removeCallback() {}", "")
    btext = btext.strip()
    # 신문사 크롤링
    try:
        pcompany = bsoup.select('#footer address')[0].a.get_text()
    except:
        pcompany = ''
    
    #url
    aclass = bsoup.find_all('em', {'class':'guide_categorization_item'})[0].get_text()
    
    return pd.DataFrame([[pdate, title, btext, pcompany, n_url, aclass]],
                        columns=['pdate', 'articleTitle', 'article', 'pcompany', 'url', 'aclass'])
                        
def search_naver_news(query, s_date, e_date):

    s_from = s_date.replace(".","")
    e_to = e_date.replace(".","")
    page = 1
    df = pd.DataFrame(columns = ['pdate', 'articleTitle', 'article', 'pcompany', 'url', 'aclass'])


    while True:
       
        print(page)
        
        url = "https://search.naver.com/search.naver?where=news&query=" + query + "&sort=1&ds=" + s_date + "&de=" + e_date + "&nso=so%3Ar%2Cp%3Afrom" + s_from + "to" + e_to + "%2Ca%3A&start=" + str(page)
        #header 추가
        header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        }
        req = requests.get(url)#,headers=header)
        print(url)
        cont = req.content
        soup = BeautifulSoup(cont, 'html.parser')
            #print(soup)
            
        if len(soup.select("._sp_each_url")) ==0:
            break
        
        news_list = [i["href"] for i in soup.select("._sp_each_url") if i["href"].startswith("https://news.naver.com")]
        with Pool(10) as p:
            get_news_list = p.map(get_news_df, news_list)

        df = df.append(pd.concat(get_news_list),ignore_index=True)
        
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


def get_naver_comment(url_i):
    columns = ['pdate', 'articleTitle', 'article', 'pcompany', 'url', 'aclass' ,'comment', 'sympathyCount', 'antipathyCount']

    comment_df = pd.DataFrame(columns = columns)

    oid=url_i.split("oid=")[1].split("&")[0]
    aid=url_i.split("aid=")[1]
    page=1    
    header = {
        "User-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36",
        "referer":url_i,
    } 
    
    pdate, articleTitle, article, pcompany, url, aclass = get_news(url_i)
    
    
    while True :
        c_url="https://apis.naver.com/commentBox/cbox/web_neo_list_jsonp.json?ticket=news&templateId=default_society&pool=cbox5&_callback=jQuery1707138182064460843_1523512042464&lang=ko&country=&objectId=news"+oid+"%2C"+aid+"&categoryId=&pageSize=20&indexSize=10&groupId=&listType=OBJECT&pageType=more&page="+str(page)+"&refresh=false&sort=FAVORITE" 
    # 파싱하는 단계입니다.
        r=requests.get(c_url,headers=header)
        cont=BeautifulSoup(r.content,"html.parser")    
        total_comm=str(cont).split('comment":')[1].split(",")[0]

        match=re.findall('"contents":([^\*]*),"userIdNo"', str(cont))
        sympathyall = re.findall('"sympathyCount":([^\*]*),"antipathyCount"', str(cont))
        antipathyall = re.findall('"antipathyCount":([^\*]*),"userBlind"', str(cont))
        
    #df에 댓글과 뉴스 저장
        for j in range(len(match)):
            comment_df = comment_df.append(pd.DataFrame([[pdate, articleTitle, article, pcompany, url, aclass, match[j], sympathyall[j],  antipathyall[j]]],
                                                         columns=columns), ignore_index=True)
    # 한번에 댓글이 20개씩 보이기 때문에 한 페이지씩 몽땅 댓글을 긁어 옵니다.
        if int(total_comm) <= ((page) * 20):
            break
        else : 
            page+=1
            
    #comment_df = comment_df.drop_duplicates(subset='url')  # 중복 기사 제거

    return comment_df
    
def get_naver_comment_list(url_l):
    columns = ['pdate', 'articleTitle', 'article', 'pcompany', 'url', 'aclass', 'comment', 'sympathyCount', 'antipathyCount']
    df = pd.DataFrame(columns = columns)
    with Pool(10) as p:
        get_news_list = p.map(get_naver_comment, url_l)
    df = df.append(pd.concat(get_news_list),ignore_index=True)
        
    return df
    


def get_rank_new(cdate):
    cdate = str(cdate).replace(".","")
    
    rank_type = ['ranking_100', 'ranking_101', 'ranking_102', 'ranking_103', 'ranking_104', 'ranking_105']
    rank_news_title = [0] * len(rank_type)
    default_url = 'https://news.naver.com'
    columns =  ['blind', 'href', 'title']
    ranknews_df = pd.DataFrame(columns=columns)

    day_url = 'https://news.naver.com/main/ranking/popularDay.nhn?rankingType=popular_day&date='+cdate
    memo_url = 'https://news.naver.com/main/ranking/popularMemo.nhn?rankingType=popular_memo&date='+cdate
    url = [day_url, memo_url]
    for turl in url:
        r=requests.get(memo_url)
        cont=BeautifulSoup(r.content,"html.parser")    

        

        for i in range(len(rank_type)):
            rank_news_title[i] = cont.find('div', id=rank_type[i])
            ranking = BeautifulSoup(str(rank_news_title[i]),"html.parser")
            blind = ranking.find('h5').text
           
            for i in range(5):
                href = default_url+ranking.find_all("a")[i]['href']
                title = ranking.find_all("a")[i]['title']
                
                ranknews_df = ranknews_df.append(pd.DataFrame([[blind, href,  title]],
                                                                 columns=columns), ignore_index=True)
    
    return ranknews_df
    
def get_week_rank(cdate = date.today()):
    cdate = str(cdate).replace("-", '')
    columns =  ['blind', 'href', 'title']
    df = pd.DataFrame(columns=columns)
    day_list = list(range(int(cdate)-6, int(cdate)+1))
    
    with Pool(7) as p:
        get_news_list = p.map(get_rank_new, day_list)

    df = df.append(pd.concat(get_news_list), ignore_index=True)

    return df
    
def get_week_rank_all(cdate = date.today()):
    df = get_week_rank(cdate)
    df2 = pd.DataFrame(columns = ['pdate', 'articleTitle', 'article', 'pcompany', 'url', 'aclass'])

    with Pool(10) as p:
        get_week_news_list = p.map(get_news_df, list(df['href']))
    df2 = df2.append(pd.concat(get_week_news_list),ignore_index=True)

    return df2



    
    


