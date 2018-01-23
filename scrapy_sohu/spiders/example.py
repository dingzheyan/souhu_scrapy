# -*- coding: utf-8 -*-
import scrapy
import json
from  scrapy import Request
from datetime import datetime,timedelta
import time
from pyquery import PyQuery as pq
import pymysql
from  pymysql.cursors import DictCursor
import sys

sys.getdefaultencoding()
class ExampleSpider(scrapy.Spider):
    name = 'example'
    # 母婴类：sceneId=26
    start_urls = ['http://v2.sohu.com/public-api/feed?scene=CHANNEL&sceneId=26&page=1&size=1000',
                  'http://v2.sohu.com/public-api/feed?scene=CATEGORY&sceneId=28&page=1&size=300',
                  'http://v2.sohu.com/public-api/feed?scene=CATEGORY&sceneId=27&page=1&size=300',
                  'http://v2.sohu.com/public-api/feed?scene=CATEGORY&sceneId=32&page=1&size=300']

    # start_urls = ['http://v2.sohu.com/public-api/feed?scene=CATEGORY&sceneId=28&page=1&size=100']
    # 母婴类
    # http://v2.sohu.com/public-api/feed?scene=CHANNEL&sceneId=26&page=1&size=1000

    # 儿科
    # http://v2.sohu.com/public-api/feed?scene=CATEGORY&sceneId=28&page=2&size=20
    # http://v2.sohu.com/public-api/feed?scene=CATEGORY&sceneId=28&page=4&size=20&callback=jQuery112406857270306852496_1516715548703&_=1516715548788
    # http://v2.sohu.com/public-api/feed?scene=CATEGORY&sceneId=28&page=5&size=20&callback=jQuery112406857270306852496_1516715548711&_=1516715548807

    # 妇产科
    # http://v2.sohu.com/public-api/feed?scene=CATEGORY&sceneId=27&page=2&size=20

    # 营养科
    # http://v2.sohu.com/public-api/feed?scene=CATEGORY&sceneId=32&page=2&size=20

    def parse(self, response):
        articles = json.loads(response.body_as_unicode())
        now=str(datetime.now().date())
        now_zero=datetime.strptime(now,"%Y-%m-%d")
        timestamp0 = time.mktime(now_zero.timetuple())
        article_values=[]
        article_type=((response.request.url).split('&')[-3]).split('=')[-1]
        sceneId=((response.request.url).split('&')[-3]).split('=')[-1]
        articletable_name,readcount_table_name=self.get_table_name(sceneId)
        for ar in articles:
            article_id = ar['id']
            authorId = ar['authorId']
            publicTime = ar['publicTime']
            authorName = ar['authorName']
            mobileTitle = ar['mobileTitle']
            title = ar['title']
            detail_url = "http://www.sohu.com/a/{0}_{1}".format(article_id, authorId)
            read_num_url = "http://v2.sohu.com/public-api/articles/{0}/pv?articleType={1}&readcount_table_name={2}".format(article_id,article_type,readcount_table_name)
            if publicTime/1000>timestamp0:
                article_value=(article_id,authorId,int(publicTime/1000),authorName,mobileTitle,title,detail_url)
                article_values.append(article_value)
                yield scrapy.Request(read_num_url,
                                     callback=self.parse_detail)
        self.insert_into_souhu_article(article_values,articletable_name)


    def parse_detail(self, response):
        # http://v2.sohu.com/public-api/articles/217384389/pv?callback=jQuery112408445633826951557_1516284842296&_=1516284842297
        d=pq(response.text)
        readcount=d("p").text()
        url=response.request.url
        article_id=url.split('/')[-2]
        readcount_table_name=(url.split('&')[-1]).split('=')[-1]
        scrapy_time=int(time.time())
        ar_read_vs=[]
        ar_read_v=(article_id,readcount,scrapy_time)
        ar_read_vs.append(ar_read_v)
        self.insert_into_souhu_article_readcount(ar_read_vs,readcount_table_name)



    def insert_into_souhu_article(self,values,articletable_name):
        conn = pymysql.connect(host="127.0.0.1", user="root",
                                     password="123456", db="scrapy", port=3306,use_unicode=True, charset="utf8")
        cur = conn.cursor(DictCursor)
        try:
            '''爬虫结果'''
            sql="insert into " + articletable_name+"(article_id,authorId,publicTime,authorName,mobileTitle,title,detail_url) VALUES(%s,%s,%s,%s,%s,%s,%s)"
            cur.executemany(sql, values)
            conn.commit()
        except Exception as e:
            raise e
        finally:
            conn.close()
            cur.close()
    def insert_into_souhu_article_readcount(self,values,readcount_table_name):
        conn = pymysql.connect(host="127.0.0.1", user="root",
                                     password="123456", db="scrapy", port=3306,use_unicode=True, charset="utf8")
        cur = conn.cursor(DictCursor)
        try:
            '''爬虫结果'''
            sql="insert into "+readcount_table_name+"(article_id,readcount,scrapy_time)"\
                        "  VALUES(%s,%s,%s)"

            cur.executemany(sql, values)
            conn.commit()
        except Exception as e:
            raise e
        finally:
            conn.close()
            cur.close()

    def get_table_name(self,sceneId):
        conn = pymysql.connect(host="127.0.0.1", user="root",
                               password="123456", db="scrapy", port=3306, use_unicode=True, charset="utf8")
        cur = conn.cursor(DictCursor)
        try:
            '''爬虫结果'''

            cur.execute(
                "select * from article_table_setting "
                "  where sceneId={0}".format(sceneId))
            result=cur.fetchone()
            return result['articletable_name'],result['readcount_table_name']
        except Exception as e:
            raise e
        finally:
            conn.close()
            cur.close()