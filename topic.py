#coding=utf-8
import MySQLdb
from bs4 import BeautifulSoup
import json
import re
import time
from math import ceil
import logging
import threading
import Queue
import ConfigParser

# from util import get_content
# from test import question_test
from zhihu import Question
from zhihu import Answer

cf = ConfigParser.ConfigParser()
cf.read("config.ini")

host = cf.get("db", "host")
port = int(cf.get("db", "port"))
user = cf.get("db", "user")
passwd = cf.get("db", "passwd")
db_name = cf.get("db", "db")
charset = cf.get("db", "charset")
use_unicode = cf.get("db", "use_unicode")
db = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db_name, charset=charset, use_unicode=use_unicode)
cursor = db.cursor()


class UpdateOneTopic(threading.Thread):
    def __init__(self,queue):
        self.queue = queue
        threading.Thread.__init__(self)

        cf = ConfigParser.ConfigParser()
        cf.read("config.ini")
        
        host = cf.get("db", "host")
        port = int(cf.get("db", "port"))
        user = cf.get("db", "user")
        passwd = cf.get("db", "passwd")
        db_name = cf.get("db", "db")
        charset = cf.get("db", "charset")
        use_unicode = cf.get("db", "use_unicode")

        self.db = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db_name, charset=charset, use_unicode=use_unicode)
        self.cursor = self.db.cursor()
        
    def run(self):
        while not self.queue.empty():
            t = self.queue.get()
            link_id = t[0]
            count_id = t[1]
            self.find_new_answer_by_question(link_id,count_id)

    def question_test(self, url):
        question = Question(url)

        # 获取该问题的标题
        title = question.get_title()
        # 获取该问题的详细描述
        detail = question.get_detail()
        # 获取回答个数
        # answers_num = question.get_answers_num()
        # 获取关注该问题的人数
        # followers_num = question.get_followers_num()
        # 获取该问题所属话题
        topics = question.get_topics()
        # 获取该问题被浏览次数
        # visit_times = question.get_visit_times()
        # 获取排名第一的回答
        # top_answer = question.get_top_answer()
        # 获取排名前十的十个回答
        top_answers = question.get_top_i_answers(10)
        # 获取所有回答
        answers = question.get_all_answers()

        # print title  # 输出：现实可以有多美好？
        # print detail
        # 输出：
        # 本问题相对于“现实可以多残酷？传送门：现实可以有多残酷？
        # 题主：       昨天看了“现实可以有多残酷“。感觉不太好，所以我
        # 开了这个问题以相对应，希望能够“中和一下“。和那个问题题主不想
        # 把它变成“比惨大会“一样，我也不想把这个变成“鸡汤故事会“，或者
        # 是“晒幸福“比赛。所以大家从“现实，实际”的角度出发，讲述自己的
        # 美好故事，让大家看看社会的冷和暖，能更加辨证地看待世界，是此
        # 题和彼题共同的“心愿“吧。
        # print answers_num  # 输出：2441
        # print followers_num  # 输出：26910
        # for topic in topics:
            # print type(topic)
            # print topic,  # 输出：情感克制 现实 社会 个人经历
        # print visit_times  # 输出: 该问题当前被浏览的次数
        # print top_answer  # 输出：<zhihu.Answer instance at 0x7f8b6582d0e0>(Answer类对象)
        # print top_answers  # 输出：<generator object get_top_i_answers at 0x7fed676eb320>(代表前十的Answer的生成器)
        # print answers  # 输出：<generator object get_all_answer at 0x7f8b66ba30a0>(代表所有Answer的生成器)
        answers_list = []
        for answer in answers: #top_answers
            answers_list.append(answer.to_txt())
        if len(answers_list) >0:
            answers = []
            topics = json.dumps(topics).decode('unicode-escape').encode('utf8')
            time_now = int(time.time())
            p_str = 'INSERT IGNORE INTO ANSWER (QURL, TITLE, DETAIL, TOPICS, AURL, USERID, CONTENT, UPVOTE, USERURL, ADD_TIME, LAST_VISIT) VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s)'
            for answer in answers_list:
                answers = answers + [(url, title, detail, topics,  answer["aurl"], answer["userid"], answer["content"], answer["upvote"], answer["userurl"], time_now, 0)]
            self.cursor.executemany(p_str, answers)
            return self.cursor.rowcount
        else:
            print "no answer"
            return 0


    def find_new_answer_by_question(self,link_id,count_id):
        new_question_amount_total = 0
        question_url = "http://www.zhihu.com/question/" + str(link_id)
        print question_url + "," + "start" + self.getName()
        try:
        # if 1==1:
            new_question_amount_one_page = self.question_test(question_url)
            if new_question_amount_one_page > 0:
                sql = "UPDATE QUESTION SET IS_FLAG = 1 WHERE LINK_ID = %s"
                self.cursor.execute(sql, (link_id, ))
                #self.db.commit()
            else:
                print question_url + "fail"
        except Exception ,e:
            print e
        #     print "question_url_error"



        # if new_question_amount_one_page <= 1:
            # break

        # if count_id % 2 == 0:
        #     print str(count_id) + " , " + self.getName() + " Finshed TOPIC " + link_id + ", page " + str(i) + " ; Add " + str(new_question_amount_total) + " questions."
        # print str(count_id) + " , " + self.getName() + " Finshed TOPIC " + link_id + ", page " + str(i) + " ; Add " + str(new_question_amount_total) + " questions." + str(new_question_amount_one_page) +"last_questions_num"


def find_new_answer_by_question(link_id):
    new_question_amount_total = 0
    question_url = "http://www.zhihu.com/question/" + str(link_id)
    print question_url + "," + "start" #+ self.getName()
    try:
    # if 1==1:
        new_question_amount_one_page = question_test(question_url)
        if new_question_amount_one_page > 0:
            sql = "UPDATE QUESTION SET IS_FLAG = 1 WHERE LINK_ID = %s"
            cursor.execute(sql, (link_id, ))
            db.commit()
        else:
            print question_url + "fail"
    except Exception ,e:
        print e

def question_test(url):
    question = Question(url)
    # 获取该问题的标题
    title = question.get_title()
    # 获取该问题的详细描述
    detail = question.get_detail()
    # 获取回答个数
    # answers_num = question.get_answers_num()
    # 获取关注该问题的人数
    # followers_num = question.get_followers_num()
    # 获取该问题所属话题
    topics = question.get_topics()
    # 获取该问题被浏览次数
    # visit_times = question.get_visit_times()
    # 获取排名第一的回答
    # top_answer = question.get_top_answer()
    # 获取排名前十的十个回答
    top_answers = question.get_top_i_answers(10)
    # 获取所有回答
    answers = question.get_all_answers()
    # print title  # 输出：现实可以有多美好？
    # print detail
    # 输出：
    # 本问题相对于“现实可以多残酷？传送门：现实可以有多残酷？
    # 题主：       昨天看了“现实可以有多残酷“。感觉不太好，所以我
    # 开了这个问题以相对应，希望能够“中和一下“。和那个问题题主不想
    # 把它变成“比惨大会“一样，我也不想把这个变成“鸡汤故事会“，或者
    # 是“晒幸福“比赛。所以大家从“现实，实际”的角度出发，讲述自己的
    # 美好故事，让大家看看社会的冷和暖，能更加辨证地看待世界，是此
    # 题和彼题共同的“心愿“吧。
    # print answers_num  # 输出：2441
    # print followers_num  # 输出：26910
    # for topic in topics:
        # print type(topic)
        # print topic,  # 输出：情感克制 现实 社会 个人经历
    # print visit_times  # 输出: 该问题当前被浏览的次数
    # print top_answer  # 输出：<zhihu.Answer instance at 0x7f8b6582d0e0>(Answer类对象)
    # print top_answers  # 输出：<generator object get_top_i_answers at 0x7fed676eb320>(代表前十的Answer的生成器)
    # print answers  # 输出：<generator object get_all_answer at 0x7f8b66ba30a0>(代表所有Answer的生成器)
    answers_list = []
    # a = 0
    for answer in answers: #top_answers
        # a=a +1
        # if a>=2:
        #     break
        answers_list.append(answer.to_txt())
    if len(answers_list) >0:
        answers = []
        topics = json.dumps(topics).decode('unicode-escape').encode('utf8')
        time_now = int(time.time())
        p_str = 'INSERT IGNORE INTO ANSWER (QURL, TITLE, DETAIL, TOPICS, AURL, USERID, CONTENT, UPVOTE, USERURL, ADD_TIME, LAST_VISIT) VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s)'
        for answer in answers_list:
            answers = answers + [(url, title, detail, topics,  answer["aurl"], answer["userid"], answer["content"], answer["upvote"], answer["userurl"], time_now, 0)]
        cursor.executemany(p_str, answers)
        return cursor.rowcount
    else:
        print "no answer"
        return 0



class UpdateTopics:
    def __init__(self):
        cf = ConfigParser.ConfigParser()
        cf.read("config.ini")
        
        host = cf.get("db", "host")
        port = int(cf.get("db", "port"))
        user = cf.get("db", "user")
        passwd = cf.get("db", "passwd")
        db_name = cf.get("db", "db")
        charset = cf.get("db", "charset")
        use_unicode = cf.get("db", "use_unicode")

        self.topic_thread_amount = int(cf.get("topic_thread_amount","topic_thread_amount"))

        self.db = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db_name, charset=charset, use_unicode=use_unicode)
        self.cursor = self.db.cursor()

    def run(self):
        time_now = int(time.time())
        before_last_vist_time = time_now - 10

        queue = Queue.Queue()
        threads = []

        i = 0

        sql = "SELECT LINK_ID FROM QUESTION WHERE  ANSWER>=1 AND IS_FLAG=0"  #LINK_ID ='44811567'  LAST_VISIT < %s and ORDER BY LAST_VISIT and LINK_ID = 19680895 and TOPIC_URL like 'http://www.zhihu.com/topic/19552320/questions?page=255'
        self.cursor.execute(sql)   #, (before_last_vist_time,)
        results = self.cursor.fetchall()
        # results = []
        # another = 49024731
        for row in results:
            link_id = str(row[0])
            queue.put([link_id, i])
            i = i + 1
        # queue.put([another,-1])
        for i in range(self.topic_thread_amount):
            threads.append(UpdateOneTopic(queue))

        for i in range(self.topic_thread_amount):
            threads[i].start()

        for i in range(self.topic_thread_amount):
            threads[i].join()

        self.db.close()

        print 'All task done'

if __name__ == '__main__':
    topic_spider = UpdateTopics()

    topic_spider.run()

