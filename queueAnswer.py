#coding=utf-8
__author__ = 'yangjiwen'
import time
import ConfigParser
import MySQLdb
import Queue
from redis import Redis

from rq import use_connection, Queue as redis_Queue
from topic import find_new_answer_by_question
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import datetime
import logging

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


if __name__ == "__main__":
    while True:
        t00 = datetime.datetime.now()
        t00 = t00.strftime("%Y-%m-%d %H:%M:%S")
        logging.warn("===============this round of question start====================%s"%(t00))
        redis_conn = Redis('121.41.75.213','6379')
        q = redis_Queue(connection=redis_conn)
        if (len(q)==0):
            print ("start_task")
            time_now = int(time.time())
            before_last_visit_time = time_now - 3600*12*0
            queue = Queue.Queue()
            threads = []
            i = 0
            after_add_time = time_now - 24*3600*145
            sql = "SELECT LINK_ID FROM QUESTION WHERE  LAST_VISIT < %s AND ADD_TIME > %s AND ANSWER>=1 AND IS_FLAG=0 ORDER BY LAST_VISIT"  #LINK_ID ='44811567'  LAST_VISIT < %s and ORDER BY LAST_VISIT and LINK_ID = 19680895 and TOPIC_URL like 'http://www.zhihu.com/topic/19552320/questions?page=255'
            cursor.execute(sql, (before_last_visit_time, after_add_time))   #, (before_last_vist_time,)
            results = cursor.fetchall()
            links = []
            redis_conn = Redis('121.41.75.213','6379')
            q = redis_Queue(connection=redis_conn)
            for row in results:
                link_id = str(row[0])
                links.append(link_id)
                i = i + 1
                result  = q.enqueue(find_new_answer_by_question, link_id, timeout=6000)
        time.sleep(600)    
