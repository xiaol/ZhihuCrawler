# ZhihuCrawler
zhihu.com的爬虫，通过话题id抓取话题下的问题id，再通过问题id抓取相应的答案。

依赖的软件：

Python 2.7

MySQL
依赖的python库：

BeautifulSoup

Requests

Html2Text

Termcolor

Lxml

Rq

运行步骤：

1、下载代码（git clone https://github.com/xiaol/ZhihuCrawler）

2、运行init.sql 建立数据库及相应表,插入TOPIC表的数据。

3、修改config.ini配置文件（info代表知乎的账号及密码，db代表mysql的host、port等信息）

4、运行python auth.py，登陆知乎

5、运行python findQuestionByTopic.py  通过话题id寻找该话题下面有哪些问题，并将数据存储在QUESTION表中

6、运行 python queueAnswer.py 构建分布式队列，通过该队列寻找问题的答案。

7、运行 python findAnswerWorker.py 执行队列任务，获取问题答案，将答案存储在ANSWER表中

8、运行 python updateQuestion.py 对QUESTION表及TOPIC表进行更新

提醒：

python findAnswerWorker.py任务可能会由于cookie失效而报错，报错后，请重新执行一次 python auth.py对cookie进行更新
