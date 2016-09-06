#coding=utf-8
import urllib2
import gzip
import StringIO
import ConfigParser

import time
from functools import wraps
import random

def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck, e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print msg
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


def get_content(toUrl,count):
    """ Return the content of given url

        Args:
            toUrl: aim url
            count: index of this connect

        Return:
            content if success
            'Fail' if fail
    """

    cf = ConfigParser.ConfigParser()
    cf.read("config.ini")
    cookie = cf.get("cookie", "cookie")

    headers = {
        'Cookie': cookie,
        'Host':'www.zhihu.com',
        'Referer':'http://www.zhihu.com/',
        'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
        # 'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36',
        'Accept-Encoding':'gzip'
    }

    req = urllib2.Request(
        url = toUrl,
        headers = headers
    )

    proxy_all = [
    "10.25.170.247:5678",
    "10.25.171.82:5678",
    "10.47.114.111:5678",
    "10.47.54.77:5678",
    "10.25.60.218:5678",
    "10.47.54.180:5678",
    "10.47.54.115:5678",
    "10.47.106.138:5678"
    ]
    current_proxy = random.choice(proxy_all)
    try:
        opener = urllib2.build_opener(urllib2.ProxyHandler({"http" : current_proxy}))  #urllib2.ProxyHandler()
        urllib2.install_opener(opener)
        page = urllib2.urlopen(req,timeout = 15)
        headers = page.info()
        content = page.read()
    # except Exception,e:
    #     if count % 1 == 0:
    #         print str(count) + ", Error: " + str(e) + " URL: " + toUrl
    #     return "FAIL"
    except urllib2.HTTPError, e:
        if e.code == 404:
            if count % 1 == 0:
                print str(count) + ", Error: " + str(e) + " URL: " + toUrl
            return "NO FOUND"
        else:
            try:
                page = urlopen_with_retry(req, proxy_all)
		headers = page.info()
                content = page.read()
            except Exception,e:
                if count % 1 == 0:
                    print str(count) + ", Error: " + str(e) + " URL: " + toUrl + "retry_fail"
                return "FAIL"

    # except urllib2.URLError, e:
    except Exception, e:
        print e
        try:
            page = urlopen_with_retry(req, proxy_all)
            headers = page.info()
            content = page.read()
        except Exception,e:
            if count % 1 == 0:
                print str(count) + ", Error: " + str(e) + " URL: " + toUrl + "retry_fail"
            return "FAIL"

    if page.info().get('Content-Encoding') == 'gzip':
        data = StringIO.StringIO(content)
        gz = gzip.GzipFile(fileobj=data)
        content = gz.read()
        gz.close()

    return content

@retry(Exception, tries=5, delay=10, backoff=2)
def urlopen_with_retry(req, proxy_all):
    current_proxy = random.choice(proxy_all)
    opener = urllib2.build_opener(urllib2.ProxyHandler({"http" : current_proxy}))  #urllib2.ProxyHandler()
    urllib2.install_opener(opener)
    return urllib2.urlopen(req)
