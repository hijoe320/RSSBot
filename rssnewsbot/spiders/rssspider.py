from time import sleep, gmtime, mktime
from datetime import datetime
import logging
import scrapy
import redis
import msgpack
import xxhash
import pymongo as pm
import feedparser as fp
from colorama import Back, Fore, Style
from ..settings import MONGODB_URI, REDIS_HOST, REDIS_PORT, REDIS_PWD, REDIS_PENDING_QUEUE


def hs(s):
    """
    hash function to convert url to fixed length hash code
    """
    return xxhash.xxh32(s).hexdigest()


def time2ts(time_struct):
    """
    convert time_struct to epoch
    """
    return mktime(time_struct)


class RSSSpider(scrapy.Spider):
    name = "rssspider"

    def __init__(self, *args, **kwargs):
        super(RSSSpider, self).__init__(*args, **kwargs)
        self.rc = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD)
        self.df = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD, db=REDIS_DUPFLT_DB)
        self.mc = pm.MongoClient(host=MONGODB_URI, connect=False)

    def start_requests(self):
        with self.mc.rssnews.feed.find() as cursor:
            logging.info("number of rss feeds = %d", cursor.count())
            for item in cursor:
                logging.debug("rss=%(url)s", item)
                yield scrapy.Request(url=item["url"], callback=self.parse, meta=item)

    def parse(self, res):
        logging.debug("%sparsing %s%s", Fore.GREEN, res.url, Style.RESET_ALL)
        rss = fp.parse(res.body)
        symbol = res.meta["symbol"]
        for e in rss.entries:
            if self.check_exist(e.link):
                continue
            if '*' in e.link:
                url = "http" + e.link.split("*http")[-1]
                self.append_task(e, url)
            elif e.link.startswith("http://finance.yahoo.com/r/"):
                yield scrapy.Request(url=e.link, callback=self.extract_url, meta=e)
            else:
                self.append_task(e, e.link)

    def extract_url(self, res):
        if res.body.startswith("<script src="):
            url = res.body.split("URL=\'")[-1].split("\'")[0]
            self.append_task(res.meta, url)
        else:
            pass

    def check_exist(self, url):
        return self.df.get(url)

    def append_task(self, entry, url):
        self.df.set(url, True, ex=3600)
        self.rc.append(PENDING_QUEUE, msgpack.packb(task))
