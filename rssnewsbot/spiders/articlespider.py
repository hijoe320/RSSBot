from time import sleep, gmtime, mktime
from collections import defaultdict
from datetime import datetime
import zlib
import urlparse
import operator
import logging
import scrapy
import redis
import msgpack
import pymongo as pm
from bs4 import BeautifulSoup, Comment, Doctype, CData, ProcessingInstruction, Declaration, Tag
from ..settings import MONGODB_URI, REDIS_HOST, REDIS_PORT, REDIS_PWD, PENDING_QUEUE


def extract_content(page):
    noisy_elms = [Comment, Doctype, CData, ProcessingInstruction, Declaration]
    noisy_tags = ["script", "style", "img", "iframe", "select"]
    text_tags = ['p', "span", "ul", "td", "li"]

    def clean_texttag(soup_texttag):
        for child in soup_texttag.contents:
            if isinstance(child, Tag):
                if "onmouseover" in child.attrs:
                    child.extract()
        return soup_texttag

    def remove_empty_tags(soup):
        if isinstance(soup, Tag):
            if len(soup.contents) > 0:
                for child in soup.contents:
                    if isinstance(child, Tag):
                        remove_empty_tags(child)
            if len(soup.contents) == 0:
                soup.extract()
        return soup

    def prune(soup):
        # remove noisy tags
        for e in soup(noisy_tags):
            e.extract()

        # remove noisy elements
        for noisy_elm in noisy_elms:
            elms = soup.findAll(e=lambda e: isinstance(e, noisy_elm))
            for e in elms:
                e.extract()

        # remove <head>
        soup.head.extract()

        # remove empty tags
        soup = remove_empty_tags(soup)

        # remove footer
        try:
            soup.footer.extract()
        except:
            pass
        for div in soup.select("#footer"):
            div.extract()
        for div in soup.select(".footer"):
            div.extract()
        return soup

    # Get containers that contain text tags,
    # build two dicts of them:
    # k: container, v: text
    # k: container, v: word count
    soup = prune(BeautifulSoup(page, 'lxml'))
    target_tags = soup.find_all(text_tags)
    container_text = defaultdict(lambda: '')
    container_wcount = defaultdict(int)
    for text_tag in target_tags:
        #texttag = self.clean_texttag(texttag)
        text = text_tag.get_text()
        wcount = len(text.split())
        container_text[text_tag.parent] += text_tag.get_text(separator=' ', strip=True) + '\n'
        container_wcount[text_tag.parent] += wcount

    # Select the container that has biggest word count
    content_container = max(container_wcount.iteritems(), key=operator.itemgetter(1))[0]
    return container_text[content_container]


class ArticleSpider(scrapy.Spider):
    name = "articlespider"

    def __init__(self, *args, **kwargs):
        super(ArticleSpider, self).__init__(*args, **kwargs)
        self.rc = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD)
        self.mc = pm.MongoClient(host=MONGODB_URI)
        self.feed_item = None

    def start_requests(self):
        cmd = self.rc.get("article_spider")
        while True:
            if cmd == "start":
                self.feed_item = msgpack.unpackb(self.rc.brpop(PENDING_QUEUE))
                yield scrapy.Request(url=self.feed_item["url"])
            elif cmd == "stop":
                logging.debug("article stopped")
                break
            else:
                logging.debug("waiting for cmd, set key 'article_spider' to 'start' or 'stop'")
                sleep(10)

    def parse(self, res):
        self.feed_item["content"] = extract_content(res.body)
        self.feed_item["source"] = urlparse.urlparse(res.url).netloc
        self.feed_item["compressed_html"] = zlib.compress(res.body)
        self.feed_item["published_dt"] = datetime.fromtimestamp(self.feed_item["published"])
        self.feed_item["parsed"] = mktime(gmtime())
        self.feed_item["parsed_dt"] = datetime.fromtimestamp(self.feed_item["parsed"])
        _id = self.mc.rssnews.news.insert_one(self.feed_item)
        self.rc.lpush("nlp", str(_id))
        logging.debug("parsed %s, mongodb _id=%s", self.feed_item["url"], _id)
