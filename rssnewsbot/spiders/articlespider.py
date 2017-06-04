from time import sleep, gmtime, mktime
from collections import defaultdict
from datetime import datetime
import zlib
import operator
import logging
import scrapy
import redis
import msgpack
import pymongo as pm
from colorama import Back, Fore, Style
from bs4 import BeautifulSoup, Comment, Doctype, CData, ProcessingInstruction, Declaration, Tag
from ..settings import MONGODB_URI, REDIS_HOST, REDIS_PORT, REDIS_PWD, PENDING_QUEUE


def extract_content(res):
    page = res.body
    noisy_elms = [Comment, Doctype, CData, ProcessingInstruction, Declaration]
    noisy_tags = ["script", "style", "img", "iframe", "select"]
    text_tags = ['p', "span", "ul", "td"]

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
            if soup.footer:
                soup.footer.extract()
        except e:
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
    if len(container_wcount) > 0:
        # Select the container that has biggest word count
        content_container = max(container_wcount.iteritems(), key=operator.itemgetter(1))[0]
        return container_text[content_container]
    else:
        return None


class ArticleSpider(scrapy.Spider):
    name = "articlespider"

    def __init__(self, *args, **kwargs):
        super(ArticleSpider, self).__init__(*args, **kwargs)
        if (REDIS_HOST == "localhost") and (REDIS_PORT == 6379):
            self.rc = redis.Redis()
        else:
            self.rc = redis.Redis()
        self.mc = pm.MongoClient(host=MONGODB_URI)

    def start_requests(self):
        while True:
            cmd = self.rc.get("article_spider")
            if cmd == "start":
                feed_item = msgpack.unpackb(self.rc.brpop(PENDING_QUEUE, 50)[1])
                if feed_item is None:
                    sleep(5)
                    continue
                url = feed_item["url"]
                req = scrapy.Request(url=url, meta=feed_item)
                req.headers["User-Agent"] = "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; en-gb) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5"
                yield scrapy.Request(url=url)
            elif cmd == "stop":
                logging.debug("%sarticle spider stopped%s", Fore.RED, Style.RESET_ALL)
                break
            else:
                logging.debug("%swaiting for cmd, set key 'article_spider' to 'start' or 'stop'%s", Fore.GREEN, Style.RESET_ALL)
                sleep(10)

    def parse(self, res):
        logging.debug("%sparsing %s%s", Fore.LIGHTBLACK_EX, res.url, Style.RESET_ALL)
        feed_item = res.meta
        feed_item["published_dt"] = datetime.fromtimestamp(feed_item["published"])
        self.parse_page(res, feed_item)

    def parse_page(self, res, feed_item):
        feed_item["url"] = res.url
        feed_item["content"] = extract_content(res) or None
        feed_item["compressed_html"] = res.body
        self.update_db(feed_item)

    def update_db(self, feed_item):
        feed_item["parsed"] = mktime(gmtime())
        feed_item["parsed_dt"] = datetime.fromtimestamp(feed_item["parsed"])
        _id = self.mc.rssnews.news.insert_one(feed_item)
        logging.debug("%sparsed %s, mongodb _id=%s%s", Back.GREEN, feed_item["url"], _id, Style.RESET_ALL)
        if feed_item["content"] is not None:
            self.rc.lpush("nlp", str(_id))
        else:
            logging.warning("%sfail to extract content, url=%s%s", Back.RED, feed_item["url"], Style.RESET_ALL)
