
from argparse import ArgumentParser
from time import mktime, sleep
from multiprocessing import Pool, Process
import logging
import urlparse
import feedparser as fp
import pymongo as pm
import redis
import msgpack
import xxhash
from redlock import RedLock


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


def extract_url(url):
    """
    extract the real url from yahoo rss feed item
    """
    if '*' in url:
        _url = "http" + url.split("*http")[-1]
    else:
        _url = url
    return "{0}://{1}{2}".format(*urlparse.urlparse(_url))


if __name__ == "__main__":
    ap = ArgumentParser(description=None)
    ap.add_argument("--mongodb-uri", type=str, default="mongodb://localhost:27017")
    ap.add_argument("--redis-host", type=str, default="localhost")
    ap.add_argument("--redis-port", default=6379, type=int)
    ap.add_argument("--redis-pwd", default=None, type=str)
    ap.add_argument("--mode", default="one", choices=["each", "all"])
    ap.add_argument("--procs", default=4, type=int)
    ap.add_argument("--update-interval", type=int, default=60)
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-8s %(message)s")

    mc = pm.MongoClient(host=args.mongodb_uri)
    rc = redis.Redis(host=args.redis_host, port=args.redis_port, password=args.redis_pwd, db=0)
    df = redis.Redis(host=args.redis_host, port=args.redis_port, password=args.redis_pwd, db=1)
    lock = RedLock([{"host": args.redis_host, "port": args.redis_port, "db": 3}])

    logging.info("building filter ...")
    with mc.rssnews.news.find({}, {"uuid": True}) as cursor:
        for news_item in cursor:
            if df.scard(news_item["uuid"]) == 0:
                for sym in news_item["symbols"]:
                    df.sadd(news_item["uuid"], sym)
        logging.info("%d existing urls in total.", df.dbsize())

    logging.info("generating tasks ...")
    with mc.rssnews.feed.find() as cursor:
        logging.info("number of rss feeds = %d", cursor.count())
        tasks = []
        for item in cursor:
            logging.debug("rss=%(url)s", item)
            t = [len(tasks), item["_id"], item["symbol"], item["url"], item.get("updated", 0)]
            tasks.append(t)

    mc.close()

    def process(task, mongodb_cli=None):
        """
        Core process function to parse rss single feed and extract feed items
        only new item will be pushed into the pending queue for spider to download.
        """
        tid, _id, symbol, rss_url, rss_updated = task
        logging.debug("processing tid=%03d, _id=%s, sym=%5s, rss_url=%s, updated=%d", tid, _id, symbol, rss_url, rss_updated)
        rss = fp.parse(rss_url)
        nb_new_items = 0
        for e in rss.entries:
            url = extract_url(e.link)
            uuid = hs(url)
            lock.acquire()
            if df.scard(uuid) == 0:
                logging.debug("add to pending queue: sym=%5s, uuid=%s, url=%s", symbol, uuid, url)
                df.sadd(uuid, symbol)
                lock.release()
                published = e.get("published_parsed", None)
                if published:
                    published = time2ts(published)
                entry = {
                    "uuid": uuid,
                    "title": e.title,
                    "link": e.link,
                    "url": url,
                    "published": published,
                    "symbols": [symbol]
                }
                mp = msgpack.packb(entry)
                rc.lpush("pending", mp)
                rc.publish("news_"+symbol, mp)
                nb_new_items += 1
            else:
                if not df.sismember(uuid, symbol):
                    df.sadd(uuid, symbol)
                    mongodb_cli.rssnews.news.update_one({"uuid":uuid}, {"$addToSet": {"symbols": symbol}})
                    nb_new_items += 1
                lock.release()
        if nb_new_items > 0:
            if hasattr(rss.feed, "updated_parsed"):
                updated = time2ts(rss.feed.updated_parsed)
                if mongodb_cli is None:
                    temp_mc = pm.MongoClient(args.mongodb_uri)
                else:
                    temp_mc = mongodb_cli
                temp_mc.rssnews.feed.find_one_and_update(
                    {"_id": _id},
                    {"$push": {"updated_timestamps": updated}, "$set": {"updated": updated}}
                )
                if mongodb_cli is None:
                    temp_mc.close()
        return nb_new_items

    class FeedWorker(object):
        def __init__(self):
            self.mc = pm.MongoClient(args.mongodb_uri)
            self.cmd = None

        def __call__(self, task):
            symbol = task[2]
            while True:
                self.cmd = rc.get("feed_updater")
                if self.cmd == "start":
                    nb_new_items = process(task, self.mc)
                    if nb_new_items > 0:
                        logging.info("added %d news urls to %s", nb_new_items, symbol)
                elif self.cmd == "stop":
                    logging.info("%s process stopped", symbol)
                    break
                if args.update_interval > 1:
                    logging.debug("%s process sleep for %d seconds", symbol, args.update_interval)
                    sleep(args.update_interval)

    if args.mode == "each":     # each rss feed has its own process
        procs = [Process(target=FeedWorker(), args=(t,)) for t in tasks]
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()
    elif args.mode == "all":    # all rss feeds are processed by a pool of workers
        logging.info("use %d processes", args.procs)
        while True:
            cmd = rc.get("feed_updater")
            if cmd == "start":
                if args.procs > 1:
                    pool = Pool(args.procs)
                    nb_new = sum(pool.map(process, tasks))
                else:
                    nb_new = sum([process(t) for t in tasks])
                if nb_new > 0:
                    logging.info("added %d new items", nb_new)
            elif cmd == "stop":
                logging.info("updater stopped")
                break
            else:
                logging.info("change value of 'feed_updater' to 'start' to start updating feeds.")
            if args.update_interval > 1:
                logging.info("wait for %d seconds", args.update_interval)
                sleep(args.update_interval)
