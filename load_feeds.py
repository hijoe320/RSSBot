import os
import logging
from argparse import ArgumentParser, ArgumentTypeError
import pymongo


def is_file(fn):
    if os.path.isfile(fn):
        return fn
    else:
        raise ArgumentTypeError("file does not exist!")


if __name__ == "__main__":
    ap = ArgumentParser()
    ap.add_argument("--mongodb-uri", default="mongodb://localhost:27017", type=str)
    ap.add_argument("--drop-existing", action="store_true")
    ap.add_argument("-f", "--fname", type=is_file, action="append")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    mc = pymongo.MongoClient(host=args.mongodb_uri)
    if args.drop_existing:
        logging.info("drop existing collection 'feed'")
        mc.rssnews.feed.drop()

    for fname in args.fname:
        logging.info("processing file %s", fname)
        with open(fname, 'r') as f:
            for line in f.readlines():
                symbol, company = line.strip().split('\t')
                logging.info("adding %s, %s", symbol, company)
                mc.rssnews.feed.insert({
                    "symbol": symbol,
                    "company": company,
                    "url": "http://finance.yahoo.com/rss/headline?s={0}".format(symbol)
                })
        mc.close()
        logging.info("done")
