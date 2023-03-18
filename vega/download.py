#!/usr/bin/env python

import cdx_toolkit
import io
import gzip
import lxml.html
import json
import tqdm
from pathlib import Path
import logging

cdx = cdx_toolkit.CDXFetcher(source='ia')
url = 'vega.dk/kalender/*'

warcinfo = {
    'software': 'pypi_cdx_toolkit iter-and-warc example',
    'isPartOf': 'EXAMPLE-COMMONCRAWL',
    'description': 'warc extraction',
    'format': 'WARC file version 1.0',
}

TOTAL_RESULTS = cdx.get_size_estimate(url)

seen_concert_ids = set()

from pathlib import Path

Path('pricing_data.jsonlines').touch()

with open("pricing_data.jsonlines", "r") as jsonfile:
    for line in jsonfile.readlines():
        d = json.loads(line)
        seen_concert_ids.add(d["concert_id"])

with open("pricing_data.jsonlines", "a") as jsonfile:
    with tqdm.tqdm(total=TOTAL_RESULTS) as pbar:
        for obj in cdx.iter(url, filter=["status:200"]):
            pbar.update(1)
            pbar.refresh()
            url = obj['url']
            if "?" in url:
                print("ignoring url: ", url)
                continue

          
            try:
                tokens =Path(url.split("kalender/", 1)[1]).parts
                concert_id = tokens[0]
                if len(tokens)>1:
                    logging.warning("skipping uninteresting url: ", url)
                    continue

            except:
                logging.exception("error getting concert_id, url:",url)
                continue

            if concert_id in seen_concert_ids:
                continue
            
            status = obj['status']
            timestamp = obj['timestamp']

            if status != '200':
                logging.error(' skipping because status was {}, not 200'.format(status))
                continue

            try:
                record = obj.fetch_warc_record()
            except RuntimeError:
                print(' skipping capture for RuntimeError 404: %s %s', url, timestamp)
                continue
       

            html = record.raw_stream.read()

            def error(str):
                logging.exception(str)
                with open(f"error_html/{concert_id}.html", "wb") as f:
                    f.write(html)

            price_tag = html.split(b'<p class="sticky-card__price">')
            if not len(price_tag)>1:
                error(f"could not extract price for {url}")
                continue
            price = price_tag[1].split(b",", 1)[0].split(b",")[0].strip()
            outdata = {
                "url": url,
                "price": int(price),
                "concert_id": concert_id
            }
            jsonfile.write(json.dumps(outdata) + "\n")
            jsonfile.flush()
            seen_concert_ids.add(concert_id)
