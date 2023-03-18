#!/usr/bin/env python

import cdx_toolkit
import io
import gzip
import lxml.html
from lxml import etree
import json
import tqdm
from pathlib import Path
import logging
import re

cdx = cdx_toolkit.CDXFetcher(source="ia")
url = "pumpehuset.dk/koncerter/*"

warcinfo = {
    "software": "pypi_cdx_toolkit iter-and-warc example",
    "isPartOf": "EXAMPLE-COMMONCRAWL",
    "description": "warc extraction",
    "format": "WARC file version 1.0",
}

TOTAL_RESULTS = cdx.get_size_estimate(url)

seen_concert_ids = set()

from pathlib import Path

Path("pumehuset.jsonlines").touch()

with open("pumehuset.jsonlines", "r") as jsonfile:
    for line in jsonfile.readlines():
        d = json.loads(line)
        seen_concert_ids.add(d["concert_id"])


objs = []
with tqdm.tqdm(total=TOTAL_RESULTS) as pbar:
    for obj in cdx.iter(url, filter=["status:200"]):
        pbar.update(1)
        pbar.refresh()
        objs.append(obj)

with open("pumehuset.jsonlines", "a") as jsonfile:
    with tqdm.tqdm(total=TOTAL_RESULTS) as pbar:
        for obj in reversed(objs):
            pbar.update(1)
            pbar.refresh()
            url = obj["url"]
            if "?" in url:
                print("ignoring url: ", url)
                continue

            try:
                concert_id = Path(url.split("koncerter/", 1)[1]).parts[0]
            except:
                print("error getting concert_id, url:", url)
                continue

            if concert_id in seen_concert_ids:
                continue

            status = obj["status"]
            timestamp = obj["timestamp"]

         

            if status != "200":
                print(" skipping because status was {}, not 200".format(status))
                continue

            try:
                record = obj.fetch_warc_record()
            except RuntimeError:
                print(" skipping capture for RuntimeError 404: %s %s", url, timestamp)
                continue
            # with open("out.html", "wb") as f:
            #     f.write(record.raw_stream.read())

            html = record.raw_stream.read()
            def error(str):
                logging.exception(str)
                with open(f"error_html/{concert_id}.html", "wb") as f:
                    f.write(html)

            try:
                root = lxml.html.fromstring(html)
            except:
                logging.warning(f"no html in {url}")
                continue

            def price_1():
                patterns = ["Pris kr.", "Price in DKK", "Pris i kr."]
                for p in patterns:
                    try:
                        price = html.decode().split(p, 1)[1].split("<", 1)[0]
                        price = re.findall(r'\d+', price)[0]
                        return price
                    except:
                        logging.error(f"price_1, for pattern:{p}")

            def price_2():
                if b"Fri entr" in html or b"Gratis entr" in html:
                    return 0
            
            

            for f in [price_1, price_2]:
                price = f()
                if price is not None:
                    break
            if price is None:
                error(f"error extracting price from")

            try:
                start = html.decode().split('datetime="', 1)[1].split('"', 1)[0]
                start = f"{start}T20:00:00"
            except:
                error(f"error extracting start date")

            try:
                artist_name = html.decode().split("<time class", 1)[0].rsplit('<', 1)[0].rsplit('>', 1)[1].lower()

            except:
                error(f"error extracting artist_name")

           
            genres = ""
            ticket_status_raw = "køb billet"
            try:
                ticket_status_raw = root.xpath(
                    '//*[@id="main-sidebar"]/div/section[1]/div[2]/p'
                )[0].text.lower()
            except:
                pass

            try:
                metadata = root.xpath(
                    '//*[@id="main-sidebar"]/div/section[1]/div[2]/a[1]'
                )[0]
                metadata_string = etree.tostring(metadata, pretty_print=True)
                genres = (
                    metadata_string.split(b'genres="')[1]
                    .split(b'"', 1)[0]
                    .decode()
                    .lower()
                )

            except:
                pass
            status_lookup = {
                "aflyst":"cancelled",
                "udsolgt":"soldout",
                "flyttet": "moved",
                "køb": "available",
                "få tilbage":"few_left"
            }
            ticket_status = "available"
            for k,v in status_lookup.items():
                if k in ticket_status_raw:
                    ticket_status = v
                    break
            print(f"ticket_status for url: {url}, status: {ticket_status_raw}, {ticket_status}")

            outdata = {
                "url": url,
                "price": int(price),
                "concert_id": concert_id,
                "artist_name": artist_name.strip(),
                "doors_open": start,
                "venue": "pumpehuset",
                "support": "",
                "ticket_status": ticket_status,
                f"{genres}": 1,
            }

            print(ticket_status)
            jsonfile.write(json.dumps(outdata, ensure_ascii=False) + "\n")
            jsonfile.flush()
            seen_concert_ids.add(concert_id)
