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
from bs4 import BeautifulSoup
import re

cdx = cdx_toolkit.CDXFetcher(source="ia")
url = "alicecph.com/event/*"

warcinfo = {
    "software": "pypi_cdx_toolkit iter-and-warc example",
    "isPartOf": "EXAMPLE-COMMONCRAWL",
    "description": "warc extraction",
    "format": "WARC file version 1.0",
}

TOTAL_RESULTS = cdx.get_size_estimate(url)

seen_concert_ids = set()

from pathlib import Path

Path("alice.jsonlines").touch()

with open("alice.jsonlines", "r") as jsonfile:
    for line in jsonfile.readlines():
        d = json.loads(line)
        seen_concert_ids.add(d["concert_id"])

objs = []
with tqdm.tqdm(total=TOTAL_RESULTS) as pbar:
    for obj in cdx.iter(url, filter=["status:200"]):
        pbar.update(1)
        pbar.refresh()
        objs.append(obj)

with open("alice.jsonlines", "a") as jsonfile:
    with tqdm.tqdm(total=TOTAL_RESULTS) as pbar:
        for obj in reversed(objs):
            pbar.update(1)
            pbar.refresh()
            url = obj["url"]
            if "?" in url:
                print("ignoring url: ", url)
                continue

            try:
                concert_id = Path(url.split("/event/", 1)[1]).parts[0]
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
                logging.exception(str + f"for url: {url}")
                with open(f"error_html/.{concert_id}.html", "wb") as f:
                    f.write(html)


            
            soup = BeautifulSoup(html, 'html.parser')

            price = None
            start_date = None

            if start_date is None:
                e = soup.find_all("div", attrs={"data-component": "EventInfo"})
                if e:
                    e = e[0]
                    text = " ".join([i.text for i in e.findChildren()[1:]])
                  
                    _, start_date, rest = text.split(" ", 2)




                # if e:
                #     event_info = e[0]
            if start_date is None:
                e = soup.find_all("div", attrs={"data-component": "EventContent"})
                if e:
                    e = e[0]
                    start_date = e.select_one("h1").text.split()[-1]
            try:
                html_string = html.decode().lower()
            except:
                html_string = str(html).lower()

            for dkk_start in [m.start() for m in re.finditer('dkk',html_string)]:
                found_numbers = re.findall(r'\d+', html_string[dkk_start-10:dkk_start])
                if found_numbers:
                    price = found_numbers[0]
                    break
            
            if not start_date or not price:
                error("error")
                continue

            artist_name = html_string.split("<title>", 1)[1].split("</title>", 1)[0].strip().lower()
            if artist_name.endswith("- alice"):
                artist_name = artist_name.replace("- alice", "")
            elif artist_name.endswith("&mdash; alice"):
                artist_name = artist_name.replace("&mdash; alice", "")

            day,month,year = start_date.strip().split(".")
            start_date =  f"{year}-{month}-{day}T20:00:00"



            genres = ""
            ticket_status_raw = "køb billet"
            try:
                ticket_status_raw = html_string.split("eventimage--status", 1)[1].split("</small", 1)[0].rsplit(">", 1)[1].strip().lower()
            except:
                try:
                    ticket_status_raw = html_string.split("eventticketbutton", 1)[1].split("</a", 1)[0].rsplit(">", 1)[1].strip().lower()
                except:
                    if "køb billet" in ticket_status_raw:
                        ticket_status_raw = "køb billet"
                    else:
                        error("error getting ticket_status")

            status_lookup = {
                "aflyst":"cancelled",
                "udsolgt":"soldout",
                "flyttet": "moved",
                "køb": "available"
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
                "artist_name": artist_name,
                "doors_open": start_date,
                "venue": "alice",
                "support": "",
                "ticket_status":ticket_status,
                f"{genres}": 1,
            }

            print(ticket_status)
            jsonfile.write(json.dumps(outdata, ensure_ascii=False) + "\n")
            jsonfile.flush()
            seen_concert_ids.add(concert_id)
