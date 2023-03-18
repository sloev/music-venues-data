import requests
import time
import logging
log = logging.warning
import json
import tqdm
import urllib3
from pathlib import Path
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def retried_get(
    *args, retries=5, **kwargs,
):
    try:
        return requests.get(*args, **kwargs, verify=False)
    except requests.exceptions.ConnectionError:
        if not retries:
            raise
    except requests.exceptions.SSLError:
        if not retries:
            raise
    except requests.exceptions.ChunkedEncodingError:
        if not retries:
            raise
        
    sleep_s = 6 - retries
    log(f"got ssl error, sleeping: {sleep_s}s (retries left: {retries})")
    time.sleep(sleep_s)
    retries -= 1
    return retried_get(*args, retries=retries, **kwargs)

seen_concert_ids = set()

with open("vega.jsonlines", "w") as f:
    page = 1
    totalpages = 100000000000
    with tqdm.tqdm() as pbar:
        while page <= totalpages:
            url = f"https://www.vega.dk/umbraco/api/EventApi/List?culture=da&date=%7B%7D&includeArchived=true&itemsPerPage=100&pageNumber={page}&searchTerm=&siteNodeId=1117&skipPrevious=true"
            resp = retried_get(url)
            data = resp.json()
            totalpages = data["TotalPages"]
            pbar.total = totalpages

            

            for item in data["Items"]:
                url = item["Url"]
                try:
                    concert_id =Path(url.split("koncertarkiv/", 1)[1]).parts[0]
                except:
                    print("error getting concert_id", url)
                    continue
                if concert_id in seen_concert_ids:
                    print("skipping seen concert_id",concert_id)
                    continue
                ticket_status_raw = (item["TicketStatus"] or {}).get("StatusClass") or ""

                status_lookup = {
                    "postponed":"postponed",
                    "free-entrance":"free_entrance",
                    "cancelled":"cancelled",
                    "sold-out":"soldout",
                    "moved": "moved",
                    "buy": "available",
                    "few-tickets":"few_left",
                    "waiting-list":"waiting_list"
                }
                ticket_status = "available"
                for k,v in status_lookup.items():
                    if k in ticket_status_raw:
                        ticket_status = v
                        break
                try:
                    data = {
                        "url": url,
                        "concert_id": concert_id,
                        "artist_name": item["Name"],
                        "doors_open": item["Start"],
                        "venue": item["VenueName"],
                        "support": item["Support"],
                        "ticket_status": ticket_status,
                        **dict([(g.lower(), 1) for g in item["GenreNames"]])
                    }
                    f.write(json.dumps(data)+"\n")
                    seen_concert_ids.add(concert_id)
                except:
                    print(item)
                    print("error")
                    exit()

            
            page += 1
            pbar.update(1)
            pbar.refresh()

    