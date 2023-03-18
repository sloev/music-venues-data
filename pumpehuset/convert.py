import pandas as pd
import json

with open("pumehuset.jsonlines", "r") as fin:
    with open("pumpehuset.jsonlines", "w") as fout:
        for l in fin.readlines():
            d = json.loads(l)
            d["price"] = int(d["price"] or "-1")
            d["artist_name"] = d["artist_name"].strip()
            fout.write(json.dumps(d)+"\n")

with open("pumpehuset.jsonlines", "r") as fin:
    df = pd.read_json(fin.read(), lines=True)
    df.to_csv("pumpehuset.csv", sep='\t')

