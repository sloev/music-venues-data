import pandas as pd

with open("vega.jsonlines", "r") as f:
    vega = pd.read_json(f.read(), lines=True)


with open("pricing_data.jsonlines", "r") as f:
    pricing = pd.read_json(f.read(), lines=True)


df = pd.concat([vega.set_index('concert_id'),pricing.set_index('concert_id')], axis=1, join='inner')

df.to_csv("vega.csv", sep='\t')

