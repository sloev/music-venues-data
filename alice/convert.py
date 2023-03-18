import pandas as pd

with open("alice.jsonlines", "r") as f:
    df = pd.read_json(f.read(), lines=True)
    df.to_csv("alice.csv", sep='\t')

