import beers
import pandas
df = pandas.read_csv("t.csv.gz")

a  = beers.check_df(df, "test.yaml", "dataset", True)

print(a.total)