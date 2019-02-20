import pandas as pd

data_raw = pd.read_csv("G:\\0.Download\\a53af1dac7f4f9ea_csv\\Origin_VIX.csv")

data_raw.Date = pd.to_datetime(data_raw.Date).dt.strftime('%Y%m%d')
data_raw.to_csv("VIX.csv")