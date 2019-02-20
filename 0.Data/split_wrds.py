import pandas as pd

data_raw = pd.read_csv("G:\\0.Download\\a392882e8ab976b7_csv\\Option_wrds.csv")

for i in range(0, 18):
    if i == 0:
        n_year_start = 0
    else:
        n_year_start = (2000 + i) * 10000

    n_year_end = (2000 + i + 1) * 10000

    d_temp = data_raw[(data_raw.last_date >= n_year_start) & (data_raw.last_date < n_year_end)]

    s_file_name = "Option_" + str(2000 + i) + ".csv"
    d_temp.to_csv(s_file_name)
    print(s_file_name + " DONE !")

