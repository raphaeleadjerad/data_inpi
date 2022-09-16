
import os
import pandas as pd
import import_inpi as inpi
import transform_rcs as trcs
import export

inpi.import_inpi()

os.mkdir("data2017/")
os.chdir("data2017/")
start_url = "http://data.cquest.org/inpi_rncs/imr/stock/2017/"
inpi.find_links(start_url, list_urls=[])

os.chdir("../")


def import_all_years():
    df_rep_2017, df_pm_2017 = trcs.transform_rcs("2017")
    df_rep_2018, df_pm_2018 = trcs.transform_rcs("2018")
    df_rep_2019, df_pm_2019 = trcs.transform_rcs("2019")
    df_rep_2020, df_pm_2020 = trcs.transform_rcs("2020")
    df_rep = pd.concat([df_rep_2017, df_rep_2018, df_rep_2019, df_rep_2020])
    df_pm = pd.concat([df_pm_2017, df_pm_2018, df_pm_2019, df_pm_2020])
    print(df_rep.shape)
    print(df_pm.shape)
    return df_rep, df_pm


df_rep, df_pm = import_all_years()

export.export2minio("df_pm_all_years.csv", df_pm)
export.export2minio("df_rep_all_years.csv", df_rep)
