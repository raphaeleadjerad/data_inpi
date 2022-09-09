import os
import glob
from zipfile import ZipFile
import pandas as pd


def _open_complex_file(zip_file, fi):
    df = pd.read_csv(zip_file.open(fi), sep=";")
    df["source"] = fi
    return df


def _import_all_files(path2data, type_table, extension="*.csv"):
    """_summary_

    Args:
        path (_type_): _description_
    """
    all_files = [file for path, subdir, files in os.walk(path2data) for\
                    file in glob.glob(os.path.join(path, extension))]
    # print(all_files)
    list_df = []
    for filename in all_files:

        filesize = os.path.getsize(filename)
        if filesize != 0 and extension == "*.csv":
            temp_df = pd.read_csv(filename, sep=";")
            temp_df["file_path"] = filename
            list_df.append(temp_df)

        if filesize != 0 and extension == "*.zip":
            zip_file = ZipFile(filename)         
            temp_df = [_open_complex_file(zip_file, text_file.filename)
                for text_file in zip_file.infolist()
                if text_file.filename.endswith(type_table)]
            list_df.append(temp_df)
    return list_df


def transform_rcs(year):
    if year in ["2019", "2020"]:
        list_df_rep = _import_all_files("data" + year + "/",
                                        '5_rep.csv', "*.zip")
        list_df_pm = _import_all_files("data" + year + "/", '_PM.csv', "*.zip")
    # if year in ["2018"]:
    #  list_df = _import_all_files("data" + year + "/", "*.csv")
    print(len(list_df_rep))
    print(len(list_df_pm))
    if year in ["2019", "2020"]:
        li = []
        for i in range(len(list_df_rep)):
            temp = pd.concat(list_df_rep[i])
            li.append(temp)
        df_final_rep = pd.concat(li)
        for i in range(len(list_df_pm)):
            temp = pd.concat(list_df_pm[i])
            li.append(temp)
        df_final_pm = pd.concat(li)
    # if year in ["2018"]:
    #   df_final = pd.concat(list_df)
    print(df_final_rep.shape)
    print(df_final_pm.shape)
    societes_rep_tc = pd.merge(df_final_pm, df_final_rep,\
        on=["Siren", "Code Greffe"], how="left")
    print(societes_rep_tc.shape)
    return societes_rep_tc

