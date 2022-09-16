"""Module doctstring

    Returns:
        _type_: _description_
    """
import os
import glob
from zipfile import ZipFile
import pandas as pd


def _open_complex_file(zip_file, myfile):
    """Function that opens up a zip file and reads it

    Args:
        zip_file (_type_): _description_
        fi (_type_): _description_

    Returns:
        _type_: _description_
    """
    datainpi = pd.read_csv(zip_file.open(myfile), sep=";")
    datainpi["source"] = myfile
    return datainpi


def _import_all_files(path2data, type_table, extension="*.csv"):
    """Function that imports all files from directories and subdirectories
    with specific extension (and specific name specified in type_table)

    Args:
        path (_type_): _description_
    """
    all_files = [file for path, subdir, files in os.walk(path2data) for\
                file in glob.glob(os.path.join(path, extension))]

    list_df = []
    for filename in all_files:
        filesize = os.path.getsize(filename)
        if filesize != 0 and extension == "*.csv" and\
                filename.endswith(type_table):
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
    """Function that transforms original series of csv and zip files
    from Inpi into two single data frames (representants and enterprises)

    Args:
        year (_type_): _description_

    Returns:
        _type_: _description_
    """
    if year in ["2017"]:
        list_df_rep = _import_all_files("/", '5_rep.csv', "*.zip")
        list_df_pm = _import_all_files("/", '_PM.csv', "*.zip")
        list_df_rep = [list_df_rep[i][0] for i in range(134)]
        list_df_pm = [list_df_pm[i][0] for i in range(134)]
    if year in ["2019", "2020"]:
        list_df_rep = _import_all_files("data" + year + "/",
                                        '5_rep.csv', "*.zip")
        list_df_pm = _import_all_files("data" + year + "/", '_PM.csv', "*.zip")
    if year in ["2018"]:
        list_df_rep = _import_all_files("data" + year + "/",
                                        '5_rep.csv', "*.csv")
        list_df_pm = _import_all_files("data" + year + "/", '_PM.csv', "*.csv")
        
    print("Taille des tables")
    print(len(list_df_rep))
    print(len(list_df_pm))
    if year in ["2019", "2020"]:
        list_to_concat = []
        for i in range(len(list_df_rep)):
            temp = pd.concat(list_df_rep[i])
            list_to_concat.append(temp)
        df_final_rep = pd.concat(list_to_concat)
        list_to_concat = []
        for i in range(len(list_df_pm)):
            temp = pd.concat(list_df_pm[i])
            list_to_concat.append(temp)
        df_final_pm = pd.concat(list_to_concat)
    if year in ["2018", "2017"]:
        df_final_rep = pd.concat(list_df_rep)
        df_final_pm = pd.concat(list_df_pm)
    print("taille des tables")
    print(df_final_rep.shape)
    print(df_final_pm.shape)
    return df_final_rep, df_final_pm
