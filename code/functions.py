import requests
import tarfile
import os
import glob
import pandas as pd
from zipfile import ZipFile

def download_tgz_raw():
    """_summary_

    Returns:
        _type_: _description_
    """
    for i in ["2018", "2019", "2020"]:
        url = 'http://data.cquest.org/inpi_rncs/imr/stock/" + i + ".tgz'
        target_path = i + '.tgz'

        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(target_path, 'wb') as f:
                f.write(response.raw.read())
    return None


def import_tgz(year, out_path):
    """_summary_

    Args:
        year (_type_): _description_
        out_path (_type_): _description_

    Returns:
        _type_: _description_
    """
    tar = tarfile.open(year + ".tgz", "r:gz", encoding='utf-8')
    tar.extractall(out_path)
    return None


def import_all_csv(path2data):
    """_summary_

    Args:
        path (_type_): _description_
    """

    extension = "*.csv"
    all_csv_files = [file for path, subdir, files in os.walk(path2data) for \
                    file in glob.glob(os.path.join(path, extension))]
    list_df = []
    for filename in all_csv_files:
        # print(filename)
        filesize = os.path.getsize(filename)
        if filesize != 0:
            temp_df = pd.read_csv(filename, sep=";")
            temp_df["file_path"] = filename
            list_df.append(temp_df)


def open_complex_file(zip_file, fi):
    df = pd.read_csv(zip_file.open(fi), sep=";")
    df["source"] = fi
    return df


def import_all_files(path2data, extension="*.csv"):
    """_summary_

    Args:
        path (_type_): _description_
    """
    all_files = [file for path, subdir, files in os.walk(path2data) for \
                    file in glob(os.path.join(path, extension))]
    #print(all_files)
    list_df = []
    for filename in all_files:

        filesize = os.path.getsize(filename)
        if filesize != 0 and extension == "*.csv":
            temp_df = pd.read_csv(filename, sep=";")
            temp_df["file_path"] = filename
            list_df.append(temp_df)

        if filesize != 0 and extension == "*.zip":
            zip_file = ZipFile(filename)         
            temp_df = [open_complex_file(zip_file, text_file.filename)
                for text_file in zip_file.infolist()
                if text_file.filename.endswith('.csv')]
            list_df.append(temp_df)
    return list_df