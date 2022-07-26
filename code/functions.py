import os
import glob
from zipfile import ZipFile
import shutil
import tarfile
import requests
import pandas as pd
import s3fs


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
            temp_df = [open_complex_file(zip_file, text_file.filename)
                for text_file in zip_file.infolist()
                if text_file.filename.endswith('.csv')]
            list_df.append(temp_df)
    return list_df


def import_all_rep(path2data, extension="*.csv"):
    """_summary_

    Args:
        path (_type_): _description_
    """
    all_files = [file for path, subdir, files in os.walk(path2data) for \
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
            temp_df = [open_complex_file(zip_file, text_file.filename)
                for text_file in zip_file.infolist()
                if text_file.filename.endswith('5_rep.csv')]
            temp_df = pd.concat(temp_df)
            list_df.append(temp_df)
    return list_df


def import_all_pm(path2data, extension="*.csv"):
    """_summary_

    Args:
        path (_type_): _description_
    """
    all_files = [file for path, subdir, files in os.walk(path2data) for \
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
            temp_df = [open_complex_file(zip_file, text_file.filename)
                for text_file in zip_file.infolist()
                if text_file.filename.endswith('_PM.csv')]
            temp_df = pd.concat(temp_df)
            list_df.append(temp_df)
    return list_df


def transform_rcs(year):
    if year in ["2019", "2020"]:
        list_df = import_all_files("data" + year + "/", "*.zip")
    if year in ["2018"]:
        list_df = import_all_files("data" + year + "/", "*.csv")
    print(len(list_df))
    if year in ["2019", "2020"]:
        li = []
        for i in range(len(list_df)):
            temp = pd.concat(list_df[i])
            li.append(temp)
        df_final = pd.concat(li)
    if year in ["2018"]:
        df_final = pd.concat(list_df)
    print(df_final.shape)
    return df_final


def export_2_minio(year, df_final):
    S3_ENDPOINT_URL = "https://" + os.environ["AWS_S3_ENDPOINT"]
    fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': S3_ENDPOINT_URL})
    BUCKET_OUT = "radjerad"
    FILE_KEY_OUT_S3 = "inpi/inpi_" + year + ".csv"
    FILE_PATH_OUT_S3 = BUCKET_OUT + "/" + FILE_KEY_OUT_S3

    with fs.open(FILE_PATH_OUT_S3, 'w') as file_out:
        df_final.to_csv(file_out)
    return None
