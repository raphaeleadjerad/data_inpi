"""This module defines functions to import data from INPI source
"""

import os
import tarfile
import s3fs
import pandas as pd
import requests
from bs4 import BeautifulSoup


def _download_tgz_raw():
    """ Function that downloads raw gzip files from INPI

    Returns:
        None: function that writes the gzip file in environment but returns
        None
    """
    for i in ["2018", "2019", "2020"]:
        url = 'http://data.cquest.org/inpi_rncs/imr/stock/' + i + '.tgz'
        target_path = i + '.tgz'

        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(target_path, 'wb') as temp:
                temp.write(response.raw.read())
        else:
            print("Erreur lecture de fichier")


def _import_tgz(year, out_path):
    """Function that extracts all files from gzip INPI file

    Args:
        year (str): year of the gzip file to treat
        out_path (str): folder to store the extracted files in

    Returns:
        _type_: _description_
    """
    tar = tarfile.open(year + ".tgz", "r:gz", encoding='utf-8')
    tar.extractall(out_path)


def import_inpi():
    """Function that applies download gzip raw files and then extract all
     files from them for all years
    """
    _download_tgz_raw()
    for year in ["2018", "2019", "2020"]:
        _import_tgz(year, "./data" + year)


def _is_directory(url):
    """Function to determine if a link is a directory or not

    Args:
        url (_type_): _description_

    Returns:
        _type_: _description_
    """
    if(url.endswith('/') and ".." not in url):
        return True
    else:
        return False


def find_links(url, list_urls):
    """Function that explores all sublinks of an url to download
    every files
    Args:
        url (_type_): _description_
    """
    page = requests.get(url).content
    bs_obj = BeautifulSoup(page, 'html.parser')
    maybe_directories = bs_obj.findAll('a', href=True)
    for link in maybe_directories:
        if _is_directory(link['href']):
            new_url = url + link['href']
            print(new_url)
            list_urls.append(new_url)
            print(list_urls)
            find_links(new_url, list_urls)  # recursion
        else:
            if link['href'].endswith('.zip'):
                target_path = link['href']
                response = requests.get(list_urls[-1] + link['href'], stream=True)
                if response.status_code == 200:
                    with open(target_path, 'wb') as myfile:
                        myfile.write(response.raw.read())


def import_inpi_s3():
    """_summary_
    Function that imports files from minio
    Args:
        year (_type_): _description_

    Returns:
        _type_: _description_
    """
    s3_url = "https://" + os.environ["AWS_S3_ENDPOINT"]
    conn = s3fs.S3FileSystem(client_kwargs={'endpoint_url': s3_url})
    bucket = "radjerad/diffusion/inpi/"
    path2data = [bucket + "df_pm_all_years.csv",
                    bucket + "df_rep_all_years.csv"]
    with conn.open(path2data[0], mode="rb") as file_in:
        personnes_morales = pd.read_csv(file_in, sep=";")
    with conn.open(path2data[1], mode="rb") as file_in:
        representants = pd.read_csv(file_in, sep=";")
    return personnes_morales, representants
