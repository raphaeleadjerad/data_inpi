"""This module defines functions to import data from INPI source
"""

import tarfile
import requests


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
