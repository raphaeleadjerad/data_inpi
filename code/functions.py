import tarfile
import os
import glob
import pandas as pd


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

