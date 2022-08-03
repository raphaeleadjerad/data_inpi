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


def export2minio(nom_fichier, df_final):
    S3_ENDPOINT_URL = "https://" + os.environ["AWS_S3_ENDPOINT"]
    fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': S3_ENDPOINT_URL})
    BUCKET_OUT = "radjerad"
    FILE_KEY_OUT_S3 = "inpi/" + nom_fichier
    FILE_PATH_OUT_S3 = BUCKET_OUT + "/" + FILE_KEY_OUT_S3

    with fs.open(FILE_PATH_OUT_S3, 'w') as file_out:
        df_final.to_csv(file_out, index=False, encoding="utf-8", sep=";")
    return None


def import_inpi(nom_fichier, cols=["code greffe", "siren", "denomination", 
        "forme_juridique_x", "qualité",
        "nom_patronymique", "nom_usage", "prénoms", "type", "date_naissance", 
        "ville_naissance", "adresse_ligne1", "adresse_ligne2",
        "adresse_ligne3", "code_postal", "ville", "code_commune", "pays", 
        "id_représentant"]):
    # Create filesystem object
    S3_ENDPOINT_URL = "https://" + os.environ["AWS_S3_ENDPOINT"]
    fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': S3_ENDPOINT_URL})
    BUCKET = "radjerad/inpi"
    FILE_KEY_S3 = nom_fichier
    FILE_PATH_S3 = BUCKET + "/" + FILE_KEY_S3
    if cols is not None:
        with fs.open(FILE_PATH_S3, mode="rb") as file_in:
            df_rep_pm = pd.read_csv(file_in, sep=";", encoding="utf-8", 
                usecols=cols)
    else:
        with fs.open(FILE_PATH_S3, mode="rb") as file_in:
            df_rep_pm = pd.read_csv(file_in, sep=";", encoding="utf-8", 
                usecols=cols)
    return df_rep_pm


def clean_rep_pm_inpi(df_rep_pm):
    df_rep_pm = df_rep_pm.rename({"code greffe": "code_greffe", 
                                "forme_juridique_x": "forme_juridique", 
                                "prénoms": "prenoms", "qualité": "qualite", 
                                "id_représentant": "id_representant"}, axis=1)
    # Traitement des cas où le nom et les prénoms sont manquants
    df_rep_pm.loc[df_rep_pm["prenoms"] == "-", "prenoms"] = ""
    re1 = "(INDIVISION SUCCESSORALE DE M\.\s|\sM\.\s|^M\.\s|MME\.\s|MR\.\s|MELLE\.\s|MONSIEUR\.?\s|MADAME\.?\s|MLLE\.\s|REPRESENTEE\sPAR\sMONSIEUR\.?\s|REPRESENTEE\sPAR\s)"
    # df_rep_pm["top_civilite"] = df_rep_pm["nom_patronymique"].str.contains(re1, regex=True)
    df_rep_pm["nom_patronymique2"] = df_rep_pm["nom_patronymique"]
    df_rep_pm["nom_patronymique2"] = df_rep_pm["nom_patronymique2"].str.replace(re1,"")
    # on a modifié un peu le code de l'insee pour enlever les representee par, ici aussi car on va essayer de prendre en compte les particules
    particule = "(DU\s[A-Za-z]+|DE\sLA\s[A-Za-z]+|[A-Za-z]+\sDU\s[A-Za-z]+|[A-Za-z]+\sDE\sLA\s[A-Za-z]+|DE\s[A-Za-z]+|[A-Za-z]+\sDE\s[A-Za-z]+)"
    df_rep_pm["top_particule"] = df_rep_pm["nom_patronymique"].str.contains(particule, regex=True)
    # Traitement de la qualite
    df_rep_pm["qualite2"] = df_rep_pm["qualite"]
    df_rep_pm["qualite2"] = df_rep_pm["qualite2"].str.lower()
    df_rep_pm["qualite2"] = df_rep_pm["qualite2"].str.strip()
    # nettoyage de la dénomination
    df_rep_pm["denomination2"] = df_rep_pm["denomination"]
    df_rep_pm["denomination2"] = df_rep_pm["denomination2"].str.lower()
    # Correction des adresses
    df_rep_pm.loc[(df_rep_pm["ville"].isna()) & (df_rep_pm["adresse_ligne3"].isna() == False), "ville"] = \
    df_rep_pm.loc[(df_rep_pm["ville"].isna()) & (df_rep_pm["adresse_ligne3"].isna() == False), "adresse_ligne3"]
    df_rep_pm["commune"] = df_rep_pm["ville"]
    df_rep_pm["commune"] = df_rep_pm["commune"].str.lower()
    df_rep_pm["commune"] = (df_rep_pm["commune"].str.replace("à", "a")
                                            .str.replace("è", "e")
                                            .str.replace("é", "e")
                                            .str.replace("ç", "c")
                                            .str.replace("ù", "u")
                                            .str.replace("ô", "o"))
    df_rep_pm["commune"] = (df_rep_pm["commune"].str.replace("saint", "st"))
    df_rep_pm["commune"] = df_rep_pm["commune"].str.replace(" ", "")
    return df_rep_pm


def add_code_com(df_rep_pm):
    S3_ENDPOINT_URL = "https://" + os.environ["AWS_S3_ENDPOINT"]
    fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': S3_ENDPOINT_URL})
    BUCKET = "radjerad/diffusion/code_postal"
    FILE_KEY_S3 = "laposte_hexasmal.csv"
    FILE_PATH_S3 = BUCKET + "/" + FILE_KEY_S3

    with fs.open(FILE_PATH_S3, mode="rb") as file_in:
        code_postal = pd.read_csv(file_in, sep=";")
    code_postal.columns = [c.lower() for c in code_postal.columns]
    code_postal["code_departement"] = code_postal["code_commune_insee"].str.slice(start=0,stop=2) # code initial de l'insee, a corriger pour les drom
    code_postal["commune"] = code_postal["nom_commune"].str.lower()
    code_postal["commune"] = (code_postal["commune"].str.replace("saint", "st"))
    code_postal["commune"] = (code_postal["commune"].str.replace("à", "a")
                                            .str.replace("è", "e")
                                            .str.replace("é", "e")
                                            .str.replace("ç", "c")
                                            .str.replace("ù", "u")
                                            .str.replace("ô", "o"))
    code_postal["commune"] = code_postal["commune"].str.replace(" ", "")
    code_postal = code_postal.drop_duplicates(subset = ["commune", "code_departement"])
    code_postal = code_postal.loc[:, ["code_commune_insee", "code_departement", "commune"]]
    df_rep_pm["code_departement"] = df_rep_pm["code_postal"].astype(str).str.slice(start=0, stop=2)
    df_rep_pm = pd.merge(df_rep_pm, code_postal, on=["code_departement", "commune"], how="left")
    return df_rep_pm


def import_file(nom_fichier):
    # Create filesystem object
    S3_ENDPOINT_URL = "https://" + os.environ["AWS_S3_ENDPOINT"]
    fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': S3_ENDPOINT_URL})
    BUCKET = "radjerad"
    FILE_KEY_S3 = nom_fichier
    FILE_PATH_S3 = BUCKET + "/" + FILE_KEY_S3
    with fs.open(FILE_PATH_S3, mode="rb") as file_in:
        df_rep_pm = pd.read_csv(file_in, sep=";", encoding="utf-8")
    return df_rep_pm