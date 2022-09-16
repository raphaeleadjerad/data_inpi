import os
import glob
from zipfile import ZipFile
import pandas as pd
import s3fs


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