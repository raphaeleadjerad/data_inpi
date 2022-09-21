
"""_summary_
    """


def clean_rep_inpi(df_rep):
    """_summary_Function that cleans data from Inpi

    Args:
        df_rep (_type_): _description_

    Returns:
        _type_: _description_
    """
    df_rep = df_rep.rename({"code greffe": "code_greffe", 
                                "forme_juridique_x": "forme_juridique", 
                                "prénoms": "prenoms", "qualité": "qualite", 
                                "id_représentant": "id_representant"}, axis=1)
    # Traitement des cas où le nom et les prénoms sont manquants
    df_rep.loc[df_rep["prenoms"] == "-", "prenoms"] = ""
    re1 = "(INDIVISION SUCCESSORALE DE M\.\s|\sM\.\s|^M\.\s|MME\.\s|MR\.\s|MELLE\.\s|MONSIEUR\.?\s|MADAME\.?\s|MLLE\.\s|REPRESENTEE\sPAR\sMONSIEUR\.?\s|REPRESENTEE\sPAR\s)"
    # df_rep["top_civilite"] = df_rep["nom_patronymique"].str.contains(re1, regex=True)
    df_rep["nom_patronymique2"] = df_rep["nom_patronymique"]
    df_rep["nom_patronymique2"] = df_rep["nom_patronymique2"].str.replace(re1,"")
    # on a modifié un peu le code de l'insee pour enlever les representee par, ici aussi car on va essayer de prendre en compte les particules
    particule = "(DU\s[A-Za-z]+|DE\sLA\s[A-Za-z]+|[A-Za-z]+\sDU\s[A-Za-z]+|[A-Za-z]+\sDE\sLA\s[A-Za-z]+|DE\s[A-Za-z]+|[A-Za-z]+\sDE\s[A-Za-z]+)"
    df_rep["top_particule"] = df_rep["nom_patronymique"].str.contains(particule, regex=True)
    # Traitement de la qualite
    df_rep["qualite2"] = df_rep["qualite"]
    df_rep["qualite2"] = df_rep["qualite2"].str.lower()
    df_rep["qualite2"] = df_rep["qualite2"].str.strip()
    # nettoyage de la dénomination
    df_rep["denomination2"] = df_rep["denomination"]
    df_rep["denomination2"] = df_rep["denomination2"].str.lower()
    # Correction des adresses
    df_rep.loc[(df_rep["ville"].isna()) & (df_rep["adresse_ligne3"].isna() == False), "ville"] = \
    df_rep.loc[(df_rep["ville"].isna()) & (df_rep["adresse_ligne3"].isna() == False), "adresse_ligne3"]
    df_rep["commune"] = df_rep["ville"]
    df_rep["commune"] = df_rep["commune"].str.lower()
    df_rep["commune"] = (df_rep["commune"].str.replace("à", "a")
                                            .str.replace("è", "e")
                                            .str.replace("é", "e")
                                            .str.replace("ç", "c")
                                            .str.replace("ù", "u")
                                            .str.replace("ô", "o"))
    df_rep["commune"] = (df_rep["commune"].str.replace("saint", "st"))
    df_rep["commune"] = df_rep["commune"].str.replace(" ", "")
    return df_rep
