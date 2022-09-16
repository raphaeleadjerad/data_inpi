"""Module that exports files to minio
    """
import os
import s3fs


def export2minio(nom_fichier, df_final):
    """Function that exports a DataFrame to S3FS minio

    Args:
        nom_fichier (_type_): _description_
        df_final (_type_): _description_

    Returns:
        _type_: _description_
    """
    endpoint = "https://" + os.environ["AWS_S3_ENDPOINT"]
    fileout = s3fs.S3FileSystem(client_kwargs={'endpoint_url': endpoint})

    path2output = "radjerad/diffusion/inpi/" + nom_fichier
    with fileout.open(path2output, 'w') as file_out:
        df_final.to_csv(file_out, index=False, encoding="utf-8", sep=";")
