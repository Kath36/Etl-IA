import os
import boto3
import pandas as pd


def download_parquet_from_s3(bucket_name, file_key, local_dir):
    try:
        # Inicializar el cliente de S3
        s3 = boto3.client('s3')

        # Descargar el archivo Parquet desde S3 al directorio local
        local_file_path = os.path.join(local_dir, file_key)
        s3.download_file(bucket_name, file_key, local_file_path)

        print(f"Archivo Parquet '{file_key}' descargado con éxito.")
        return local_file_path
    except Exception as e:
        print(f"Error al descargar el archivo Parquet desde S3: {e}")
        return None


def convert_parquet_to_csv(parquet_file_path, local_dir):
    try:
        # Cargar el archivo Parquet
        df = pd.read_parquet(parquet_file_path)

        # Generar el nombre del archivo CSV
        csv_file_name = os.path.join(local_dir, os.path.splitext(parquet_file_path)[0] + '.csv')

        # Guardar el DataFrame como archivo CSV
        df.to_csv(csv_file_name, index=False)

        print(f"Archivo CSV '{csv_file_name}' generado con éxito.")
    except Exception as e:
        print(f"Error al convertir el archivo Parquet a CSV: {e}")


def list_files_in_bucket(bucket_name):
    try:
        # Inicializar el cliente de S3
        s3 = boto3.client('s3')

        # Obtener la lista de objetos en el bucket
        response = s3.list_objects_v2(Bucket=bucket_name)

        # Extraer los nombres de archivo con extensión .parquet
        parquet_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].lower().endswith('.parquet')]

        return parquet_files
    except Exception as e:
        print(f"Error al listar archivos en el bucket S3: {e}")
        return []


# Nombre del bucket en S3
bucket_name = 'kathbucket1'

# Directorio local para guardar los archivos descargados
local_directory = os.path.join(os.path.expanduser('~'), 'Desktop')

# Obtener la lista de archivos .parquet en el bucket
parquet_files = list_files_in_bucket(bucket_name)

# Descargar y convertir cada archivo Parquet a CSV
for parquet_file_key in parquet_files:
    local_parquet_file_path = download_parquet_from_s3(bucket_name, parquet_file_key, local_directory)
    if local_parquet_file_path:
        convert_parquet_to_csv(local_parquet_file_path, local_directory)
