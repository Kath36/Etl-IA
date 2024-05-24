import pandas as pd
import boto3
from io import BytesIO

def Extract(bucket_name, file_key_ff, file_key_fc):
    # Descargar los archivos CSV desde S3
    s3 = boto3.client('s3')
    response_ff = s3.get_object(Bucket=bucket_name, Key=file_key_ff)
    response_fc = s3.get_object(Bucket=bucket_name, Key=file_key_fc)

    # Leer los archivos CSV en DataFrames
    delitos_ff_df = pd.read_csv(BytesIO(response_ff['Body'].read()), encoding='latin1')
    delitos_fc_df = pd.read_csv(BytesIO(response_fc['Body'].read()), encoding='latin1')

    # Fusionar las columnas 'TotalFF' y 'TotalFC' en una sola columna 'DelitosT'
    delitos_ff_df['DelitosT'] = delitos_ff_df['TotalFF'] + delitos_fc_df['TotalFC']

    # Seleccionar solo las columnas 'YEAR', 'ENTITY', 'DelitosT'
    merged_df = delitos_ff_df[['YEAR', 'ENTITY', 'DelitosT']]

    # Realizar la limpieza de datos
    # Eliminar registros duplicados
    merged_df = merged_df.drop_duplicates()

    # Eliminar registros con valores nulos o NaN
    merged_df = merged_df.dropna()

    # Convertir el tipo de datos de la columna 'YEAR' a entero (si es necesario)
    merged_df['YEAR'] = merged_df['YEAR'].astype(int)

    # Establecer el formato uniforme para los valores de texto en mayúscuLAS Y MINUSCULAS NBBBBBBBBBBB
    merged_df['ENTITY'] = merged_df['ENTITY'].str.upper()

    # Agrupar por 'ENTITY' y sumar los valores de 'DelitosT'
    merged_df = merged_df.groupby('ENTITY', as_index=False).agg({'DelitosT': 'sum'})

    # Guardar el DataFrame fusionado y limpiado como CSV en S3
    csv_buffer = merged_df.to_csv(index=False)
    s3.put_object(Bucket=bucket_name, Key='Delitos.csv', Body=csv_buffer)

if __name__ == "__main__":
    # Nombre del bucket y claves de los archivos en S3
    bucket_name = 'kathbuckets'
    file_key_ff = 'DelitosFF.csv'
    file_key_fc = 'DelitosFC.csv'

    # Ejecutar la función para fusionar, limpiar y cargar los archivos
    Extract(bucket_name, file_key_ff, file_key_fc)
