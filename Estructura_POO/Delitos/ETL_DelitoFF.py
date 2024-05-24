import pandas as pd
import boto3
from io import BytesIO

def Extract(row):
    # Sumar los valores de las columnas de ENERO a DICIEMBRE, tratando los valores nulos como cero
    return row['ENERO':'DICIEMBRE'].fillna(0).sum()

def transform(df):
    # Crear la nueva columna TotalFF aplicando la función calculate_totalff
    df['TotalFF'] = df.apply(Extract, axis=1)
    return (df[['AÑO', 'ENTIDAD', 'TIPO', 'TotalFF']].rename
            (columns={'AÑO': 'YEAR', 'ENTIDAD': 'ENTITY', 'TIPO': 'TYPE'}))

def Low(df, bucket_name, file_name):
    # Convertir DataFrame a CSV en memoria
    csv_buffer = df.to_csv(index=False)

    # Subir el archivo CSV a S3
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer)

def main(bucket_name, file_key):
    # Descargar el archivo Delitos_FF_Todo.csv desde S3
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    delitos_df = pd.read_csv(BytesIO(response['Body'].read()), encoding='latin1')

    # Filtrar las filas desde 2020 hasta 2023

    # Transformar el DataFrame y renombrar las columnas
    transformed_df = transform(delitos_df)

    # Guardar el DataFrame transformado como CSV en S3
    Low(transformed_df, bucket_name, 'DelitosFF.csv')

if __name__ == "__main__":
    # Nombre del bucket y clave del archivo en S3
    bucket_name = 'kathbuckets'
    file_key = 'Delitos_FF_Todo.csv'

    # Ejecutar el proceso principal
    main(bucket_name, file_key)
