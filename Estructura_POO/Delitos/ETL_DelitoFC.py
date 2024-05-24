import pandas as pd
import boto3
from io import BytesIO

def Extract(row):
    # Sumar los valores de las columnas de enero a diciembre, tratando los valores nulos como cero
    return row['Enero':'Diciembre'].fillna(0).sum()

def Transform(df):
    # Crear la nueva columna TotalFC aplicando la función calculate_totalfc
    df['TotalFC'] = df.apply(Extract(), axis=1)
    return (df[['Año', 'Entidad', 'Tipo de delito', 'TotalFC']].rename
            (columns={'Año': 'YEAR', 'Entidad': 'ENTITY', 'Tipo de delito': 'CRIME_TYPE'}))

def Low(df, bucket_name, file_name):
    # Convertir DataFrame a CSV en memoria
    csv_buffer = df.to_csv(index=False)

    # Subir el archivo CSV a S3
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer)

def main(bucket_name, file_key):
    # Descargar el archivo Delitos_FC_Todo.csv desde S3
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    delitos_df = pd.read_csv(BytesIO(response['Body'].read()), encoding='latin1')

    # Transformar el DataFrame y renombrar las columnas
    transformed_df = Transform(delitos_df)

    # Guardar el DataFrame transformado como CSV en S3
    Low(transformed_df, bucket_name, 'DelitosFC.csv')

if __name__ == "__main__":
    # Nombre del bucket y clave del archivo en S3
    bucket_name = 'kathbuckets'
    file_key = 'Delitos_FC_Todo.csv'

    # Ejecutar el proceso principal
    main(bucket_name, file_key)
