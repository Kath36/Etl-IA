import pandas as pd
import boto3

# Función para extraer datos desde S3
def extract_data_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_csv(obj['Body'])
    return df

# Función para transformar datos (seleccionando solo las columnas requeridas)
def transform_data(df):
    selected_columns = ['age', 'sex', 'chest pain', 'trestbps',
                        'chol', 'fbs', 'restecg', 'thalach', 'exang',
                        'oldpeak', 'slope', 'ca', 'thal']
    df_cleaned = df[selected_columns].dropna()
    return df_cleaned

# Función para cargar datos transformados en S3
def load_data_to_s3(df, bucket_name, file_key):
    s3 = boto3.client('s3')
    csv_buffer = df.to_csv(index=False).encode('utf-8')
    s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer)
    print("Conjunto de datos limpio cargado en el bucket de S3 con éxito.")

# Especificar la información del bucket y el archivo
bucket_name = 'buckdiabetes'
file_key = 'PREDICCION.csv'
output_file_key = 'Analisis.csv'

# Etapa Extract: Extraer datos desde S3
df = extract_data_from_s3(bucket_name, file_key)

# Etapa Transform: Transformar los datos (seleccionar solo las columnas requeridas y eliminar filas con valores nulos)
df_cleaned = transform_data(df)

# Etapa Load: Cargar los datos transformados en S3
load_data_to_s3(df_cleaned, bucket_name, output_file_key)