import pandas as pd
import boto3
from io import StringIO


class Extractor:
    def __init__(self, bucket_name, file_names):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self.file_names = file_names
        self.dfs = []

    def extract(self):
        """Extrae los archivos CSV desde S3 y los almacena en una lista de DataFrames."""
        for file in self.file_names:
            csv_obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=file)
            body = csv_obj['Body']
            csv_string = body.read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_string))
            self.dfs.append(df)


class Cleaner:
    def __init__(self, df):
        self.df = df

    def clean(self):
        """Limpia el DataFrame reemplazando valores faltantes y ordenando columnas."""
        df = self.df

        # Reemplazar '?' con 0
        df.replace('?', 0, inplace=True)

        # Eliminar filas con valores NA
        df.dropna(inplace=True)

        # Reordenar columnas
        columns_order = [
            "age", "sex", "cp", "trestbps", "chol", "fbs", "restecg", "thalach", "exang",
            "oldpeak", "slope", "ca", "thal", "target"
        ]
        df = df[columns_order]

        # Mantener solo las filas donde 'target' es 1 o 2
        df = df[df['target'].isin([1, 2])]

        # Ordenar por edad de forma ascendente
        df.sort_values(by='age', ascending=True, inplace=True)

        self.clean_df = df


def run_etl(bucket_name, file_names):
    """Ejecuta el proceso ETL: Extrae, limpia y retorna el DataFrame limpio."""
    extractor = Extractor(bucket_name, file_names)
    extractor.extract()
    combined_df = pd.concat(extractor.dfs, ignore_index=True)

    cleaner = Cleaner(combined_df)
    cleaner.clean()

    return cleaner.clean_df


def load(clean_df, bucket_name, clean_file_name):
    """Carga el DataFrame limpio en un archivo CSV en S3."""
    s3_client = boto3.client('s3')
    csv_buffer = StringIO()
    clean_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    s3_client.put_object(
        Bucket=bucket_name,
        Key=clean_file_name,
        Body=csv_buffer.getvalue()
    )
    print(f"El archivo limpio ha sido subido a S3 con éxito como {clean_file_name}.")


# Parámetros de configuración
bucket_name = 'proyectoo3'
file_names = [
    'cleveland_heart_disease.csv',
    'hungary_heart_disease.csv',
    'switzerland_heart_disease.csv',
    'va_long_beach_heart_disease.csv'
]
clean_file_name = 'data.csv'

# Ejecución del proceso ETL
clean_df = run_etl(bucket_name, file_names)
load(clean_df, bucket_name, clean_file_name)
