from ETL1 import Extractor, Transformer
import boto3
from io import StringIO

def run_etl(bucket_name, file_names):
    extractor = Extractor(bucket_name, file_names)
    extractor.extract()
    transformer =Transformer(extractor.dfs)
    transformer.transform()
    return transformer.clean_df

def load(clean_df, bucket_name, clean_file_name):
    s3_client = boto3.client('s3')
    csv_buffer = StringIO()
    clean_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    s3_client.put_object(
        Bucket=bucket_name,
        Key=clean_file_name,
        Body=csv_buffer.getvalue()
    )
    print("El archivo limpio ha sido subido a S3 con éxito.")