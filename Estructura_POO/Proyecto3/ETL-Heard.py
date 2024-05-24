from ETL2 import run_etl, load

bucket_name = 'proyectoo3'
file_names = [
    'cleveland_heart_disease.csv',
    'hungary_heart_disease.csv',
    'switzerland_heart_disease.csv',
    'va_long_beach_heart_disease.csv'
]
clean_file_name = 'heart_disease.csv'

# Ejecución del proceso ETL
clean_df = run_etl(bucket_name, file_names)
load(clean_df, bucket_name, clean_file_name)