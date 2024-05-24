import pandas as pd
import numpy as np
import boto3
from io import StringIO

class ETL:
    def __init__(self, source_bucket, target_bucket, columns, repor
        t_prefix, report_extension):
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
        self.columns = columns
        self.report_prefix = report_prefix
        self.report_extension = report_extension

    def extract(self):
        print("Extracting data from source bucket:", self.source_bucket)
        s3 = boto3.client('s3')
        all_files = []
        for obj in s3.list_objects_v2(Bucket=self.source_bucket)['Contents']:
            file_name = obj['Key']
            if file_name.endswith('.csv'):
                print("Reading file:", file_name)
                csv_obj = s3.get_object(Bucket=self.source_bucket, Key=file_name)
                csv_content = csv_obj['Body'].read().decode('utf-8')
                file_df = pd.read_csv(StringIO(csv_content))
                print("Sample data from file:")
                print(file_df.head())
                all_files.append(file_df)

        combined_df = pd.concat(all_files, ignore_index=True)
        return combined_df

    def clean_data(self, df):
        print("Cleaning data")
        # Eliminar filas con valores no válidos en la columna "Order Date"
        df = df[df["Order Date"] != "Order Date"].copy()  # Añade .copy() para evitar el SettingWithCopyWarning

        # Convertir la columna "Order Date" a objetos de fecha y hora
        df.loc[:, "Order Date"] = pd.to_datetime(df["Order Date"], format="%m/%d/%y %H:%M", errors='coerce')

        # Convertir las columnas 'Quantity Ordered' y 'Price Each' a tipo numérico
        df.loc[:, 'Quantity Ordered'] = pd.to_numeric(df['Quantity Ordered'], errors='coerce')
        df.loc[:, 'Price Each'] = pd.to_numeric(df['Price Each'], errors='coerce')

        # Eliminar filas con valores NaN después de la conversión
        df = df.dropna(subset=["Order Date", "Quantity Ordered", "Price Each"])

        return df

    def generate_temporal_analysis_report(self, df):
        print("Generating Temporal Analysis report")
        # Filtrar filas con valores nulos en la columna "Order Date"
        df = df.dropna(subset=['Order Date'])

        # Convertir la columna "Order Date" a objetos de fecha y hora si aún no lo está
        if not pd.api.types.is_datetime64_any_dtype(df['Order Date']):
            df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')

        # Crear nuevas columnas para mes y año de cada orden
        df['Month'] = df['Order Date'].dt.month
        df['Year'] = df['Order Date'].dt.year

        # Identificar meses de mayor y menor actividad
        monthly_sales = df.groupby(['Year', 'Month'])['Quantity Ordered'].sum()
        max_month = monthly_sales.idxmax()
        min_month = monthly_sales.idxmin()

        # Crear el DataFrame para el reporte
        temporal_report = pd.DataFrame({'Max_Month_Activity': [max_month], 'Min_Month_Activity': [min_month]})

        return temporal_report

    def generate_product_analysis_report(self, df):
        print("Generating Product Analysis report")
        # Convertir la columna 'Quantity Ordered' a tipo numérico
        df['Quantity Ordered'] = pd.to_numeric(df['Quantity Ordered'], errors='coerce')

        # Eliminar filas con valores NaN después de la conversión
        df = df.dropna(subset=['Quantity Ordered'])

        # Calcular total de ingresos por producto
        df['Total Income'] = df['Quantity Ordered'] * df['Price Each']

        # Identificar productos más vendidos y menos vendidos
        top_selling_products = df.groupby('Product')['Quantity Ordered'].sum().nlargest(5)
        least_selling_products = df.groupby('Product')['Quantity Ordered'].sum().nsmallest(5)

        # Realizar análisis de variación de precios de los productos
        product_price_variation = df.groupby('Product')['Price Each'].std()

        # Crear el DataFrame para el reporte
        product_report = pd.DataFrame({'Top_Selling_Products': top_selling_products,
                                       'Least_Selling_Products': least_selling_products,
                                       'Price_Variation': product_price_variation})

        return product_report

    def generate_descriptive_statistics_report(self, df):
        print("Generating Descriptive Statistics report")
        # Calcular estadísticas descriptivas
        quantity_stats = df['Quantity Ordered'].describe()
        price_stats = df['Price Each'].describe()

        # Crear el DataFrame para el reporte
        statistics_report = pd.DataFrame({'Quantity_Statistics': quantity_stats, 'Price_Statistics': price_stats})

        return statistics_report

    def load(self, df, file_name):
        print("Loading transformed data into target bucket:", self.target_bucket)
        # Guardar reporte en formato Parquet
        file_key = self.report_prefix + file_name + self.report_extension
        df.to_parquet(f's3://{self.target_bucket}/{file_key}', index=False)
        print("File uploaded to S3:", file_key)

def etl_report(etl_obj, parameters):
    data_df = etl_obj.extract()
    cleaned_df = etl_obj.clean_data(data_df)

    # Generar reportes
    temporal_report = etl_obj.generate_temporal_analysis_report(cleaned_df)
    product_report = etl_obj.generate_product_analysis_report(cleaned_df)
    statistics_report = etl_obj.generate_descriptive_statistics_report(cleaned_df)

    # Cargar reportes
    etl_obj.load(temporal_report, "Temporal_Analysis_Report")
    etl_obj.load(product_report, "Product_Analysis_Report")
    etl_obj.load(statistics_report, "Descriptive_Statistics_Report")

def main():
    print("Starting")
    # Parameters
    parameters = [
        'kathbucket1',
        'kathbucket1',
        ["Order ID", "Product", "Quantity Ordered", "Price Each", "Order Date", "Purchase Address"],
        'daily_report_',
        '.parquet'
    ]

    # Init
    etl_obj = ETL(
        source_bucket=parameters[0],
        target_bucket=parameters[1],
        columns=parameters[2],
        report_prefix=parameters[3],
        report_extension=parameters[4]
    )

    # run application
    etl_report(etl_obj, parameters)

if __name__ == "__main__":
    main()
