import pandas as pd
import boto3
from io import StringIO

class ETL:
    def __init__(self, source_bucket, target_bucket, columns, report_prefix, report_extension):
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
        self.columns = columns
        self.report_prefix = report_prefix
        self.report_extension = report_extension

    def extract(self):
        print("Extrayendo datos del bucket de origen:", self.source_bucket)
        s3 = boto3.client('s3')
        all_files = []
        for obj in s3.list_objects_v2(Bucket=self.source_bucket)['Contents']:
            file_name = obj['Key']
            if file_name.endswith('.csv'):
                print("Leyendo archivo:", file_name)
                csv_obj = s3.get_object(Bucket=self.source_bucket, Key=file_name)
                csv_content = csv_obj['Body'].read().decode('utf-8')
                file_df = pd.read_csv(StringIO(csv_content))
                print("Datos de muestra del archivo:")
                print(file_df.head())
                all_files.append(file_df)

        combined_df = pd.concat(all_files, ignore_index=True)
        return combined_df

    def clean_data(self, df):
        print("Limpiando datos")
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
        print("Generando reporte de análisis temporal")
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
        temporal_report = pd.DataFrame({'Mes_Actividad_Max': [max_month], 'Mes_Actividad_Min': [min_month]})
        print("Reporte de análisis temporal:")
        print(temporal_report)
        return temporal_report

    def generate_product_analysis_report(self, df):
        print("Generando reporte de análisis de productos")
        # Convertir la columna 'Quantity Ordered' a tipo numérico
        df['Quantity Ordered'] = pd.to_numeric(df['Quantity Ordered'], errors='coerce')

        # Eliminar filas con valores NaN después de la conversión
        df = df.dropna(subset=['Quantity Ordered'])
        # Calcular total de ingresos por producto
        df['Ingreso Total'] = df['Quantity Ordered'] * df['Price Each']
        # Identificar productos más vendidos y menos vendidos
        productos_mas_vendidos = df.groupby('Product')['Quantity Ordered'].sum().nlargest(5)
        productos_menos_vendidos = df.groupby('Product')['Quantity Ordered'].sum().nsmallest(5)
        # Realizar análisis de variación de precios de los productos
        variacion_precio_producto = df.groupby('Product')['Price Each'].std()
        # Crear el DataFrame para el reporte
        product_report = pd.DataFrame({'Productos_Mas_Vendidos': productos_mas_vendidos,
                                       'Productos_Menos_Vendidos': productos_menos_vendidos,
                                       'Variacion_Precio': variacion_precio_producto})
        print("Reporte de análisis de productos:")
        print(product_report)
        return product_report

    def generate_descriptive_statistics_report(self, df):
        print("Generando reporte de estadísticas descriptivas")
        # Calcular estadísticas descriptivas
        estadisticas_cantidad = df['Quantity Ordered'].describe()
        estadisticas_precio = df['Price Each'].describe()

        # Crear el DataFrame para el reporte
        statistics_report = pd.DataFrame({'Estadisticas_Cantidad': estadisticas_cantidad,
                                          'Estadisticas_Precio': estadisticas_precio})
        print("Reporte de estadísticas descriptivas:")
        print(statistics_report)

        return statistics_report

    def load(self, df, nombre_archivo):
        print("Cargando datos transformados en el bucket de destino:", self.target_bucket)
        # Guardar reporte en formato Parquet
        file_key = self.report_prefix + nombre_archivo + self.report_extension
        df.to_parquet(f's3://{self.target_bucket}/{file_key}', index=False)
        print("Archivo cargado en S3:", file_key)