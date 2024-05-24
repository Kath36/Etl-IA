import duckdb
import boto3
import pandas as pd

# Conexión a DuckDB
conn = duckdb.connect(database=':memory:', read_only=False)

# Conexión a AWS S3
s3 = boto3.client('s3')

# Descargar el archivo CSV desde S3
bucket_name = 'bucksalesss'
file_name = 'nuevo.csv'
csv_file = s3.get_object(Bucket=bucket_name, Key=file_name)

# Leer el archivo CSV en un DataFrame de Pandas
df = pd.read_csv(csv_file['Body'])

# Convertir las columnas "Quantity Ordered" y "Price Each" a números
df['Quantity Ordered'] = pd.to_numeric(df['Quantity Ordered'], errors='coerce')
df['Price Each'] = pd.to_numeric(df['Price Each'], errors='coerce')

# Registrar el DataFrame en DuckDB
conn.register('ventas', df)

# Calcular el total de ingresos por producto
query_total_ingresos = '''
    SELECT Product, SUM("Quantity Ordered" * "Price Each") AS Total_Ingresos
    FROM ventas
    GROUP BY Product
    ORDER BY Total_Ingresos DESC
'''
result_total_ingresos = conn.execute(query_total_ingresos)
print("Total de ingresos por producto:")
print(result_total_ingresos.fetch_df())

# Identificar los productos más vendidos y los menos vendidos
query_productos_vendidos = '''
    SELECT Product, SUM("Quantity Ordered") AS Total_Vendido
    FROM ventas
    GROUP BY Product
    ORDER BY Total_Vendido DESC
'''
result_productos_vendidos = conn.execute(query_productos_vendidos)
print("\nProductos más vendidos:")
print(result_productos_vendidos.fetch_df())

result_productos_menos_vendidos = conn.execute(query_productos_vendidos.replace("DESC", "ASC"))
print("\nProductos menos vendidos:")
print(result_productos_menos_vendidos.fetch_df())

# Calcular estadísticas descriptivas para las columnas "Cantidad ordenada" y "Precio"
query_estadisticas = '''
    SELECT
        COUNT(*) AS Total_Filas,
        MIN("Quantity Ordered") AS Min_Cantidad,
        MAX("Quantity Ordered") AS Max_Cantidad,
        AVG("Quantity Ordered") AS Promedio_Cantidad,
        STDDEV("Quantity Ordered") AS Desviacion_Cantidad,
        MIN("Price Each") AS Min_Precio,
        MAX("Price Each") AS Max_Precio,
        AVG("Price Each") AS Promedio_Precio,
        STDDEV("Price Each") AS Desviacion_Precio
    FROM ventas
'''
result_estadisticas = conn.execute(query_estadisticas)
print("\nEstadísticas descriptivas:")
print(result_estadisticas.fetch_df())
