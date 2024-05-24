

from ETL import ETL

def etl_report(etl_obj, parametros):
    data_df = etl_obj.extract()
    cleaned_df = etl_obj.clean_data(data_df)

    # Generar reportes
    temporal_report = etl_obj.generate_temporal_analysis_report(cleaned_df)
    product_report = etl_obj.generate_product_analysis_report(cleaned_df)
    statistics_report = etl_obj.generate_descriptive_statistics_report(cleaned_df)

    # Cargar reportes
    etl_obj.load(temporal_report, "Reporte_Analisis_Temporal")
    etl_obj.load(product_report, "Reporte_Analisis_Productos")
    etl_obj.load(statistics_report, "Reporte_Estadisticas_Descriptivas")

def main():
    print("Iniciando")
    # Parámetros
    parametros = [
        'kathbucket1',
        'kathbucket1',
        ["Order ID", "Product", "Quantity Ordered", "Price Each", "Order Date", "Purchase Address"],
        'reporte_diario_',
        '.parquet'
    ]

    # Inicialización
    etl_obj = ETL(
        source_bucket=parametros[0],
        target_bucket=parametros[1],
        columns=parametros[2],
        report_prefix=parametros[3],
        report_extension=parametros[4]
    )

    # Ejecutar aplicación
    etl_report(etl_obj, parametros)

if __name__ == "__main__":
    main()