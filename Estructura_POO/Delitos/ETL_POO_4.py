import pandas as pd

# Leer los datos de DelitosT y población total por estado desde un archivo CSV
delitos_df = pd.read_csv('Delitos.csv', encoding='latin-1')

# Leer los datos de educación desde un archivo CSV
educacion_df = pd.read_excel('Educacion_MediaS_Superior.xlsx')

# Leer los datos de población total por estado desde un archivo CSV
poblacion_df = pd.read_excel('Población.xlsx')

# Fusionar los DataFrames
merged_df = pd.merge(delitos_df, educacion_df, on='Estado')
merged_df = pd.merge(merged_df, poblacion_df, on='Estado')

# Calcular la tasa de delitos por 100,000 habitantes
merged_df['TasaDelitos'] = (merged_df['DelitosT'] / merged_df['ToralP']) * 100000

# Calcular la correlación
correlation = merged_df[['TasaDelitos', 'Media superior', 'Superior']].corr()

print("Matriz de correlación:")
print(correlation)
