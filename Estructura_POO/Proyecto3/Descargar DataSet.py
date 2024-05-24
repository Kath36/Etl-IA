import pandas as pd

# Descargar el conjunto de datos completo desde la URL de UCI
url = "https://archive.ics.uci.edu/ml/machine-learning-databases/heart-disease/processed.cleveland.data"
cleveland_df = pd.read_csv(url, header=None)

url = "https://archive.ics.uci.edu/ml/machine-learning-databases/heart-disease/processed.hungarian.data"
hungary_df = pd.read_csv(url, header=None)

url = "https://archive.ics.uci.edu/ml/machine-learning-databases/heart-disease/processed.switzerland.data"
switzerland_df = pd.read_csv(url, header=None)

url = "https://archive.ics.uci.edu/ml/machine-learning-databases/heart-disease/processed.va.data"
va_long_beach_df = pd.read_csv(url, header=None)

# Nombres de las columnas según la documentación del dataset
column_names = [
    "age", "sex", "cp", "trestbps", "chol", "fbs", "restecg", "thalach", "exang",
    "oldpeak", "slope", "ca", "thal", "target"
]

# Asignar nombres de columnas a cada dataframe
cleveland_df.columns = column_names
hungary_df.columns = column_names
switzerland_df.columns = column_names
va_long_beach_df.columns = column_names

# Guardar los dataframes en archivos CSV separados
cleveland_df.to_csv('cleveland_heart_disease.csv', index=False)
hungary_df.to_csv('hungary_heart_disease.csv', index=False)
switzerland_df.to_csv('switzerland_heart_disease.csv', index=False)
va_long_beach_df.to_csv('va_long_beach_heart_disease.csv', index=False)
x