# Librerías para funcionamiento
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.metrics import accuracy_score

# Leer el csv y separamos las características y las objetivos
data = pd.read_csv('data.csv')
X = data[['age', 'sex', 'cp', 'trestbps', 'chol', 'fbs', 'restecg',
           'thalach', 'exang', 'oldpeak', 'slope', 'ca', 'thal']]
y = data['target']

# Normalizamos los datos
escalador = preprocessing.MinMaxScaler()
XScalado = escalador.fit_transform(X)

# Entrenar el modelo
regresion = LogisticRegression()
regresion.fit(X, y)

# Nuevos datos para predecir
persona = {
    'age': 51,
    'sex': 1,
    'cp': 2,
    'trestbps': 107,
    'chol': 257,
    'fbs': 2,
    'restecg': 0,
    'thalach': 132,
    'exang': 1,
    'oldpeak': 0.8,
    'slope': 2,
    'ca': 1,
    'thal': 2
}

# Convertir los datos a un DataFrame
nEntrada = pd.DataFrame([persona])

# Predecir sobre los nuevos datos
prediccion = regresion.predict(nEntrada)

# Imprimir la predicción
if prediccion[0] == 2:
    print(f"El modelo predice que la persona está enferma.")
else:
    print("El modelo predice que la persona está sana.")

# Calcular y imprimir la precisión del modelo
XEnt, Xtest, yEnt, yTest = train_test_split(X, y, test_size=0.3, random_state=42)
y_pred = regresion.predict(Xtest)
precision = accuracy_score(yTest, y_pred)
print("Precisión del modelo:", precision)# Librerías para funcionamiento
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.metrics import accuracy_score

# Leer el csv y separamos las características y las objetivos
data = pd.read_csv('ML/data.csv')
X = data[['age', 'sex', 'cp', 'trestbps', 'chol', 'fbs', 'restecg',
           'thalach', 'exang', 'oldpeak', 'slope', 'ca', 'thal']]
y = data['target']

# Normalizamos los datos
escalador = preprocessing.MinMaxScaler()
XScalado = escalador.fit_transform(X)

# Entrenar el modelo
regresion = LogisticRegression()
regresion.fit(X, y)

# Nuevos datos para predecir
persona = {
    'age': 51,
    'sex': 1,
    'cp': 2,
    'trestbps': 107,
    'chol': 257,
    'fbs': 2,
    'restecg': 0,
    'thalach': 132,
    'exang': 1,
    'oldpeak': 0.8,
    'slope': 2,
    'ca': 1,
    'thal': 2
}

# Convertir los datos a un DataFrame
nEntrada = pd.DataFrame([persona])

# Predecir sobre los nuevos datos
prediccion = regresion.predict(nEntrada)

# Imprimir la predicción
if prediccion[0] == 2:
    print(f"El modelo predice que la persona está enferma.")
else:
    print("El modelo predice que la persona está sana.")

# Calcular y imprimir la precisión del modelo
XEnt, Xtest, yEnt, yTest = train_test_split(X, y, test_size=0.3, random_state=42)
y_pred = regresion.predict(Xtest)
precision = accuracy_score(yTest, y_pred)
print("Precisión del modelo:", precision)