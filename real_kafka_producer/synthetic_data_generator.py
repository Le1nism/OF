import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
import pandas as pd
from scipy.stats import expon
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import seaborn as sns
from copulas.multivariate import GaussianMultivariate
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
import warnings
import numpy as np

#nomi delle colonne da generare
columns_to_generate = [
    'Durata','CabEnabled_M1', 'CabEnabled_M8', 'ERTMS_PiastraSts', 'HMI_ACPntSts_T2', 'HMI_ACPntSts_T7', 'HMI_DCPntSts_T2',
    'HMI_DCPntSts_T7', 'HMI_Iline', 'HMI_Irsts_T2', 'HMI_Irsts_T7', 'HMI_VBatt_T2', 'HMI_VBatt_T4', 'HMI_VBatt_T5',
    'HMI_VBatt_T7', 'HMI_Vline', 'HMI_impSIL', 'LineVoltType', 'MDS_LedLimVel', 'MDS_StatoMarcia', '_GPS_LAT',
    '_GPS_LON', 'ldvvelimps', 'ldvveltreno', 'usB1BCilPres_M1', 'usB1BCilPres_M3', 'usB1BCilPres_M6', 'usB1BCilPres_M8',
    'usB1BCilPres_T2', 'usB1BCilPres_T4', 'usB1BCilPres_T5', 'usB1BCilPres_T7', 'usB2BCilPres_M1', 'usB2BCilPres_M3',
    'usB2BCilPres_M6', 'usB2BCilPres_M8', 'usB2BCilPres_T2', 'usB2BCilPres_T4', 'usB2BCilPres_T5', 'usB2BCilPres_T7',
    'usBpPres', 'usMpPres'
]

#unisco il dataset con le classificazioni
dataset = pd.read_csv('dataset.csv')
classifications = pd.read_csv('descrizioni_classificate.csv')
event_mapped = dict(zip(classifications['Descrizione'], classifications['Tipo_Evento']))
dataset['Tipo_Evento'] = dataset['Descrizione'].map(event_mapped)
dataset.to_csv('dataset_classificato.csv', index=False)

# rappresetaione grafica dei dati
if 'Componente' in dataset.columns and 'Tipo_Evento' in dataset.columns:
    eventi_per_componente = dataset.groupby(['Componente', 'Tipo_Evento']).size().unstack(fill_value=0)
    top_anomalies = eventi_per_componente['Anomalia'].nlargest(10)
    plt.figure(figsize=(10, 6))
    top_anomalies.plot(kind='bar', color='red')
    plt.title('Top 10 Componenti con Più Anomalie')
    plt.xlabel('Componente')
    plt.ylabel('Numero di Anomalie')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()
    top_normal_events = eventi_per_componente['Funzionamento Normale'].nlargest(10)
    plt.figure(figsize=(10, 6))
    top_normal_events.plot(kind='bar', color='green')
    plt.title('Top 10 Componenti con Più Eventi Normali')
    plt.xlabel('Componente')
    plt.ylabel('Numero di Eventi Normali')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()
    counts = dataset['Tipo_Evento'].value_counts()
    plt.figure(figsize=(6, 6))
    counts.plot(kind='pie', autopct='%1.1f%%', colors=['red', 'green'])
    plt.title('Distribuzione Totale di Anomalie ed Eventi Normali')
    plt.ylabel('')
    plt.show()
else:
    print("Le colonne 'Componente' e 'Tipo_Evento' devono essere presenti nel dataset per generare i grafici.")

#filtro il dataset per il componente VEHICLE e rimuovo le colonne vuote
print(f"Numero di righe prima del filtraggio: {dataset.shape[0]}")
print(f"Numero di colonne prima della rimozione di colonne vuote: {dataset.shape[1]}")
df_vehicle = dataset[dataset['Componente'] == 'VEHICLE']
df_vehicle_cleaned = df_vehicle.dropna(axis=1, how='all')
print(f"Numero di righe dopo il filtraggio: {df_vehicle_cleaned.shape[0]}")
print(f"Numero di colonne dopo la rimozione di colonne vuote: {df_vehicle_cleaned.shape[1]}")

#visualizzazione della distribuzione degli eventi
event_type = df_vehicle_cleaned['Tipo_Evento'].value_counts()
event_type.plot(kind='bar', color=['green', 'red'])
plt.title('Distribuzione degli Eventi (Anomalia vs Funzionamento Normale)')
plt.xlabel('Tipo di Evento')
plt.ylabel('Numero di Eventi')
plt.show()

#visualizzazione della distribuzione degli eventi
df_unique_description = df_vehicle_cleaned[['Descrizione', 'Tipo_Evento']].drop_duplicates()
event_type = df_unique_description['Tipo_Evento'].value_counts()
event_type.plot(kind='bar', color=['green', 'red'])
plt.title('Distribuzione degli Eventi (Anomalia vs Funzionamento Normale)')
plt.xlabel('Tipo di Evento')
plt.ylabel('Numero di Eventi')
plt.show()

#visualizzazione della distribazione delle anomalie nel tempo
df_vehicle_cleaned['Timestamp'] = pd.to_datetime(df_vehicle_cleaned['Timestamp'], errors='coerce')
df_anomalie = df_vehicle_cleaned[df_vehicle_cleaned['Tipo_Evento'] == 'Anomalia'].dropna(subset=['Timestamp'])
df_anomalie.set_index('Timestamp', inplace=True)
df_anomalie.resample('D').size().plot(title='Distribuzione delle Anomalie nel Tempo', figsize=(10, 6))
plt.xlabel('Data')
plt.ylabel('Numero di Anomalie')
plt.show()

#visualizzazione della distribazione dei funzionamenti normali nel tempo
df_vehicle_cleaned['Timestamp'] = pd.to_datetime(df_vehicle_cleaned['Timestamp'], errors='coerce')
df_normali = df_vehicle_cleaned[df_vehicle_cleaned['Tipo_Evento'] == 'Funzionamento Normale'].dropna(subset=['Timestamp'])
df_normali.set_index('Timestamp', inplace=True)
df_normali.resample('D').size().plot(title='Distribuzione delle Anomalie nel Tempo', figsize=(10, 6))
plt.xlabel('Data')
plt.ylabel('Numero di Anomalie')
plt.show()

#aggiungo la colonna 'Tipo_Evento_Classificato' al dataset
df_vehicle_cleaned['Tipo_Evento_Classificato'] = df_vehicle_cleaned['Tipo_Evento'].apply(lambda x: 1 if x == 'Funzionamento Normale' else 0)
df_vehicle_cleaned.head()

#rumuovo le righe che contengono valori mancanti
df_cleaned_no_nan = df_vehicle_cleaned.dropna()
df_cleaned_no_nan.isnull().sum().sum(), df_cleaned_no_nan.shape
df_cleaned_no_nan.to_csv('dataset_cleaned.csv', index=False) #ho salvato i nuovi file per poterli vedere e capire la struttura, non ha un'utilità pratica

#matrice di correlazione
correlation_matrix = df_vehicle_cleaned[columns_to_generate].corr()
plt.figure(figsize=(20, 15))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', linewidths=0.5)
plt.title('Matrice di Correlazione delle Feature Numeriche')
plt.show()

#funzione per plottare la matrice di confusione
def plot_confusion_matrix(y_true, y_pred, title):
    cm = confusion_matrix(y_true, y_pred, labels=['Anomalia', 'Funzionamento Normale'])
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=['Anomalia', 'Funzionamento Normale'],
                yticklabels=['Anomalia', 'Funzionamento Normale'])
    plt.xlabel('Predizione')
    plt.ylabel('Valore Reale')
    plt.title(title)
    plt.show()

#creatzione modello e valutazione accuratezza
X_final = df_cleaned_no_nan.drop(columns=['Tipo_Evento_Classificato', 'Tipo_Evento', 'Descrizione', 'Timestamp', 'Timestamp chiusura', 'Posizione', 'Flotta', 'Veicolo', 'Codice', 'Nome', 'Test'])
y_final = df_cleaned_no_nan['Tipo_Evento_Classificato']
y_final = y_final.replace({1: 'Funzionamento Normale', 0: 'Anomalia'})
non_numeric_columns = X_final.select_dtypes(include=['object']).columns
X_final = X_final.drop(columns=non_numeric_columns)
X_train, X_test, y_train, y_test = train_test_split(X_final, y_final, test_size=0.2, random_state=42)
rf_model = RandomForestClassifier(random_state=42)
rf_model.fit(X_train, y_train)
y_pred = rf_model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
classification_rep = classification_report(y_test, y_pred, target_names=['Anomalia', 'Funzionamento Normale'])
print(f"Accuratezza del modello: {accuracy:.4f}")
print("Report di classificazione:")
print(classification_rep)
conf_matrix = confusion_matrix(y_test, y_pred, labels=['Anomalia', 'Funzionamento Normale'])
disp = ConfusionMatrixDisplay(confusion_matrix=conf_matrix, display_labels=['Anomalia', 'Funzionamento Normale'])
disp.plot(cmap=plt.cm.Blues)
plt.title('Confusion Matrix')
plt.show()

#leggo il file csv e pulisco i dati
df_cleaned_no_nan['Timestamp'] = pd.to_datetime(df_cleaned_no_nan['Timestamp'], errors='coerce')
vehicle_data_clean = df_cleaned_no_nan.dropna(subset=['Timestamp', 'Timestamp chiusura'])
vehicle_data_clean = vehicle_data_clean.drop_duplicates(subset=['Descrizione', 'Timestamp'])
print(f"Righe rimaste dopo la pulizia: {len(vehicle_data_clean)}")

#calcolo il tempo di interarrivo medio tra le anomalie
anomalies = vehicle_data_clean[vehicle_data_clean['Tipo_Evento'] == 'Anomalia']
anomalies_sorted = anomalies.sort_values(by='Timestamp')
anomalies_sorted['Interarrival_Time'] = anomalies_sorted['Timestamp'].diff().dt.total_seconds()
anomalies_interarrival = anomalies_sorted['Interarrival_Time'].dropna()
print(f"Numero di anomalie dopo il calcolo del tempo di interarrivo: {len(anomalies_interarrival)}")

#filtro le anomalie per la deviazione standard
mean_anomalies = anomalies_interarrival.mean()
std_anomalies = anomalies_interarrival.std()
filtered_anomalies_interarrival = anomalies_interarrival[
    (anomalies_interarrival > mean_anomalies - 2 * std_anomalies) &
    (anomalies_interarrival < mean_anomalies + 2 * std_anomalies)
]
print(f"Numero di anomalie dopo il filtraggio: {len(filtered_anomalies_interarrival)}")

#distribuzione esponenziale sui tempi di interarrivo delle anomalie, filtrato per deviazione standard maggiore di 2
lambda_expon_anomalies = 1 / filtered_anomalies_interarrival.mean()
x_limit = 500
expon_distribution_anomalies = expon.pdf(range(x_limit), scale=1/lambda_expon_anomalies)
plt.figure(figsize=(10,6))
plt.hist(filtered_anomalies_interarrival, bins=100, density=True, alpha=0.6, color='g', label='Interarrival Times (Anomalies)')
plt.plot(range(x_limit), expon_distribution_anomalies, 'r-', lw=2, label='Exponential Distribution (Anomalies)')
plt.title('Distribuzione Esponenziale vs Tempi di Interarrivo delle Anomalie (Filtrati)')
plt.xlabel('Tempo di Interarrivo (secondi)')
plt.ylabel('Densità di probabilità')
plt.xlim(0, x_limit)
plt.legend()
plt.show()

#calcolo il tempo di interarrivo medio tra i funzionamenti normali
normal_operations = vehicle_data_clean[vehicle_data_clean['Tipo_Evento'] == 'Funzionamento Normale']
normal_operations_sorted = normal_operations.sort_values(by='Timestamp')
normal_operations_sorted['Interarrival_Time'] = normal_operations_sorted['Timestamp'].diff().dt.total_seconds()
normal_interarrival = normal_operations_sorted['Interarrival_Time'].dropna()
print(f"Numero di operazioni normali dopo il calcolo del tempo di interarrivo: {len(normal_interarrival)}")

#filtro i log di funzionamento nornmale per la deviazione standard
mean_normal = normal_interarrival.mean()
std_normal = normal_interarrival.std()
filtered_normal_interarrival = normal_interarrival[
    (normal_interarrival > mean_normal - 2 * std_normal) &
    (normal_interarrival < mean_normal + 2 * std_normal)
]
print(f"Numero di operazioni normali dopo il filtraggio: {len(filtered_normal_interarrival)}")

#distribuzione esponenziale sui tempi di interarrivo del funzionamento normale, filtrato per deviazione standard maggiore di 2
lambda_expon_normal = 1 / filtered_normal_interarrival.mean()
expon_distribution_normal = expon.pdf(range(x_limit), scale=1/lambda_expon_normal)
plt.figure(figsize=(10,6))
plt.hist(filtered_normal_interarrival, bins=100, density=True, alpha=0.6, color='b', label='Interarrival Times (Normal Operation)')
plt.plot(range(x_limit), expon_distribution_normal, 'r-', lw=2, label='Exponential Distribution (Normal Operation)')
plt.title('Distribuzione Esponenziale vs Tempi di Interarrivo del Funzionamento Normale (Filtrati)')
plt.xlabel('Tempo di Interarrivo (secondi)')
plt.ylabel('Densità di probabilità')
plt.xlim(0, x_limit)
plt.legend()
plt.show()

#calcolo media e mediana dei tempi di interarrivo delle anomalie e del funzionamento normale
mean_filtered_anomalies = filtered_anomalies_interarrival.mean()
median_filtered_anomalies = filtered_anomalies_interarrival.median()
print(f"Media dei tempi di interarrivo delle anomalie filtrate: {mean_filtered_anomalies}")
print(f"Mediana dei tempi di interarrivo delle anomalie filtrate: {median_filtered_anomalies}")
mean_filtered_normal = filtered_normal_interarrival.mean()
median_filtered_normal = filtered_normal_interarrival.median()
print(f"Media dei tempi di interarrivo del funzionamento normale filtrato: {mean_filtered_normal}")
print(f"Mediana dei tempi di interarrivo del funzionamento normale filtrato: {median_filtered_normal}")

#caricare il dataset e selezionare i dati
print(f"Dataset caricato con {len(df_cleaned_no_nan)} righe.")
df_anomalie = df_cleaned_no_nan[df_cleaned_no_nan['Tipo_Evento_Classificato'] == 0]
df_normali = df_cleaned_no_nan[df_cleaned_no_nan['Tipo_Evento_Classificato'] == 1]
print(f"Eventi di anomalia selezionati: {len(df_anomalie)} righe.")
print(f"Eventi di funzionamento normale selezionati: {len(df_normali)} righe.")

#matrice di correlazione
correlation_matrix = df_anomalie[columns_to_generate].corr()
plt.figure(figsize=(20, 15))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', linewidths=0.5)
plt.title('Matrice di Correlazione delle Feature Numeriche')
plt.show()

#creatzione modello e valutazione accuratezza
X_final = df_cleaned_no_nan.drop(columns=['Tipo_Evento_Classificato', 'Tipo_Evento', 'Descrizione', 'Timestamp', 'Timestamp chiusura', 'Posizione', 'Flotta', 'Veicolo', 'Codice', 'Nome', 'Test', 'Longitudine', 'Latitudine', 'Contemporaneo'])
y_final = df_cleaned_no_nan['Tipo_Evento_Classificato']
y_final = y_final.replace({1: 'Funzionamento Normale', 0: 'Anomalia'})
non_numeric_columns = X_final.select_dtypes(include=['object']).columns
X_final = X_final.drop(columns=non_numeric_columns)
X_train, X_test, y_train, y_test = train_test_split(X_final, y_final, test_size=0.2, random_state=42)
rf_model = RandomForestClassifier(random_state=42)
rf_model.fit(X_train, y_train)
y_pred = rf_model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
classification_rep = classification_report(y_test, y_pred, target_names=['Anomalia', 'Funzionamento Normale'])
print(f"Accuratezza del modello: {accuracy:.4f}")
print("Report di classificazione:")
print(classification_rep)

#rimuovo i duplicati consecutivi
columns_to_check = ['Descrizione', 'Timestamp', 'Codice', 'Nome']
df_deduplicated_anomalies = df_anomalie.loc[(df_anomalie[columns_to_check] != df_anomalie[columns_to_check].shift()).any(axis=1)]
print(f"Righe originali: {len(df_anomalie)}")
print(f"Righe dopo rimozione delle ridondanze: {len(df_deduplicated_anomalies)}")

#rimuovo i duplicati consecutivi
columns_to_check = ['Descrizione', 'Timestamp', 'Codice', 'Nome']
df_deduplicated_normals = df_normali.loc[(df_normali[columns_to_check] != df_normali[columns_to_check].shift()).any(axis=1)]
print(f"Righe originali: {len(df_normali)}")
print(f"Righe dopo rimozione delle ridondanze: {len(df_deduplicated_normals)}")

#generazione dati sintetici con copula gaussiana
warnings.filterwarnings("ignore", category=RuntimeWarning)
data_us_anomalie = df_deduplicated_anomalies[columns_to_generate].drop(columns=['Durata'])
data_us_normali = df_deduplicated_normals[columns_to_generate].drop(columns=['Durata'])
copula_us_anomalie = GaussianMultivariate()
copula_us_anomalie.fit(data_us_anomalie)
copula_us_normali = GaussianMultivariate()
copula_us_normali.fit(data_us_normali)
synthetic_us_anomalie = copula_us_anomalie.sample(len(data_us_anomalie))
synthetic_us_normali = copula_us_normali.sample(len(data_us_normali))
alpha = 0.2
beta = 1.9
media_durata_anomalie = 157 * alpha
media_durata_normali = 115 * alpha
sigma_anomalie = 1 * beta
sigma_normali = 1 * beta
synthetic_us_anomalie['Durata'] = np.random.lognormal(mean=np.log(media_durata_anomalie), sigma=sigma_anomalie, size=len(synthetic_us_anomalie))
synthetic_us_normali['Durata'] = np.random.lognormal(mean=np.log(media_durata_normali), sigma=sigma_normali, size=len(synthetic_us_normali))
synthetic_us_anomalie['Tipo_Evento'] = "Anomalia"
synthetic_us_normali['Tipo_Evento'] = "Funzionamento Normale"
synthetic_us_anomalie['Tipo_Evento_Classificato'] = 0
synthetic_us_normali['Tipo_Evento_Classificato'] = 1
warnings.filterwarnings("default", category=RuntimeWarning)

#sposto colonna durata in prima posizione e salvo i dataset
col_durata_anomalie = synthetic_us_anomalie.pop('Durata')
synthetic_us_anomalie.insert(0, 'Durata', col_durata_anomalie)
col_durata_normali = synthetic_us_normali.pop('Durata')
synthetic_us_normali.insert(0, 'Durata', col_durata_normali)
synthetic_us_anomalie.to_csv('dataset_sintetico_anomalie.csv', index=False)
synthetic_us_normali.to_csv('dataset_sintetico_normali.csv', index=False)

#calcolo media e mediana dei tempi di interarrivo delle anomalie e del funzionamento normale
mean_filtered_synthetic_anomalies = synthetic_us_anomalie['Durata'].mean()
median_filtered_synthetic_anomalies = synthetic_us_anomalie['Durata'].median()
print(f"Media dei tempi di interarrivo delle anomalie filtrate: {mean_filtered_synthetic_anomalies}")
print(f"Mediana dei tempi di interarrivo delle anomalie filtrate: {median_filtered_synthetic_anomalies}")
mean_filtered_synthetic_normal = synthetic_us_normali['Durata'].mean()
median_filtered_synthetic_normal = synthetic_us_normali['Durata'].median()
print(f"Media dei tempi di interarrivo del funzionamento normale filtrato: {mean_filtered_synthetic_normal}")
print(f"Mediana dei tempi di interarrivo del funzionamento normale filtrato: {median_filtered_synthetic_normal}")

#matrice di correlazione dei dati sintetici con copula gaussiana
numerical_columns = synthetic_us_anomalie.select_dtypes(include=['float64', 'int64']).columns
correlation_matrix = synthetic_us_anomalie[numerical_columns].corr()
plt.figure(figsize=(20, 15))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', linewidths=0.5)
plt.title('Matrice di Correlazione delle Feature Numeriche')
plt.show()

#matrice di #matrice di correlazione dei dati sintetici con copula gaussiana
numerical_columns = synthetic_us_normali.select_dtypes(include=['float64', 'int64']).columns
correlation_matrix = synthetic_us_normali[numerical_columns].corr()
plt.figure(figsize=(20, 15))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', linewidths=0.5)
plt.title('Matrice di Correlazione delle Feature Numeriche')
plt.show()

#funzione per plottare la matrice di confusione
def plot_confusion_matrix(y_true, y_pred, title):
    cm = confusion_matrix(y_true, y_pred, labels=['Anomalia', 'Funzionamento Normale'])
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=['Anomalia', 'Funzionamento Normale'],
                yticklabels=['Anomalia', 'Funzionamento Normale'])
    plt.xlabel('Predizione')
    plt.ylabel('Valore Reale')
    plt.title(title)
    plt.show()

#testiamo il modello sulle anomalie sintetiche
X_sintetici_anomalie = synthetic_us_anomalie.drop(columns=[
    'Tipo_Evento_Classificato','Timestamp', 'Timestamp chiusura'], errors='ignore')
non_numeric_columns_sintetici_anomalie = X_sintetici_anomalie.select_dtypes(include=['object']).columns
X_sintetici_anomalie = X_sintetici_anomalie.drop(columns=non_numeric_columns_sintetici_anomalie)
y_sintetici_true_anomalie = ['Anomalia'] * len(X_sintetici_anomalie)
y_pred_sintetici_anomalie = rf_model.predict(X_sintetici_anomalie)
accuracy_sintetici_anomalie = accuracy_score(y_sintetici_true_anomalie, y_pred_sintetici_anomalie)
classification_rep_sintetici_anomalie = classification_report(y_sintetici_true_anomalie, y_pred_sintetici_anomalie,
                                                              target_names=['Anomalia', 'Funzionamento Normale'],
                                                              zero_division=1)
print(f"Accuratezza del modello sui dati sintetici (anomalie): {accuracy_sintetici_anomalie:.4f}")
print("Report di classificazione sui dati sintetici (anomalie):")
print(classification_rep_sintetici_anomalie)
plot_confusion_matrix(y_sintetici_true_anomalie, y_pred_sintetici_anomalie, 'Confusion Matrix - Anomalie Sintetiche')

#testiamo il modello sui funzionamenti normali sintetiche
X_sintetici_normali = synthetic_us_normali.drop(columns=[
    'Tipo_Evento_Classificato', 'Timestamp', 'Timestamp chiusura'], errors='ignore')
non_numeric_columns_sintetici_normali = X_sintetici_normali.select_dtypes(include=['object']).columns
X_sintetici_normali = X_sintetici_normali.drop(columns=non_numeric_columns_sintetici_normali)
y_sintetici_true_normali = ['Funzionamento Normale'] * len(X_sintetici_normali)
y_pred_sintetici_normali = rf_model.predict(X_sintetici_normali)
accuracy_sintetici_normali = accuracy_score(y_sintetici_true_normali, y_pred_sintetici_normali)
classification_rep_sintetici_normali = classification_report(y_sintetici_true_normali, y_pred_sintetici_normali,
                                                             target_names=['Anomalia', 'Funzionamento Normale'],
                                                             zero_division=1)
print(f"Accuratezza del modello sui dati sintetici (funzionamenti normali): {accuracy_sintetici_normali:.4f}")
print("Report di classificazione sui dati sintetici (funzionamenti normali):")
print(classification_rep_sintetici_normali)
plot_confusion_matrix(y_sintetici_true_normali, y_pred_sintetici_normali, 'Confusion Matrix - Funzionamenti Normali Sintetici')

# rappresentazione grafica delle distribuzioni
dataset_cleaned = pd.read_csv('dataset_cleaned.csv')
dataset_sintetico_anomalie = pd.read_csv('dataset_sintetico_anomalie.csv')
dataset_sintetico_normali = pd.read_csv('dataset_sintetico_normali.csv')
common_columns = dataset_sintetico_anomalie.columns.intersection(dataset_sintetico_normali.columns)
filtered_dataset_cleaned = dataset_cleaned[common_columns]
plt.rcParams["figure.figsize"] = (10, 6)
for column in common_columns:
    plt.figure()
    plt.hist(filtered_dataset_cleaned[column].dropna(), bins=30, alpha=0.5, label='Dataset Cleaned', density=True)
    plt.hist(dataset_sintetico_anomalie[column].dropna(), bins=30, alpha=0.5, label='Dataset Sintetico Anomalie',
             density=True)
    plt.hist(dataset_sintetico_normali[column].dropna(), bins=30, alpha=0.5, label='Dataset Sintetico Normali',
             density=True)
    plt.title(f'Distribution of {column}')
    plt.xlabel(column)
    plt.ylabel('Density')
    plt.legend()
    plt.show()


# script per la generazione continua di dati sintetici
import json
import os
import time
import random
import logging
import warnings
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from random import uniform
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from copulas.multivariate import GaussianMultivariate

# Configure logging for detailed information
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables for Kafka broker and topic name
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'train-sensor-data')

# Validate that KAFKA_BROKER and TOPIC_NAME are set
if not KAFKA_BROKER:
    raise ValueError("Environment variable KAFKA_BROKER is missing.")
if not TOPIC_NAME:
    raise ValueError("Environment variable TOPIC_NAME is missing.")

# Kafka producer configuration
conf_prod = {
    'bootstrap.servers': KAFKA_BROKER,
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': lambda x, ctx: json.dumps(x).encode('utf-8')
}
producer = SerializingProducer(conf_prod)

columns_to_generate = [
    'Durata', 'CabEnabled_M1', 'CabEnabled_M8', 'ERTMS_PiastraSts', 'HMI_ACPntSts_T2', 'HMI_ACPntSts_T7',
    'HMI_DCPntSts_T2', 'HMI_DCPntSts_T7', 'HMI_Iline', 'HMI_Irsts_T2', 'HMI_Irsts_T7', 'HMI_VBatt_T2',
    'HMI_VBatt_T4', 'HMI_VBatt_T5', 'HMI_VBatt_T7', 'HMI_Vline', 'HMI_impSIL', 'LineVoltType', 'MDS_LedLimVel',
    'MDS_StatoMarcia', '_GPS_LAT', '_GPS_LON', 'ldvvelimps', 'ldvveltreno', 'usB1BCilPres_M1', 'usB1BCilPres_M3',
    'usB1BCilPres_M6', 'usB1BCilPres_M8', 'usB1BCilPres_T2', 'usB1BCilPres_T4', 'usB1BCilPres_T5', 'usB1BCilPres_T7',
    'usB2BCilPres_M1', 'usB2BCilPres_M3', 'usB2BCilPres_M6', 'usB2BCilPres_M8', 'usB2BCilPres_T2', 'usB2BCilPres_T4',
    'usB2BCilPres_T5', 'usB2BCilPres_T7', 'usBpPres', 'usMpPres'
]

all_columns = [
                  'Flotta', 'Veicolo', 'Codice', 'Nome', 'Descrizione', 'Test', 'Timestamp', 'Timestamp chiusura',
                  'Durata',
                  'Posizione', 'Sistema', 'Componente', 'Latitudine', 'Longitudine', 'Contemporaneo',
                  'Timestamp segnale'
              ] + columns_to_generate + ['Tipo_Evento', 'Tipo_Evento_Classificato']

# Load and prepare synthetic data
df_originale = pd.read_csv('dataset_cleaned.csv')
df_anomalie = df_originale[df_originale['Tipo_Evento'] == 'Anomalia']
df_normali = df_originale[df_originale['Tipo_Evento'] == 'Funzionamento Normale']
columns_to_check = ['Descrizione', 'Timestamp', 'Codice', 'Nome']
df_deduplicated_anomalies = df_anomalie.loc[(df_anomalie[columns_to_check] != df_anomalie[columns_to_check].shift()).any(axis=1)]
df_deduplicated_normals = df_normali.loc[(df_normali[columns_to_check] != df_normali[columns_to_check].shift()).any(axis=1)]

timestamp_iniziale = pd.Timestamp.now()

def rigeneraDatasetSintetico(file_anomalie, file_normali, df_anomalie, df_normali, num_righe):
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    df_anomalie_copula = df_anomalie.drop(columns=['Durata'], errors='ignore')
    df_normali_copula = df_normali.drop(columns=['Durata'], errors='ignore')

    copula_anomalie = GaussianMultivariate()
    copula_anomalie.fit(df_anomalie_copula)
    synthetic_anomalie = copula_anomalie.sample(num_righe // 2)

    copula_normali = GaussianMultivariate()
    copula_normali.fit(df_normali_copula)
    synthetic_normali = copula_normali.sample(num_righe // 2)

    alpha = 0.2
    beta = 1.9
    media_durata_anomalie = 157 * alpha
    media_durata_normali = 115 * alpha
    sigma_anomalie = 1 * beta
    sigma_normali = 1 * beta

    synthetic_anomalie['Durata'] = np.random.lognormal(mean=np.log(media_durata_anomalie), sigma=sigma_anomalie, size=len(synthetic_anomalie))
    synthetic_normali['Durata'] = np.random.lognormal(mean=np.log(media_durata_normali), sigma=sigma_normali, size=len(synthetic_normali))

    for df in [synthetic_anomalie, synthetic_normali]:
        df['Flotta'] = 'ETR700'
        df['Veicolo'] = 'e700_4801'
        df['Test'] = 'N'
        df['Timestamp'] = pd.Timestamp.now()
        df['Timestamp chiusura'] = df['Timestamp'] + pd.to_timedelta(df['Durata'], unit='s')
        df['Posizione'] = np.nan
        df['Sistema'] = 'VEHICLE'
        df['Componente'] = 'VEHICLE'
        df['Timestamp segnale'] = np.nan

    for col in all_columns:
        if col not in synthetic_anomalie.columns:
            synthetic_anomalie[col] = np.nan
        if col not in synthetic_normali.columns:
            synthetic_normali[col] = np.nan

    synthetic_anomalie = synthetic_anomalie.round(2)
    synthetic_normali = synthetic_normali.round(2)

    synthetic_anomalie = synthetic_anomalie[all_columns]
    synthetic_normali = synthetic_normali[all_columns]

    synthetic_anomalie.to_csv(file_anomalie, index=False)
    synthetic_normali.to_csv(file_normali, index=False)
    warnings.filterwarnings("default", category=RuntimeWarning)
    print(f"Dataset sintetici rigenerati: {file_anomalie} e {file_normali}")

def produce_message(data):
    """
    Produce a message to Kafka for a specific sensor type.

    Args:
        data (str): The type of sensor to produce a message for.
    """
    try:
        logging.info(f"Producing message >>> {data}")
        producer.produce(topic=TOPIC_NAME, value=data) # Send the message to Kafka
        producer.flush()  # Ensure the message is immediately sent
        logging.info(f"Message sent >>> {data}")
    except Exception as e:
        print(f"Error while producing message: {e}")

# Continuous data generation function
def generazioneContinuataDataset(file_anomalie, file_normali, df_anomalie, df_normali, num_righe_rigenerazione=1000):
    rigeneraDatasetSintetico(file_anomalie, file_normali, df_anomalie, df_normali, num_righe_rigenerazione)
    dataset_anomalie = pd.read_csv(file_anomalie)
    dataset_normali = pd.read_csv(file_normali)

    while True:
        data_to_send = dataset_anomalie.iloc[0].to_dict() if np.random.rand() < 0.5 else dataset_normali.iloc[0].to_dict()
        data_to_send['Timestamp'] = str(data_to_send['Timestamp'])
        produce_message(data_to_send)
        dataset_anomalie = dataset_anomalie.iloc[1:] if np.random.rand() < 0.5 else dataset_anomalie
        dataset_normali = dataset_normali.iloc[1:] if not np.random.rand() < 0.5 else dataset_normali
        time.sleep(1)

if __name__ == '__main__':
    generazioneContinuataDataset('dataset_sintetico_anomalie.csv', 'dataset_sintetico_normali.csv',
                                 df_deduplicated_anomalies[columns_to_generate],
                                 df_deduplicated_normals[columns_to_generate])

