import pandas as pd
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from scipy.stats import lognorm
import pickle
import threading
import time



# Constants:

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
    'Flotta', 'Veicolo', 'Codice', 'Nome', 'Descrizione', 'Test', 'Timestamp', 'Timestamp chiusura', 'Durata', 
    'Posizione', 'Sistema', 'Componente', 'Latitudine', 'Longitudine', 'Contemporaneo', 'Timestamp segnale'
] + columns_to_generate + ['Tipo_Evento', 'Tipo_Evento_Classificato']

# load the copula objects later:
with open('copula_anomalie.pkl', 'rb') as f:
    copula_anomalie = pickle.load(f)

with open('copula_normali.pkl', 'rb') as f:
    copula_normali = pickle.load(f)


def thread_anomalie(name, lognormal_anomalie):
  while True:
    synthetic_anomalie = copula_anomalie.sample(1)
    durata_anomalia = lognormal_anomalie.rvs(size=1)
    synthetic_anomalie['Durata'] = durata_anomalia
    synthetic_anomalie['Flotta'] = 'ETR700'
    synthetic_anomalie['Veicolo'] = 'e700_4801'
    synthetic_anomalie['Test'] = 'N'
    synthetic_anomalie['Timestamp'] = pd.Timestamp.now()
    synthetic_anomalie['Timestamp chiusura'] = synthetic_anomalie['Timestamp'] + pd.to_timedelta(synthetic_anomalie['Durata'], unit='s')
    synthetic_anomalie['Posizione'] = np.nan
    synthetic_anomalie['Sistema'] = 'VEHICLE'
    synthetic_anomalie['Componente'] = 'VEHICLE'
    synthetic_anomalie['Timestamp segnale'] = np.nan

    for col in all_columns:
        if col not in synthetic_anomalie.columns:
            synthetic_anomalie[col] = np.nan
    
    synthetic_anomalie = synthetic_anomalie.round(2)
    synthetic_anomalie = synthetic_anomalie[all_columns]
    print(f"Nuova anomalia generata: {synthetic_anomalie}")
    time.sleep(durata_anomalia[0])


def thread_normali(name, lognormal_normali):
  while True:
    synthetic_normali = copula_normali.sample(1)
    durata_normale = lognormal_normali.rvs(size=1)
    synthetic_normali['Durata'] = durata_normale
    synthetic_normali['Flotta'] = 'ETR700'
    synthetic_normali['Veicolo'] = 'e700_4801'
    synthetic_normali['Test'] = 'N'
    synthetic_normali['Timestamp'] = pd.Timestamp.now()
    synthetic_normali['Timestamp chiusura'] = synthetic_normali['Timestamp'] + pd.to_timedelta(synthetic_normali['Durata'], unit='s')
    synthetic_normali['Posizione'] = np.nan
    synthetic_normali['Sistema'] = 'VEHICLE'
    synthetic_normali['Componente'] = 'VEHICLE'
    synthetic_normali['Timestamp segnale'] = np.nan

    for col in all_columns:
        if col not in synthetic_normali.columns:
            synthetic_normali[col] = np.nan
    
    synthetic_normali = synthetic_normali.round(2)
    synthetic_normali = synthetic_normali[all_columns]
    print(f"Nuova diagnostica generata: {synthetic_normali}")
    time.sleep(durata_normale[0])

# receive the alpha
if __name__ == '__main__':

    alpha = 0.2
    beta = 1.9
    media_durata_anomalie = 157 * alpha
    media_durata_normali = 115 * alpha
    sigma_anomalie = 1 * beta
    sigma_normali = 1 * beta

    lognormal_anomalie = lognorm(s=sigma_anomalie, scale=np.exp(np.log(media_durata_anomalie)))

    lognormal_normali = lognorm(s=sigma_normali, scale=np.exp(np.log(media_durata_normali)))


        
    thread1 = threading.Thread(target=thread_anomalie, args=(1, lognormal_anomalie))
    thread2 = threading.Thread(target=thread_normali, args=(2, lognormal_normali))

    # Set daemon to True
    thread1.daemon = True
    thread2.daemon = True

    # Start the threads
    thread1.start()
    thread2.start()

    # Wait for the threads to finish
    thread1.join()
    thread2.join()