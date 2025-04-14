# Piano di Conversione Architettura
**Da centralizzata command-approach a distribuita**

## Panoramica
Trasformazione dell'architettura per permettere a ogni treno di avere il proprio server e comunicare via HTTP.

## Status
- ✅ **Completati**: 1
- 🔲 **Da fare**: 2, 3, 4, 5, 6, 7, 8

## Obiettivi
- Ogni treno ha il suo server che gestisce il suo consumatore
- Il manager centrale comunica verso i treni usando HTTP
- Ogni treno può operare in modo indipendente
- Il sistema è più scalabile e resiliente

## Piano di Implementazione

### 1. ✅ Creazione del Train Server Component
- Creare il server component per ogni treno che gestisce richieste HTTP
- **Struttura della directory del train server**:
  ```
  train_server/
  ├── app.py
  ├── Dockerfile
  ├── requirements.txt
  └── train_controller.py
  ```
- Implementare un server FastAPI in app.py che:
  - Espone gli endpoint per il controllo del treno
  - Gestisce gli aggiornamenti di configurazione
  - Gestisce il processo di consumer del treno
- Il Train Server implementato con FastAPI, per ottenere performance migliori e documentazione automatica per le API

### 2. ✅ Modifica del Consumer Manager
- Refactoring della classe ConsumerManager:
  - Rimuovere l'esecuzione diretta di comandi
  - Aggiungere la funzionalità client in HTTP per comunicare con i server dei treni
  - Aggiornare l'approccio di gestione delle configurazioni

### 3. ✅ Aggiornare la configurazione Docker
- Modificare il docker-compose.yml per:
  - Aggiungere un servizio Train Server per ogni treno
  - Aggiornare il networking per permettere la comunicazione HTTP tra le componenti
  - Configurare le dependencies necessarie per il servizio

### 4. 🔲 Implementazione della comunicazione HTTP
- Creazione di un nuovo modulo di comunicazione per l'interazione basata su HTTP:
  - Rimpiazzare la comunicazione basata su comandi via Kafka con richieste HTTP
  - Implementare meccanismi di gestione degli errori e reiterazioni di esecuzioni fallite
  - Aggiungere autenticazione/autorizzazione per comunicazione sicura

### 5. 🔲 Aggiornare l'implementazione del Consumer
- Modificare il consumer per:
  - Accettare configurazioni via endpoint HTTP
  - Segnalare status e metriche via HTTP
  - Gestire shutdown controllati tramite segnali HTTP

### 6. 🔲 Implementazione di service discovery
- Aggiungere un meccanismo di service discovery per:
  - Permettere ai treni di registrarsi autonomamente
  - Abilitare al manager centrale di scoprire treni disponibili
  - Supportare aggiunta/rimozione dinamica di treni

### 7. 🔲 Aggiornare la gestione delle configurazioni
- Modificare il sistema di configurazioni per:
  - Supportare configurazioni distribuite
  - Permettere configurazioni di treni singoli via HTTP
  - Implementare validazioni di configurazioni

### 8. 🔲 Implementare il monitoraggio della salute
- Aggiungere endpoint di controllo salute per:
  - Monitorare lo status dei treni
  - Identificare fallimenti
  - Supportare il ripristino automatico
