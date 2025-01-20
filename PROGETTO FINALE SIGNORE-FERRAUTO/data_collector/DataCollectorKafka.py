import os
import yfinance as yf
import mysql.connector
from mysql.connector import Error
from circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
from confluent_kafka import Producer  # Kafka Producer
import json
import time
import schedule
import CQRS
import prometheus_client 
import socket

# Configurazione del producer Kafka
producer_config = {
        'bootstrap.servers': 'kafka:9092',  # Kafka broker address
        'acks': 'all',  # Ensure all in-sync replicas acknowledge the message
        'retries': 3 
    }
producer = Producer(producer_config)
topic = 'to-alert-system'

#----------------- METRICHE PER WHITEBOX MONITORING ------------------------------
my_service = 'data_collector'
my_hostname = socket.gethostname()

DB_UPDATE_DURATION = prometheus_client.Gauge(
    'db_update_duration_seconds', 
    'Tempo impiegato per aggiornare il database',
    ['service', 'hostname'])

YFINANCE_TOT_COUNTER = prometheus_client.Counter(
    'yfinance_total_calls',
    'Numero totale di chiamate a yahoo finance',
    ['service', 'hostname']
)

YFINANCE_SUCC_COUNTER = prometheus_client.Counter(
    'yfinance_succesful_calls',
    'Numero di chiamate effettuate con successo a yahoo finance',
    ['service', 'hostname']
)
# -----------------------------------------------------------------------------------

# Funzione per ottenere il valore corrente di un ticker da yfinance
def fetch_ticker_value(ticker):
    """
    Recupera il valore più recente del ticker specificato utilizzando yfinance.
    
    Parametri:
    - ticker (str): Ticker del titolo da recuperare.

    Restituisce:
    - dict: Contiene 'date', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'splits'.
    """
    stock = yf.Ticker(ticker)
    history = stock.history(period="1d")
    if history.empty:
        raise Exception(f"Nessun dato disponibile per il ticker {ticker}")
    
    #YFINANCE_SUCC_COUNTER.labels(service=my_service, hostname=my_hostname).inc()
    return {
        "date": history.index[0],
        "open": history.iloc[0, 0],
        "high": history.iloc[0, 1],
        "low": history.iloc[0, 2],
        "close": history.iloc[0, 3],
        "volume": history.iloc[0, 4],
        "dividends": history.iloc[0, 5],
        "splits": history.iloc[0, 6]
    }

# Circuit Breaker per proteggere le chiamate a yfinance
circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=10)

# Funzione per inviare notifiche su Kafka
def notify_kafka():
    """
    Invia una notifica su Kafka per segnalare il completamento dell'aggiornamento dei ticker.
    """
    try:
        message = {'status': 'update_completed', 'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")}
        producer.produce(topic, json.dumps(message).encode('utf-8'))  
        producer.flush()  
        print(f"Notifica inviata a Kafka: {message}")
    except Exception as e:
       print(f"Errore durante l'invio a Kafka: {e}")

# Funzione principale per il DataCollector
def collect_data():
    """
    Per ogni utente nel database, recupera il valore più recente del ticker tramite yfinance
    e aggiorna il database. Invia una notifica a Kafka al completamento.
    """

    read_service = CQRS.QueryHandler()
    write_service = CQRS.CommandHandler()

    # Pianifica la rimozione dei dati non più osservati da alcun utente
    def emptyDati():    
            write_service.deleteUnobservedData()
            print("Pulizia dei dati non osservati completata.")


    # Pianifica la pulizia ogni 24 ore
    schedule.every(24).hours.do(emptyDati)

    while True:
        schedule.run_pending()  # Esegue le attività pianificate
        
        # Cache per evitare chiamate duplicate a yfinance per lo stesso ticker
        ticker_cache = {}

        # Metrica per whitebox monitoring: tempo di inizio aggiornamento database
        start_time = time.time()

        try:
            # Recupera i tickers dalla tabella Utenti
            users = read_service.getUsers()

            for user in users:
                id = user['id']
                ticker = user['ticker']

                try:
                    # Recupera i dati del ticker, evitando chiamate duplicate a yfinance per lo stesso dato durante un'iterazione del data collector
                    if ticker not in ticker_cache:
                    
                        # Chiama yfinance tramite il Circuit Breaker e incrementa le metriche  
                        YFINANCE_TOT_COUNTER.labels(service=my_service, hostname=my_hostname).inc()

                        data = circuit_breaker.call(fetch_ticker_value, ticker)

                        YFINANCE_SUCC_COUNTER.labels(service=my_service, hostname=my_hostname).inc()
                        
                        print(f"Dati recuperati per {ticker}: {data}")

                        
                        is_data = read_service.getData(ticker, data['date'])
                        
                        if not is_data:
                            
                            # Se il dato non è presente nella tabella dati lo aggiungo
                            data_id = write_service.insertData(ticker, data)
                            
                            # Creo la sessione per l'utente corrente e il nuovo dato
                            write_service.insertDataSession(id, data_id)
                            

                            # Aggiorno la cache con l'ID del dato appena creato
                            ticker_cache[ticker] = data_id
                        
                        
                        else:
                            # Dato già presente nella tabella dati ma non in ticker cache (di conseguenza inserito in un'iterazione precedente del data collector), 
                            # verifico se esiste o meno la sessione con l'utente corrente ed eventualmente la aggiungo
                            if read_service.getSession(id, is_data['id']) is None:
                                write_service.insertDataSession(id, is_data['id'])  
                        
                            
                            # Aggiorno ticker cache. So che il dato per questo ticker è già presente nella tabella dati, quindi la prossima
                            # volta non lo ritiro da yfinance
                            ticker_cache[ticker] = is_data['id']

                    else: # Dato già presente nella tabella dati e in ticker cache (di conseguenza inserito nella corrente iterazione del data collector).
                            # Devo però ancora verificare se esiste la sessione con l'utente corrente
                            if read_service.getSession(id, ticker_cache[ticker]) is None:
                                write_service.insertDataSession(id, ticker_cache[ticker])

                except CircuitBreakerOpenException:
                    print(f"Fetch del ticker '{ticker}' impossibilitato: Circuit Breaker aperto.")     
                except Exception as e:
                    print(f"Errore durante il fetch del ticker '{ticker}': {e}")
        
            
            # Imposta il tempo di aggiornamento
            DB_UPDATE_DURATION.labels(service=my_service, hostname=my_hostname).set(time.time() - start_time)

            # Invia notifica a Kafka
            notify_kafka()
        
            # Attendi 60 secondi prima della prossima iterazione
            time.sleep(60)

        except mysql.connector.Error as e:
            print(f"Errore nel database: {e}")
        



if __name__ == "__main__":
    prometheus_client.start_http_server(9999)
    collect_data()
