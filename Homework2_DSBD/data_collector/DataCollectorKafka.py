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

# Configurazione del producer Kafka
producer_config = {
        'bootstrap.servers': 'kafka:9092',  # Kafka broker address
        'acks': 'all',  # Ensure all in-sync replicas acknowledge the message
        
        # In questo caso ha poco senso questa impostazione. Il datacollector non deve inviare
        # molteplici messaggi per i quali ha senso preservare in ricezione l'ordine di trasmissione
        #'max.in.flight.requests.per.connection': 1,  
        'retries': 3 
    }
producer = Producer(producer_config)
topic = 'to-alert-system'



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
        producer.produce(topic, json.dumps(message).encode('utf-8'))  # Accoda il messaggio nel buffer interno del producer prima di inviarlo al broker in batch (per ottimizzare il throughput)
        producer.flush()  # Invia tutta la coda di messaggi del buffer interno del producer. Blocca l'esecuzione, attende la consegna di tutti i messaggi.
        print(f"Notifica inviata a Kafka: {message}")
    except Exception as e:
       print(f"Errore durante l'invio a Kafka: {e}")

# Funzione principale per il DataCollector
def collect_data():
    """
    Per ogni utente nel database, recupera il valore più recente del ticker tramite yfinance
    e aggiorna il database. Invia una notifica a Kafka al completamento.
    """
    try:
        # Connessione al database
        conn = CQRS.connect_db()
        cursor = conn.cursor(dictionary=True)

        read_service = CQRS.QueryHandler(cursor)
        write_service = CQRS.CommandHandler(conn)

        # Pianifica la rimozione dei dati non più osservati da alcun utente
        def emptyDati():
            try:
                cursor.execute("LOCK TABLES dati WRITE, utenti READ;")
                write_service.deleteUnobservedData()
                cursor.execute("UNLOCK TABLES;")
                
                print("Pulizia dei dati non osservati completata.")
            except Error as e:
                print(f"Errore durante la pulizia dei dati: {e}")

        # Pianifica la pulizia ogni 24 ore
        schedule.every(24).hours.do(emptyDati)

        while True:
            schedule.run_pending()  # Esegue le attività pianificate

            # Recupera i tickers dalla tabella Utenti
            users = read_service.getUsers()

            # Cache per evitare chiamate duplicate a yfinance per lo stesso ticker
            ticker_cache = {}

            for user in users:
                id = user['id']
                ticker = user['ticker']

                try:
                    # Recupera i dati del ticker, evitando duplicati
                    if ticker not in ticker_cache:
                        # Chiama yfinance tramite il Circuit Breaker
                        data = circuit_breaker.call(fetch_ticker_value, ticker)
                        print(f"Dati recuperati per {ticker}: {data}")

                        # Inserisci il record nella tabella `dati`
                        data_id = write_service.insertData(ticker, data)

                        # Aggiorna la cache con l'ID appena creato
                        ticker_cache[ticker] = data_id

                    # Inserisci nella tabella `sessioni_utenti`
                    write_service.insertDataSession(id, ticker_cache[ticker])

                except CircuitBreakerOpenException:
                    print(f"Fetch del ticker '{ticker}' impossibilitato: Circuit Breaker aperto.")
                except Exception as e:
                    print(f"Errore durante il fetch del ticker '{ticker}': {e}")
                finally:
                    cursor.execute("UNLOCK TABLES;")
                    conn.commit()  # Salva le modifiche nel database

            # Invia notifica a Kafka
            notify_kafka()

            # Attendi 60 secondi prima della prossima iterazione
            time.sleep(60)

    except Error as e:
        print(f"Errore di connessione al database: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    collect_data()
