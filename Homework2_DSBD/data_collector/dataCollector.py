import os
import yfinance as yf           
import mysql.connector          
from mysql.connector import Error   
from circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
import time
import schedule     

# Connettersi al database MySQL
def connect_db():
    return mysql.connector.connect(   
        host=os.getenv("DB_HOST"),  
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

# Funzione per ottenere il valore corrente di un ticker da yfinance
def fetch_ticker_value(ticker):
    """
    Fetches the latest stock value for the given ticker using yfinance.

    Parameters:
    - ticker (str): The stock ticker to fetch.

    Returns:
    - dict: Contains 'value' (latest stock price) and 'date' (current time).
    """
    stock = yf.Ticker(ticker)
    history = stock.history(period="1d")
    if history.empty:
        raise Exception(f"Nessun dato disponibile per il ticker {ticker}")
    
    return {"date": history.index[0], "open": history.iloc[0,0], "high" : history.iloc[0,1], "low" : history.iloc[0,2], "close" : history.iloc[0,3], "volume": history.iloc[0,4], "dividends" : history.iloc[0,5], "splits" : history.iloc[0,6]}




# Circuit Breaker per proteggere le chiamate a yfinance
circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=10)




def collect_data():
    """
    Per ogni utente ritira tramite yfinance il record più recente relativo al suo ticker.
    """
    try:
        # Connessione al database
        conn = connect_db()
        cursor = conn.cursor(dictionary=True)

        # Schedulo l'eliminazione periodica dei dati non più osservati da alcun utente
        def emptyDati() :
            try:
                cursor.execute("LOCK TABLES dati WRITE, utenti READ;") 
                cursor.execute("DELETE FROM dati WHERE ticker NOT IN (SELECT DISTINCT ticker FROM utenti);")
                cursor.execute("UNLOCK TABLES;")
                conn.commit()
            except Error as e:
                print(f"Database error: {e}")

        
        # Schedulata ogni 24 ore
        schedule.every(24).hours.do(emptyDati)
        
        while True:

            schedule.run_pending()

            # Recupera i tickers dalla tabella Utenti.          
            # Faccio il lock sulle tabelle per evitare collisioni con il server
            cursor.execute("LOCK TABLES utenti READ, dati WRITE, sessioni_utenti WRITE;")
            
            cursor.execute("SELECT id, email, ticker FROM utenti;")
            users = cursor.fetchall()

            # Dizionario di ticker per evitare di ripetere il fetch di ticker value già fetchati all'interno di una iterazione.
            # Conterrà record del tipo -> 
            # ticker_dickt = {
            #       ticker : id_dato  (id del dato nella tabella Dati)
            #}
            ticker_cache = {}

            for user in users:
                
                id = user['id']
                ticker = user['ticker']

                try:

                        if ticker not in ticker_cache :

                            # Se non ho ancora fetchato dati per questo ticker richiamo l'API di yfinance e inserisco
                            # i dati nella tabella Dati. Dopodichè aggiorno ticker_cache in modo tale da evitare di
                            # rifare la chiamata a yfinance e poter direttamente fare l'unico insert necessario,
                            # ovvero quello nella tabella Sessioni_utenti

                            # Usa il Circuit Breaker per chiamare fetch_ticker_value
                            data = circuit_breaker.call(fetch_ticker_value, ticker)
                            print(f"Fetched data for {ticker}: {data}")

                            # Inserisci il record di dati nella tabella dati               
                            cursor.execute(
                                "INSERT INTO dati (ticker, date, open, high, low, close, volume, dividends, splits) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);",
                                (ticker, data['date'].to_pydatetime(), float(data['open']), float(data['high']), float(data['low']), float(data['close']), int(data['volume']), float(data['dividends']), float(data['splits']))
                            )

                            # Recupero l'id della riga appena creata che è stato generato automaticamente e aggiorno la cache di ticker
                            ticker_cache[ticker] = cursor.lastrowid

                        # Dopodichè crea l'entry corrispondente nella tabella sessioni_utenti. Se il ticker è già stato fetchato fai solo questo insert.
                        cursor.execute(
                            "INSERT INTO sessioni_utenti(id_utente, id_dato) VALUES (%s, %s);", (id, ticker_cache[ticker])
                        )
                        
                        cursor.execute("UNLOCK TABLES;")
                        
                        conn.commit()

                except CircuitBreakerOpenException:
                        print(f"Fetch del ticker '{ticker}' impossibilitato. Yfinance non risponde: Circuit breaker aperto.")
                except Exception as e:
                        print(f"Error fetching data for {ticker}: {e}")
            
            # Aspetta 60 secondi prima della prossima iterazione
            time.sleep(60)

    except Error as e:
        print(f"Database error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    collect_data()
