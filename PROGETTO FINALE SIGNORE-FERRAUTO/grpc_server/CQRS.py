import os
import mysql.connector
from typing import Any, List, Dict
import yfinance as yf

# Settaggio connessione al database MySQL. os.getenv() mi permette di ottenere 
# il valore di quelle variabili d'ambiente definite nel docker-compose.yml

DB_CONFIG = {
    "host" : os.getenv("DB_HOST", "localhost"), 
    "user" : os.getenv("DB_USER", "my_user"),
    "password" : os.getenv("DB_PASSWORD", "my_pass"),
    "database" : os.getenv("DB_NAME", "my_db")
        }

#  ------------------------   USER DEFINED EXCEPTIONS   -------------------------------------------------
class EmailAlreadyExistsException(Exception):
    def __init__(self, email):
       
        self.email = email
        super().__init__(f"L'email '{email}' è già stata registrata.")

        
class TickerNotValidException(Exception):
    def __init__(self, ticker):     
        self.ticker = ticker
        super().__init__(f"Il ticker '{ticker}' non è valido.")


class UserDoesNotExistsException(Exception):
    def __init__(self, message="Email non trovata." ):     
        super().__init__(message)


class NoDataFoundException(Exception):
    def __init__(self, message="Nessun dato trovato." ):     
        super().__init__(message)


class NotEnoughDataException(Exception):
    def __init__(self, message="La quantità di dati inserita eccede quella presente." ):     
        super().__init__(message)


class InvalidInputTypeException(Exception):
    def __init__(self, message="Il valore inserito non è un numero valido."):
        super().__init__(message)


class ThresholdsViolatedException(Exception):
    def __init__(self, message="'High record' deve avere un valore maggiore di 'low record'."):
        super().__init__(message)
#------------------------------------------------------------------------------------------------------------ 

def isValidTicker(ticker):
    """Ritorna True se il ticker è valido, False altrimenti."""
    stock = yf.Ticker(ticker)
    history = stock.history(period="1d")
    return not history.empty 

#---------------------------------- CQRS ----------------------------------------------------------------

class QueryHandler:

    def __init__(self):
        self.db_config = DB_CONFIG

    def fetch_all(self, query: str, params: tuple = ()): # ritorna una lista dove ogni elemnto è una riga del db
        connection = mysql.connector.connect(**self.db_config)
        cursor = connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params)
            results = cursor.fetchall()
            return results
        except mysql.connector.Error as err:
            print(f"Errore durante l'esecuzione della query di lettura: {err}")
            raise err
        finally:
            cursor.close()
            connection.close()
        

    def getUserByEmail(self, email):
            query = "SELECT * FROM utenti WHERE email = %s;"
            res = self.fetch_all(query, (email,))
            return res[0] if res else None


    def getLatestValue(self, email):
        
        try:
            # Controlla se l'utente inserito esiste  
            if self.getUserByEmail(email) is None:
                raise UserDoesNotExistsException
      
            query = """
                    SELECT dati.ticker, date, open, high, low, close, volume, dividends, splits 
                    FROM utenti JOIN sessioni_utenti ON utenti.id = sessioni_utenti.id_utente 
                    JOIN dati ON sessioni_utenti.id_dato = dati.id 
                    WHERE utenti.email = %s ORDER BY date DESC LIMIT 1;
                    """

            res = self.fetch_all(query, (email,))
            if res:
                record = res[0]
                value=(str(record['ticker']), str(record['date']), float(record['open']), float(record['high']), float(record['low']), float(record['close']), int(record['volume']), float(record['dividends']), float(record['splits']))
                return (True, value)
            else:
                raise NoDataFoundException
        
        except(UserDoesNotExistsException, NoDataFoundException, mysql.connector.Error) as e:
            return (False, str(e))
    
    def getUserData(self, email):
        query = "SELECT * FROM utenti JOIN sessioni_utenti ON utenti.id = sessioni_utenti.id_utente WHERE utenti.email = %s;"
        res = self.fetch_all(query, (email,))
        return res

    def getAverageValue(self, email, count):
        try:

            # Controlla che request.count sia un parametro valido
            if not isinstance(count, int) or count <= 0 :
                raise InvalidInputTypeException
        
            # Controlla se l'utente inserito esiste  
            if self.getUserByEmail(email) is None:
                raise UserDoesNotExistsException

        
            # Controlla se esistono n=count dati
            if len(self.getUserData(email)) < count:
                raise NotEnoughDataException

            query = """
                    SELECT AVG(open), AVG(high), AVG(low), AVG(close), AVG(volume), AVG(dividends), AVG(splits) 
                    FROM (SELECT open, high, low, close, volume, dividends, splits 
                            FROM utenti JOIN sessioni_utenti ON utenti.id = sessioni_utenti.id_utente JOIN dati ON sessioni_utenti.id_dato = dati.id 
                            WHERE utenti.email = %s 
                            ORDER BY dati.date DESC LIMIT %s) AS subquery;
                    """
            res = self.fetch_all(query, (email, count))

            if res:
             record = res[0]
             value=(float(record['AVG(open)']), float(record['AVG(high)']), float(record['AVG(low)']), float(record['AVG(close)']), int(record['AVG(volume)']), float(record['AVG(dividends)']), float(record['AVG(splits)']))
             return( True, value)
            
            else:
                return None
        except (InvalidInputTypeException, UserDoesNotExistsException, NotEnoughDataException, mysql.connector.Error) as e:
            return (False, str(e))

        




class CommandHandler:
    def __init__(self):
        self.db_config = DB_CONFIG
        self.read_service = QueryHandler()

    def execute(self, query: str, params: tuple = ()):
        connection = mysql.connector.connect(**self.db_config)
        cursor = connection.cursor()
        try:
            cursor.execute(query, params)
            connection.commit()
        except mysql.connector.Error as err:
            connection.rollback()
            raise err
        finally:
            cursor.close()
            connection.close()


    def registerUser(self, email, ticker, low_value, high_value):
        
        try:
            # Controlla se l'email esiste già   
            if self.read_service.getUserByEmail(email):
                raise EmailAlreadyExistsException(email)

            # Controlla se il ticker è valido
            if not isValidTicker(ticker):
                raise TickerNotValidException(ticker)
                
            # Verifica che valga la condizione "high_value is NULL or high_value > low_value"
            if high_value is not None and low_value is not None and high_value <= low_value:
                raise ThresholdsViolatedException    
        
            query = "INSERT INTO utenti (email, ticker, low_value, high_value) VALUES (%s, %s, %s, %s);"
            self.execute(query, (email, ticker, low_value, high_value))

            return (True, "Utente registrato con successo.")
          
        except (EmailAlreadyExistsException, TickerNotValidException, ThresholdsViolatedException, mysql.connector.Error) as e:
            return (False, str(e))
        
    def updateUser(self, email, ticker, low_value, high_value):

        try:
             # Controlla se l'utente inserito esiste  
            if self.read_service.getUserByEmail(email) is None:
               raise UserDoesNotExistsException

            # Controlla se il ticker è valido
            if not isValidTicker(ticker):
                raise TickerNotValidException(ticker)
                
            # Verifica che valga la condizione "high_value is NULL or high_value > low_value"
            if high_value is not None and low_value is not None and high_value <= low_value:
                raise ThresholdsViolatedException
            
            query = "UPDATE utenti SET ticker = %s, low_value = %s, high_value = %s WHERE email = %s;"
            self.execute(query, (ticker, low_value, high_value, email))

            return(True, "Utente aggiornato con successo.")
            
        except(UserDoesNotExistsException, TickerNotValidException, ThresholdsViolatedException, mysql.connector.Error) as e:
            return (False, str(e))

     

    def deleteUser(self, email):
        try:
            # Controlla se l'utente inserito esiste  
            if self.read_service.getUserByEmail(email) is None:
                raise UserDoesNotExistsException
            
            query = "DELETE FROM utenti WHERE email = %s;"
            self.execute(query, (email,))

            return(True, "Utente cancellato con successo.")

        except (UserDoesNotExistsException, mysql.connector.Error) as e:
            return (False, str(e))


