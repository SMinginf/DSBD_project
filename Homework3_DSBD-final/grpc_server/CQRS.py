import os
import mysql.connector
from typing import Any, List, Dict

# Settaggio connessione al database MySQL. os.getenv() mi permette di ottenere 
# il valore di quelle variabili d'ambiente definite nel docker-compose.yml

DB_CONFIG = {
    "host" : os.getenv("DB_HOST", "localhost"), 
    "user" : os.getenv("DB_USER", "my_user"),
    "password" : os.getenv("DB_PASSWORD", "my_pass"),
    "database" : os.getenv("DB_NAME", "my_db")
        }

#---------------------------------- CQRS ----------------------------------------------------------------
class CommandHandler:
    def __init__(self):
        self.db_config = DB_CONFIG

    def execute(self, query: str, params: tuple = ()):
        connection = mysql.connector.connect(**self.db_config)
        cursor = connection.cursor()
        try:
            cursor.execute(query, params)
            connection.commit()
        except mysql.connector.Error as err:
            print(f"Errore durante l'esecuzione della query di scrittura: {err}")
            connection.rollback()
        finally:
            cursor.close()
            connection.close()


    def registerUser(self, email, ticker, low_value, high_value):
        query = "INSERT INTO utenti (email, ticker, low_value, high_value) VALUES (%s, %s, %s, %s);"
        self.execute(query, (email, ticker, low_value, high_value))

    def updateUser(self, email, ticker, low_value, high_value):
        query = "UPDATE utenti SET ticker = %s, low_value = %s, high_value = %s WHERE email = %s;"
        self.execute(query, (ticker, low_value, high_value, email))

    def deleteUser(self, email):
        query = "DELETE FROM utenti WHERE email = %s;"
        self.execute(query, (email,))

    def deleteUnobservedData(self):
        query = "DELETE FROM dati WHERE ticker NOT IN (SELECT DISTINCT ticker FROM utenti);"
        self.execute(query)

    def insertData(self, ticker, data):
        query = """
                INSERT INTO dati (ticker, date, open, high, low, close, volume, dividends, splits) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
        self.execute(query, (
                                ticker,
                                data['date'].to_pydatetime(),
                                float(data['open']),
                                float(data['high']),
                                float(data['low']),
                                float(data['close']),
                                int(data['volume']),
                                float(data['dividends']),
                                float(data['splits'])
                            ))
       

    def insertDataSession(self, user_id, data_id):
        query = "INSERT INTO sessioni_utenti (id_utente, id_dato) VALUES (%s, %s);"
        self.execute(query, (user_id, data_id))
        


class QueryHandler:

    def __init__(self):
        self.db_config = DB_CONFIG

    def fetch_all(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        connection = mysql.connector.connect(**self.db_config)
        cursor = connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params)
            results = cursor.fetchall()
            return results
        except mysql.connector.Error as err:
            print(f"Errore durante l'esecuzione della query di lettura: {err}")
            return []
        finally:
            cursor.close()
            connection.close()
        

    def getUserByEmail(self, email):
        query = "SELECT * FROM utenti WHERE email = %s;"
        res = self.fetch_all(query, (email,))
        return res[0] if res else None

    def getLatestValue(self, email):
        query = """
                SELECT dati.ticker, date, open, high, low, close, volume, dividends, splits 
                FROM utenti JOIN sessioni_utenti ON utenti.id = sessioni_utenti.id_utente 
                JOIN dati ON sessioni_utenti.id_dato = dati.id 
                WHERE utenti.email = %s ORDER BY date DESC LIMIT 1;
                """

        res = self.fetch_all(query, (email,))
        return res[0] if res else None
    
    def getNUserData(self, email):
        query = "SELECT COUNT(*) FROM utenti JOIN sessioni_utenti ON utenti.id = sessioni_utenti.id_utente WHERE utenti.email = %s;"
        res = self.fetch_all(query, (email,))
        return res[0] if res else None  

    def getAverageValue(self, email, count):
        query = """
                SELECT AVG(open), AVG(high), AVG(low), AVG(close), AVG(volume), AVG(dividends), AVG(splits) 
                FROM (SELECT open, high, low, close, volume, dividends, splits 
                        FROM utenti JOIN sessioni_utenti ON utenti.id = sessioni_utenti.id_utente JOIN dati ON sessioni_utenti.id_dato = dati.id 
                        WHERE utenti.email = %s 
                        ORDER BY dati.date DESC LIMIT %s) AS subquery;"
                """
        res = self.fetch_all(query, (email, count))
        return res[0] if res else None
    
    def getUsers(self):
        query = "SELECT id, email, ticker FROM utenti;"
        res = self.fetch_all(query)
        return res
        
