import os
import mysql.connector



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
            last_row_id = cursor.lastrowid
            connection.commit()
            return last_row_id      
        except mysql.connector.Error as err:
            connection.rollback()
            raise err
        finally:
            cursor.close()
            connection.close()


    def deleteUnobservedData(self):
        query = "DELETE FROM dati WHERE ticker NOT IN (SELECT DISTINCT ticker FROM utenti);"
        self.execute(query)

    def insertData(self, ticker, data):
        query = """
                INSERT INTO dati (ticker, date, open, high, low, close, volume, dividends, splits) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
        last_row_id = self.execute(query, (
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
        return last_row_id
        
       

    def insertDataSession(self, user_id, data_id):
        query = "INSERT INTO sessioni_utenti (id_utente, id_dato) VALUES (%s, %s);"
        self.execute(query, (user_id, data_id))
        


class QueryHandler:

    def __init__(self):
        self.db_config = DB_CONFIG

    def fetch_all(self, query: str, params: tuple = ()): 
        connection = mysql.connector.connect(**self.db_config)
        cursor = connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params)
            results = cursor.fetchall()
            return results
        except mysql.connector.Error as err:
            raise err
        finally:
            cursor.close()
            connection.close()
        
    
    
    def getUsers(self):
        query = "SELECT id, email, ticker FROM utenti;"
        res = self.fetch_all(query)
        return res
        
    def getData(self, ticker, date):
        query = "SELECT * FROM dati where dati.ticker = %s and dati.date = %s;"
        res = self.fetch_all(query, (ticker, date.to_pydatetime()))
        return res[0] if res else None

    def getSession(self, user_id, data_id):
        query= "SELECT * FROM sessioni_utenti where sessioni_utenti.id_utente = %s and sessioni_utenti.id_dato = %s;"
        res = self.fetch_all(query, (user_id, data_id))
        return res[0] if res else None

    