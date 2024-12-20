import os
import mysql.connector

# Settaggio connessione al database MySQL. os.getenv() mi permette di ottenere 
# il valore di quelle variabili d'ambiente definite nel docker-compose.yml
def connect_db():
    # Metto i valori di default per connettersi al container col db quando eseguo il server direttamente da visual studio
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"), 
        user= os.getenv("DB_USER", "my_user"),
        password= os.getenv("DB_PASSWORD", "my_pass"),
        database=os.getenv("DB_NAME", "my_db")
    )


#---------------------------------- CQRS ----------------------------------------------------------------
class CommandHandler:
    def __init__(self, conn):
        self.conn = conn
        self.cur = self.conn.cursor()

    
    def registerUser(self, email, ticker, low_value, high_value):
        self.cur.execute("INSERT INTO utenti (email, ticker) VALUES (%s, %s)", (email, ticker))
        self.conn.commit()

    def updateUser(self, email, ticker, low_value, high_value):
        self.cur.execute("UPDATE utenti SET ticker = %s, low_value = %s, high_value = %s WHERE email = %s;", (ticker, low_value, high_value, email))
        self.conn.commit()

    def deleteUser(self, email):
        self.cur.execute("DELETE FROM utenti WHERE email = %s;", (email,))
        self.conn.commit()

    def deleteUnobservedData(self):
        self.cur.execute("DELETE FROM dati WHERE ticker NOT IN (SELECT DISTINCT ticker FROM utenti);")
        self.conn.commit()

    def insertData(self, ticker, data):
        self.cur.execute(
                            """
                            INSERT INTO dati (ticker, date, open, high, low, close, volume, dividends, splits) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """,
                            (
                                ticker,
                                data['date'].to_pydatetime(),
                                float(data['open']),
                                float(data['high']),
                                float(data['low']),
                                float(data['close']),
                                int(data['volume']),
                                float(data['dividends']),
                                float(data['splits'])
                            )
                        )
        data_id = self.cur.lastrowid
        self.conn.commit()
        return data_id

    def insertDataSession(self, user_id, data_id):
        self.cur.execute(
                        """
                        INSERT INTO sessioni_utenti (id_utente, id_dato) 
                        VALUES (%s, %s);
                        """,
                        (user_id, data_id)
                    )
        self.conn.commit()
        


class QueryHandler:
    def __init__(self, cur):
        self.cur = cur
        

    def getNUsers(self, email):
        self.cur.execute("SELECT COUNT(*) FROM utenti WHERE email = %s", (email,))
        return self.cur.fetchone()[0] 

    def getLatestValue(self, email):

        self.cur.execute("SELECT dati.ticker, date, open, high, low, close, volume, dividends, splits "
                             "FROM utenti JOIN sessioni_utenti ON utenti.id = sessioni_utenti.id_utente "
                             "JOIN dati ON sessioni_utenti.id_dato = dati.id "
                             "WHERE utenti.email = %s ORDER BY date DESC LIMIT 1;", (email,))
        return self.cur.fetchone()
    
    def getNUserData(self, email):
        self.cur.execute("SELECT COUNT(*) FROM utenti JOIN sessioni_utenti ON utenti.id = sessioni_utenti.id_utente WHERE utenti.email = %s", (email,))
        return self.cur.fetchone()[0]  

    def getAverageValue(self, email, count):
        self.cur.execute( "SELECT AVG(open), AVG(high), AVG(low), AVG(close), AVG(volume), AVG(dividends), AVG(splits) "
                         "FROM (SELECT open, high, low, close, volume, dividends, splits "
                                "FROM utenti JOIN sessioni_utenti ON utenti.id = sessioni_utenti.id_utente JOIN dati ON sessioni_utenti.id_dato = dati.id "
                                "WHERE utenti.email = %s "
                                "ORDER BY dati.date DESC LIMIT %s) AS subquery", (email, count))
        return self.cur.fetchone()
    
    def getUsers(self):
        self.cur.execute("SELECT id, email, ticker FROM utenti;")
        return self.cur.fetchall()
        
