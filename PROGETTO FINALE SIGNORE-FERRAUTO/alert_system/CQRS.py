import os
import mysql.connector


# Settaggio connessione al database MySQL. os.getenv() mi permette di ottenere 
# il valore di quelle variabili d'ambiente definite nel docker-compose.yml

DB_CONFIG = {
    "host" : os.getenv("DB_HOST", "localhost"), 
    "user" : os.getenv("DB_USER", "my_user"),
    "password" : os.getenv("DB_PASSWORD", "my_pass"),
    "database" : os.getenv("DB_NAME", "my_db")
        }

#---------------------------------- CQRS ----------------------------------------------------------------

class QueryHandler:

    def __init__(self):
        self.db_config = DB_CONFIG

    def fetch_all(self, query: str, params: tuple = ()): # La funzione ritorna una List[Dict[str, Any]]
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
        

    def getLastUsersData(self):
        query = """
                SELECT 
                         utenti.id as user_id,
                         utenti.email,
                         dati.id as data_id, 
                         dati.ticker, 
                         utenti.high_value, 
                         utenti.low_value, 
                         dati.date, 
                         dati.close
                FROM 
                    utenti
                JOIN 
                    sessioni_utenti ON utenti.id = sessioni_utenti.id_utente
                JOIN 
                    dati ON sessioni_utenti.id_dato = dati.id
                JOIN (
                    -- Sottoquery per ottenere la data massima per ciascun utente e ticker
                    SELECT  
                        utenti.id AS id_utente, 
                        MAX(dati.date) AS max_date
                    FROM 
                        utenti
                    JOIN 
                        sessioni_utenti ON utenti.id = sessioni_utenti.id_utente
                    JOIN 
                        dati ON sessioni_utenti.id_dato = dati.id
                    GROUP BY 
                        utenti.id
                ) AS latest_data 
                ON 
                    utenti.id = latest_data.id_utente 
                    AND dati.date = latest_data.max_date;
                """
        res = self.fetch_all(query)
        return res if res else None

        
