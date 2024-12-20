import grpc
from concurrent import futures
import CQRS
import financial_service_pb2 as pb2
import financial_service_pb2_grpc as pb2_grpc
import mysql.connector
from threading import Lock
import yfinance as yf
#from google.protobuf.wrappers_pb2 import FloatValue



# A dictionary to store processed request IDs and their responses
''' 
# Creazione di un dizionario con tupla (client_id, request_id) come chiave e respone come valore
dizionario = {
    ('client_id_1', 'request_id_1'): 'response_1',
    ('client_id_1', 'request_id_2'): 'response_2'
}
'''
cache = {}

# A lock to synchronize access to the cache for thread safety
cache_lock = Lock()




def processWithCache(context, cache_lock, cache, process_function):
    """
    Implementa la politica At Most Once.
    Gestisce la logica della cache e delega a una funzione di elaborazione se necessario.

    Args:
        context: contiene i metadati della richiesta proveniente dal client.
        cache_lock (threading.Lock): Il lock per la sincronizzazione della cache.
        cache (dict): Il dizionario che funge da cache per le risposte.
        process_function: La funzione da chiamare per elaborare la richiesta.

    Returns: La risposta, dalla cache o dalla funzione di elaborazione.
    """
    
    # Extract metadata from the context
    meta = dict(context.invocation_metadata())
    print(meta)
    client_id = meta.get('client_id', 'unknown')    # Default to 'unknown' if not present
    request_id = meta.get('request_id', 'unknown')  # Default to 'unknown' if not present
    
    with cache_lock:
        # Controlla se la richiesta è già nella cache
        if (client_id,request_id) in cache:
            print(f"Returning cached response for ClientID = {client_id} and RequestID = {request_id}")
            return cache[(client_id,request_id)]

    # Elabora la richiesta (se non è in cache)
    response = process_function()

    # Memorizza la risposta nella cache inserendo come key la tupla (clientid, requestid)
    with cache_lock:
        cache[(client_id, request_id)] = response

    return response

def isValidTicker(ticker):
    """Ritorna True se il ticker è valido, False altrimenti."""
    stock = yf.Ticker(ticker)
    history = stock.history(period="1d")
    return not history.empty 




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
    def __init__(self, message="'High value' deve avere un valore maggiore di 'low value'."):
        super().__init__(message)
#------------------------------------------------------------------------------------------------------------


class FinancialService(pb2_grpc.FinancialServiceServicer):
    def __init__(self):
        self.conn = CQRS.connect_db()
        self.cur = self.conn.cursor()
        self.read_service = CQRS.QueryHandler(self.cur)
        self.write_service = CQRS.CommandHandler(self.conn)
    
    def RegisterUser(self, request, context):            
        def processRegister():
            try:
                # Lock della tabella utenti
                self.cur.execute("LOCK TABLES utenti WRITE;")
                
                # Controlla se l'email esiste già   
                if self.read_service.getNUsers(request.email) > 0:
                    raise EmailAlreadyExistsException(request.email)

                # Controlla se il ticker è valido
                if not isValidTicker(request.ticker):
                    raise TickerNotValidException(request.ticker)
                
                # Verifica che valga la condizione "high_value is NULL or high_value > low_value"
                low_value = request.low_value.value if request.HasField("low_value") else None
                high_value = request.high_value.value if request.HasField("high_value")  else None
                if high_value is not None and low_value is not None and high_value <= low_value:
                    raise ThresholdsViolatedException
                
                # Query
                self.write_service.registerUser(request.email, request.ticker, low_value, high_value )

                return pb2.UserResponse(success=True, message="Utente registrato con successo.")
            
            finally:
                #Rilascia il lock
                self.cur.execute("UNLOCK TABLES;")
                
        try:    
            # Utilizza la funzione delegata per gestire la cache e l'elaborazione
            response = processWithCache(context, cache_lock, cache, processRegister)
            return response
        except (EmailAlreadyExistsException, TickerNotValidException, mysql.connector.Error, ThresholdsViolatedException) as e:
            if isinstance(e, mysql.connector.Error):
                self.conn.rollback()
            return pb2.UserResponse(success=False, message=str(e))


    def UpdateUser(self, request, context):
        def processUpdate():
            try:
                self.cur.execute("LOCK TABLES utenti WRITE, sessioni_utenti WRITE;")
                
                # Controlla se l'utente inserito esiste  
                if self.read_service.getNUsers(request.email) == 0:
                    raise UserDoesNotExistsException

                # Controlla se il ticker è valido
                if not isValidTicker(request.ticker):
                    raise TickerNotValidException(request.ticker)
                
                # Verifica che valga la condizione "high_value is NULL or high_value > low_value"
                low_value = request.low_value.value if request.HasField("low_value") else None
                high_value = request.high_value.value if request.HasField("high_value")  else None
                if high_value is not None and low_value is not None and high_value <= low_value:
                    raise ThresholdsViolatedException
                
                # Aggiorna il ticker
                self.write_service.updateUser(request.email, request.ticker, low_value, high_value)                
                
                return pb2.UserResponse(success=True, message="Utente aggiornato con successo.")
            
            finally:
                self.cur.execute("UNLOCK TABLES;")
    
        try:
            response = processWithCache(context, cache_lock, cache, processUpdate)
            return response
        
        except (UserDoesNotExistsException, TickerNotValidException, mysql.connector.Error, ThresholdsViolatedException) as e:
            if isinstance(e, mysql.connector.Error): 
                self.conn.rollback()
            return pb2.UserResponse(success=False, message=str(e))

    
    
    def DeleteUser(self, request, context):
        
        def processDelete():

            try:       
                # Lock della tabella 
                self.cur.execute("LOCK TABLES utenti WRITE;")
            
                # Controlla se l'utente inserito esiste  
                if self.read_service.getNUsers(request.email) == 0:
                    raise UserDoesNotExistsException

                self.write_service.deleteUser(request.email)

                return pb2.UserResponse(success=True, message="Utente cancellato con successo.")
            
            finally:
                self.cur.execute("UNLOCK TABLES;")
        
        try:
            response = processWithCache(context, cache_lock, cache, processDelete)
            return response
        
        except (UserDoesNotExistsException, mysql.connector.Error) as e:
            if isinstance(e, mysql.connector.Error):
                self.conn.rollback()
            return pb2.UserResponse(success=False, message=str(e))


    def GetLatestValue(self, request, context):
        
        def processValue():
        
            try:
                self.cur.execute("LOCK TABLES utenti READ, sessioni_utenti READ, dati READ;")
            
                # Controlla se l'utente inserito esiste  
                if self.read_service.getNUsers(request.email) == 0:
                    raise UserDoesNotExistsException

                result = self.read_service.getLatestValue(request.email)
                
                if result:
                    data = pb2.FinancialData(
                    ticker = str(result[0]),  
                    date = str(result[1]),  
                    open = float(result[2]),  
                    high = float(result[3]),
                    low = float(result[4]), 
                    close = float(result[5]), 
                    volume = int(result[6]),
                    dividends = float(result[7]),
                    splits = float(result[8])
                )

                    return pb2.StockValueResponse(success=True, data = data)
                else:
                    raise NoDataFoundException
         
            finally:
                self.cur.execute("UNLOCK TABLES;")
        try:    
            response = processWithCache(context, cache_lock, cache, processValue)
            return response
        
        except (UserDoesNotExistsException, NoDataFoundException, mysql.connector.Error) as e:
            if isinstance(e, mysql.connector.Error):
                self.conn.rollback() 
            return pb2.StockValueResponse(success=False, message=str(e))

        

    def GetAverageValue(self, request, context):
        
        def process_avg():
        
            try:
                self.cur.execute("LOCK TABLES utenti READ, sessioni_utenti READ, dati READ;")
        
                # Controlla che request.count sia un parametro valido
                if not isinstance(request.count, int) or request.count <= 0 :
                    raise InvalidInputTypeException
        
                # Controlla se l'utente inserito esiste  
                if self.read_service.getNUsers(request.email) == 0:
                    raise UserDoesNotExistsException

        
                # Controlla se esistono n=count dati
                if self.read_service.getNUserData(request.email) < request.count:
                    raise NotEnoughDataException

                result = self.read_service.getAverageValue(request.email, request.count)
                
                data = pb2.FinancialData(
                                         open = float(result[0]), 
                                         high = float(result[1]), 
                                         low = float(result[2]), 
                                         close = float(result[3]), 
                                         volume = int(result[4]), 
                                         dividends = float(result[5]), 
                                         splits = float(result[6]))
                return pb2.StockValueResponse(success=True, data=data)
            
            finally:
                self.cur.execute("UNLOCK TABLES;")
        try:
            response = processWithCache(context, cache_lock, cache, process_avg)
            return response
        
        except (InvalidInputTypeException, UserDoesNotExistsException, NotEnoughDataException, mysql.connector.Error) as e:
            if isinstance(e, mysql.connector.Error):
                self.conn.rollback()
            return pb2.StockValueResponse(success=False, message=str(e))



def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_FinancialServiceServicer_to_server(FinancialService(), server)
    server.add_insecure_port('[::]:50051')
    print("Server running on port 50051")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()

