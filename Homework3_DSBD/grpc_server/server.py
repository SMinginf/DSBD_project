import grpc
from concurrent import futures
import CQRS
import financial_service_pb2 as pb2
import financial_service_pb2_grpc as pb2_grpc
import datetime
from threading import Lock
import yfinance as yf
import prometheus_client
import socket
import time

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

# ------------------------------ WHITEBOX MONITORING --------------------------------------
my_service = 'grpc_server'
my_hostname = socket.gethostname()

REQUEST_COUNTER = prometheus_client.Counter(
    'total_received_requests',
    'Numero totale di richieste ricevute',
    ['service', 'hostname', 'method']
)

REQUEST_ELABORATION_TIME = prometheus_client.Gauge(
    'request_elaboration_time',
    'Tempo impiegato per il processamento di una richiesta',
    ['service', 'hostname', 'method']
)
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

        self.read_service = CQRS.QueryHandler()
        self.write_service = CQRS.CommandHandler()
    
    def RegisterUser(self, request, context):   
        
        REQUEST_COUNTER.labels(service=my_service, hostname=my_hostname, method='RegisterUser').inc()     
        
        def processRegister():
                
            # Controlla se l'email esiste già   
            if self.read_service.getUserByEmail(request.email):
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
            self.write_service.registerUser(request.email, request.ticker, low_value, high_value)

            return pb2.UserResponse(success=True, message="Utente registrato con successo.")
            
                
        try:
            start_time = time.time()

            # Utilizza la funzione delegata per gestire la cache e l'elaborazione
            response = processWithCache(context, cache_lock, cache, processRegister)
            return response
        
        except (EmailAlreadyExistsException, TickerNotValidException, ThresholdsViolatedException) as e:
            return pb2.UserResponse(success=False, message=str(e))
        finally:
            REQUEST_ELABORATION_TIME.labels(service=my_service, hostname=my_hostname, method='RegisterUser').set(time.time()-start_time)
            

            



    def UpdateUser(self, request, context):
        
        REQUEST_COUNTER.labels(service=my_service, hostname=my_hostname, method='UpdateUser').inc()
        
        def processUpdate():
              
               # Controlla se l'utente inserito esiste  
            if self.read_service.getUserByEmail(request.email) is None:
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

    
        try:
            start_time = time.time()  
            response = processWithCache(context, cache_lock, cache, processUpdate)
            return response
        
        except (UserDoesNotExistsException, TickerNotValidException, ThresholdsViolatedException) as e:
            return pb2.UserResponse(success=False, message=str(e))
        finally:
            REQUEST_ELABORATION_TIME.labels(service=my_service, hostname=my_hostname, method='UpdateUser').set(time.time() - start_time)



    
    
    def DeleteUser(self, request, context):

        REQUEST_COUNTER.labels(service=my_service, hostname=my_hostname, method='DeleteUser').inc() 
        
        def processDelete():
            
                # Controlla se l'utente inserito esiste  
            if self.read_service.getUserByEmail(request.email) is None:
                raise UserDoesNotExistsException

            self.write_service.deleteUser(request.email)

            return pb2.UserResponse(success=True, message="Utente cancellato con successo.")
            
        
        try:
            start_time=time.time() 
        
            response = processWithCache(context, cache_lock, cache, processDelete)

            return response
        
        except (UserDoesNotExistsException) as e:
            return pb2.UserResponse(success=False, message=str(e))
        finally:
            REQUEST_ELABORATION_TIME.labels(service=my_service, hostname=my_hostname, method='DeleteUser').set(time.time()-start_time)
       


    def GetLatestValue(self, request, context):
        
        REQUEST_COUNTER.labels(service=my_service, hostname=my_hostname, method='GetLatestValue').inc() 
            
        def processValue():
            
                # Controlla se l'utente inserito esiste  
            if self.read_service.getUserByEmail(request.email) is None:
                raise UserDoesNotExistsException

            result = self.read_service.getLatestValue(request.email)
                
            if result:
                data = pb2.FinancialData(
                    ticker = str(result['ticker']),  
                    date = str(result['date']), 
                    open = float(result['open']),  
                    high = float(result['high']),
                    low = float(result['low']), 
                    close = float(result['close']), 
                    volume = int(result['volume']),
                    dividends = float(result['dividends']),
                    splits = float(result['splits'])
                )

                return pb2.StockValueResponse(success=True, data = data)
            else:
                raise NoDataFoundException
         
        
        try:   
            start_time= time.time() 
            response = processWithCache(context, cache_lock, cache, processValue)
            return response
        
        except (UserDoesNotExistsException, NoDataFoundException, Exception) as e:          
            return pb2.StockValueResponse(success=False, message=str(e))
        finally:
           REQUEST_ELABORATION_TIME.labels(service=my_service, hostname=my_hostname, method='GetLatestValue').set(time.time()-start_time)

        

    def GetAverageValue(self, request, context):
        
        REQUEST_COUNTER.labels(service=my_service, hostname=my_hostname, method='GetAverageValue').inc()  
        
        def process_avg():
        
        
                # Controlla che request.count sia un parametro valido
            if not isinstance(request.count, int) or request.count <= 0 :
                raise InvalidInputTypeException
        
                # Controlla se l'utente inserito esiste  
            if self.read_service.getUserByEmail(request.email) is None:
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
            
        try:
            start_time = time.time()
            response = processWithCache(context, cache_lock, cache, process_avg)
            return response
        
        except (InvalidInputTypeException, UserDoesNotExistsException, NotEnoughDataException) as e:
            return pb2.StockValueResponse(success=False, message=str(e))
        finally:
            REQUEST_ELABORATION_TIME.labels(service=my_service, hostname=my_hostname, method='GetAverageValue').set(time.time()-start_time)



def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_FinancialServiceServicer_to_server(FinancialService(), server)
    server.add_insecure_port('[::]:50051')
    print("Server running on port 50051")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    prometheus_client.start_http_server(9999)
    serve()

