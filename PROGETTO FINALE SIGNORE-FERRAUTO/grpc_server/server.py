import grpc
from concurrent import futures
import CQRS
import financial_service_pb2 as pb2
import financial_service_pb2_grpc as pb2_grpc
from threading import Lock
import prometheus_client
import socket
import time


''' 
# Creazione di un dizionario con tupla (client_id, request_id) come chiave e response come valore
dizionario = {
    ('client_id_1', 'request_id_1'): 'response_1',
    ('client_id_1', 'request_id_2'): 'response_2'
}
'''
cache = {}

# Lock per sincronizzare l'accesso alla cache 
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
    
    # Estrai i metadati dal context
    meta = dict(context.invocation_metadata())
    print(meta)
    client_id = meta.get('client_id', 'unknown')    
    request_id = meta.get('request_id', 'unknown')  
    
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
#------------------------------------------------------------------------------------------------------------ 

class FinancialService(pb2_grpc.FinancialServiceServicer):
    def __init__(self):

        self.read_service = CQRS.QueryHandler()
        self.write_service = CQRS.CommandHandler()
    
    def RegisterUser(self, request, context):   
        
        REQUEST_COUNTER.labels(service=my_service, hostname=my_hostname, method='RegisterUser').inc()     
        
        def processRegister():
                
                
            # Imposta correttamente i campi high_value e low_value
            low_value = request.low_value.value if request.HasField("low_value") else None
            high_value = request.high_value.value if request.HasField("high_value")  else None             
            
            # Command
            outcome = self.write_service.registerUser(request.email, request.ticker, low_value, high_value)

            return pb2.UserResponse(success=outcome[0], message=outcome[1])
                

        start_time = time.time()

        # Utilizza la funzione delegata per gestire la cache e l'elaborazione
        response = processWithCache(context, cache_lock, cache, processRegister)

        REQUEST_ELABORATION_TIME.labels(service=my_service, hostname=my_hostname, method='RegisterUser').set(time.time()-start_time)

        return response
        
             



    def UpdateUser(self, request, context):
        
        REQUEST_COUNTER.labels(service=my_service, hostname=my_hostname, method='UpdateUser').inc()
        
        def processUpdate():
                
            # Imposta correttamente i campi high_value e low_value
            low_value = request.low_value.value if request.HasField("low_value") else None
            high_value = request.high_value.value if request.HasField("high_value")  else None
            
            # Aggiorna il ticker
            outcome=self.write_service.updateUser(request.email, request.ticker, low_value, high_value)                
                
            return pb2.UserResponse(success=outcome[0], message=outcome[1])


        start_time = time.time()  
        response = processWithCache(context, cache_lock, cache, processUpdate)
        REQUEST_ELABORATION_TIME.labels(service=my_service, hostname=my_hostname, method='UpdateUser').set(time.time() - start_time)
        
        return response

            

    
    
    def DeleteUser(self, request, context):

        REQUEST_COUNTER.labels(service=my_service, hostname=my_hostname, method='DeleteUser').inc() 
        
        def processDelete():        
            outcome = self.write_service.deleteUser(request.email)
            return pb2.UserResponse(success=outcome[0], message=outcome[1])
            
        
        start_time=time.time()   
        response = processWithCache(context, cache_lock, cache, processDelete)
        REQUEST_ELABORATION_TIME.labels(service=my_service, hostname=my_hostname, method='DeleteUser').set(time.time()-start_time)
        return response
        
   
            


    def GetLatestValue(self, request, context):
        
        REQUEST_COUNTER.labels(service=my_service, hostname=my_hostname, method='GetLatestValue').inc() 
            
        def processValue():
            
            result = self.read_service.getLatestValue(request.email)
                
            if result[0]:
                tuple = result[1]
                data = pb2.FinancialData(
                    ticker = tuple[0],  
                    date = tuple[1], 
                    open = tuple[2],  
                    high = tuple[3],
                    low = tuple[4], 
                    close = tuple[5], 
                    volume = tuple[6],
                    dividends = tuple[7],
                    splits = tuple[8]
                )

                return pb2.StockValueResponse(success=True, data = data)
            else:
               return pb2.StockValueResponse(success=False, message = result[1])
         
        
        
        start_time= time.time() 
        response = processWithCache(context, cache_lock, cache, processValue)
        REQUEST_ELABORATION_TIME.labels(service=my_service, hostname=my_hostname, method='GetLatestValue').set(time.time()-start_time)

        return response
        
        
           
        

    def GetAverageValue(self, request, context):
        
        REQUEST_COUNTER.labels(service=my_service, hostname=my_hostname, method='GetAverageValue').inc()  
        
        def process_avg():


            result = self.read_service.getAverageValue(request.email, request.count)
                
            if result[0]:
                tuple = result[1]
                data = pb2.FinancialData(
                                     open = tuple[0], 
                                     high = tuple[1], 
                                     low = tuple[2], 
                                     close = tuple[3], 
                                     volume = tuple[4], 
                                     dividends = tuple[5], 
                                     splits = tuple[6])
                return pb2.StockValueResponse(success=True, data=data)
            else:
                return pb2.StockValueResponse(success=False, message = result[1])
            

        start_time = time.time()
        response = processWithCache(context, cache_lock, cache, process_avg)
        REQUEST_ELABORATION_TIME.labels(service=my_service, hostname=my_hostname, method='GetAverageValue').set(time.time()-start_time)
        return response
            


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

