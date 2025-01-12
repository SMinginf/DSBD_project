from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from datetime import datetime, timedelta
import json
import CQRS
import redis

# ------------------------------------ CONFIGURAZIONE KAFKA -----------------------------------------------------------------------
consumer_config = {
    #'auto.offset.reset': 'earliest'
    'bootstrap.servers': 'kafka:9092',  # Kafka broker address
    #'bootstrap.servers': 'localhost:9092',
    'group.id': 'alert-system-group',  # Consumer group ID
    #'auto.offset.reset': 'earliest',  # Start reading from the earliest message
    'enable.auto.commit': True,  # Automatically commit offsets periodically
    'auto.commit.interval.ms': 5000  # Commit offsets every 5000ms (5 seconds)
}
producer_config = {'bootstrap.servers': 'kafka:9092',  # Kafka broker address
        'acks': 'all',  # Ensure all in-sync replicas acknowledge the message
        #'max.in.flight.requests.per.connection': 1,  # Only one in-flight request per connection
        'retries': 3 
        }
consumer = Consumer(consumer_config)
producer = Producer(producer_config)
consumer.subscribe(['to-alert-system'])
producer_topic = 'to-notifier'

# -------------------------------------------------- REDIS -------------------------------------------------------------------------

# Connessione a Redis
redis_client = redis.StrictRedis(host='redis', port=6379, decode_responses=True)

def should_send_notification(user_id, data_id, notif_key) :
    """
    Determina se una notifica deve essere inviata al rispettivo utente.
    Una notifica deve essere recapitata all'utente ogni 10 minuti.
    Usa Redis per registrare l'invio di tali notifiche.
    """
    key = f"{user_id}:{data_id}:[{notif_key}]"
    last_notification = redis_client.get(key)

    now = datetime.now()
    if last_notification is None:
        # Nessuna notifica precedente, salva la key della notifica con scadenza timedelta(days=10)
        redis_client.setex(key, timedelta(days=10), now.isoformat())

        return True
    
    # notifica già creata in passato da non più di 10 giorni
    last_notification_time = datetime.fromisoformat(last_notification)

    if now - last_notification_time > timedelta(minutes=10):

        # Permetti l'invio della notifica se sono passati almeno 10 minuti, dopodichè aggiorna la data della notifica
        redis_client.setex(key, timedelta(days=10), now.isoformat())

        return True


    return False

# ----------------------------------------------------------------------------------------------------------------------------------
def delivery_report(err, msg):
    global delivered_count
    if err is not None:
        print(f"Messaggio non consegnato: {err}")
    else:
        print(f"Messaggio consegnato a {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")
        delivered_count += 1


def scan_database_and_notify():
    """
    Scansiona il database per ticker che superano le soglie definite e invia notifiche via Kafka.
    """
    read_service = CQRS.QueryHandler()

    #msgs_count = 0
    #msg_threshold = 3
    
    try:
        
        records = read_service.getLastUsersData()

        for record in records:
            if record['high_value'] is not None and record['close'] > record['high_value']:
                condition = f"Valore {record['close']} sopra la soglia {record['high_value']}"
                notif_key = f"{record['close']}>>{record['high_value']}"
            elif record['low_value'] is not None and record['close'] < record['low_value']:
                condition = f"Valore {record['close']} sotto la soglia {record['low_value']}"
                notif_key = f"{record['close']}<<{record['low_value']}"
            else:
                continue  # Nessuna soglia superata, continua

            # Crea la notifica e inviala solo se non è già stata processata negli ultimi 10 minuti 
            if not should_send_notification(record['user_id'], record['data_id'], notif_key):
                print(f"Notifica non inviata - Limite di tempo non ancora trascorso.")
                continue
            
            message = {'email': record['email'], 'ticker': record['ticker'], 'ticker_date': record['date'].isoformat(), 'condition': condition}
            
            producer.produce(producer_topic, json.dumps(message), callback = delivery_report)            
            #msgs_count += 1

            # Per evitare di fare il flush ad ogni messaggio o di tutti i messaggi in una sola volta alla fine
            #if msgs_count >= msg_threshold :
            #    producer.flush()
            #    msgs_count = 0
            #    print(f"Flush di {msg_threshold} messaggi a Kafka.")
        
        # Flush degli ultimi messaggi rimanenti
        producer.flush()
        print(f"Messaggi totali consegnati: {delivered_count}")

    except Exception as e:
        print(f"Errore: {e}")



# Consumo dei messaggi dal topic Kafka
while True:

    msg = consumer.poll(1.0)
    
    if msg is None:
        continue
    
    if msg.error():
        if msg.error().code() == KafkaError._ALL_BROKERS_DOWN :
            print("Tutti i broker Kafka sono inattivi. Uscita dal consumer.")
            break
        else:
            print(f"Errore nel Consumer: {msg.error()}")
            continue
    
    # Processa il messaggio ricevuto
    try:        
        data = json.loads(msg.value().decode('utf-8'))
        print(f"Ricevuto messaggio: {data}")
        
        # Esegui la scansione del database e la notifica
        scan_database_and_notify()

    except Exception as e:
        print(f"Errore: {e}")
        continue  # Vai alla prossima iterazione se il messaggio non è valido


