from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import json
import time
import CQRS

# Configurazione di Kafka
consumer_config = {
    #'bootstrap.servers': 'localhost:9092',
    #'group.id': 'alert-system-group',
    #'auto.offset.reset': 'earliest'
    'bootstrap.servers': 'kafka:9092',  # Kafka broker address
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


def scan_database_and_notify():
    """
    Scansiona il database per ticker che superano le soglie definite e invia notifiche via Kafka.
    """
    conn = CQRS.connect_db()
    cursor = conn.cursor(dictionary=True)
    read_service = CQRS.QueryHandler(cursor)

    msgs_count = 0
    msg_threshold = 1
    tot_msgs = 0
    
    try:
        
        records = read_service.getLastUsersData()

        for record in records:
            if record['high_value'] is not None and record['close'] > record['high_value']:
                condition = f"Valore {record['close']} sopra la soglia {record['high_value']}"
            elif record['low_value'] is not None and record['close'] < record['low_value']:
                condition = f"Valore {record['close']} sotto la soglia {record['low_value']}"
            else:
                continue  # Nessuna soglia superata, continua

            # Crea il messaggio e invialo al topic
            message = {'email': record['email'], 'ticker': record['ticker'], 'ticker_date': record['date'].isoformat(), 'condition': condition}
            producer.produce(producer_topic, json.dumps(message))
            
            msgs_count += 1
            tot_msgs += 1

            # Per evitare di fare il flush ad ogni messaggio o di tutti i messaggi in una sola volta alla fine)
            if msgs_count >= msg_threshold :
                producer.flush()
                msgs_count = 0
                print(f"Flush di {msg_threshold} messaggi a Kafka.")
        
        # Flush degli ultimi messaggi rimanenti
        producer.flush()
        print(f"Ultimi messaggi consegnati. Messaggi totali consegnati: {tot_msgs}")

    except Exception as e:
        print(f"Errore: {e}")

    finally:
        cursor.close()
        conn.close()

# Consumo dei messaggi dal topic Kafka
while True:
    # input di poll:
    # È un valore numerico che rappresenta il timeout per l'attesa del messaggio:
    # - Se un messaggio è disponibile nel topic e partizione a cui il consumer è sottoscritto, poll() lo restituisce immediatamente, senza aspettare il tempo massimo.
    # - Se nessun messaggio è disponibile entro il timeout specificato, poll() restituisce None.
    msg = consumer.poll(1.0)
    if msg is None:
        #print("Il messaggio è None.")
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
        # Decodifica il messaggio JSON ricevuto
        #print(f"Print di msg.value() = {msg.value()}\n Print di msg = {msg}\n")
        
        data = json.loads(msg.value().decode('utf-8'))
        print(f"Ricevuto messaggio: {data}")
        
        # Esegui la scansione del database e la notifica
        scan_database_and_notify()

    except Exception as e:
        print(f"Errore: {e}")
        continue  # Vai alla prossima iterazione se il messaggio non è valido


