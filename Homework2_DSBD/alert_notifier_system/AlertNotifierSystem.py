from confluent_kafka import Consumer
import json
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

# Configurazione di Kafka
consumer_config = {
    #'bootstrap.servers': 'kafka:9092',  # Indirizzo del broker Kafka
    #'group.id': 'notifier-group',           # Gruppo consumer per gestione offset
    #'auto.offset.reset': 'earliest'         # Legge i messaggi dall'inizio se non trova un offset salvato
    'bootstrap.servers': 'kafka:9092',  # Kafka broker address
    'group.id': 'notifier-group',  # Consumer group ID
    #'auto.offset.reset': 'earliest',  # Start reading from the earliest message
    'enable.auto.commit': True,  # Automatically commit offsets periodically
    'auto.commit.interval.ms': 5000  # Commit offsets every 5000ms (5 seconds)
}
consumer = Consumer(consumer_config)
consumer.subscribe(['to-notifier'])  # Iscrizione al topic Kafka


# Configura le credenziali e il server SMTP
smtp_server = "smtp.gmail.com"
smtp_port = 587  # Porta per TLS
#os.getenv("SMTP_USER") = "signore.marco26@gmail.com"
#os.getenv("SMTP_PASSWORD") = "kcqw thxc hqio jdpt"  


# Funzione per inviare email
def send_email(to_email, subject, body):
    # Crea il messaggio
    messaggio = MIMEMultipart()
    messaggio["From"] = os.getenv("SMTP_USER")
    messaggio["To"] = to_email
    messaggio["Subject"] = subject
    messaggio.attach(MIMEText(body, "plain"))

    try:
        # Connetti al server SMTP e invia l'email
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Attiva la modalità TLS
        server.login(os.getenv("SMTP_USER", "signore.marco26@gmail.com"), os.getenv("SMTP_PASSWORD","kcqw thxc hqio jdpt"))
        server.sendmail(os.getenv("SMTP_USER", "signore.marco26@gmail.com" ), to_email, messaggio.as_string())
        print("Email inviata con successo!")
    except Exception as e:
        print(f"Errore durante l'invio dell'email: {e}")
    finally:
        server.quit()



# Consumo dei messaggi dal topic Kafka
while True:
    msg = consumer.poll(1.0)  # Attende fino a 1 secondo per ricevere un messaggio
    if msg is None:
        continue  # Nessun messaggio ricevuto
    if msg.error():
        print(f"Errore nel Consumer: {msg.error()}")
        continue

    # Processa il messaggio ricevuto
    try:
        data = json.loads(msg.value().decode('utf-8'))  # Decodifica il messaggio JSON
        print(f"Ricevuto messaggio: {data}")

        # Parametri per l'email
        to_email = data['email']
        subject = f"Alert per ticker '{data['ticker']}' "
        body = f"Condizione violata per il ticker: {data['ticker']}, ticker_date: {datetime.fromisoformat(data['ticker_date'])}.\n{data['condition']}"

        # Invia l'email
        send_email(to_email, subject, body)
        
    except Exception as e:
        print(f"Errore durante l'elaborazione del messaggio: {e}")
