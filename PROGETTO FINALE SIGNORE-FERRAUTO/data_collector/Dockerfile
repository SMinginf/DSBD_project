# Immagine Python leggera
FROM python:3.10-slim

# Imposta la direcotry di lavoro
WORKDIR /app

# Copia i file necessari nella directory corrente del container (app), ovvero
# applicazione e dipendenze (requirements.txt)
COPY DataCollectorKafka.py .
COPY CQRS.py .
COPY circuit_breaker.py .
COPY wait-for-it.sh ../usr/local/bin
COPY requirements.txt .

# Installa le dipendenze
RUN pip install --no-cache-dir -r requirements.txt

# Esegui lo script per aspettare il db
RUN chmod +x ../usr/local/bin/wait-for-it.sh

#EXPOSE 50051 #superfluo

# Comando per eseguire il server
CMD ["wait-for-it.sh", "mysql:3306", "--timeout=30", "--","wait-for-it.sh", "kafka:9092", "--timeout=30", "--", "python", "DataCollectorKafka.py"]
