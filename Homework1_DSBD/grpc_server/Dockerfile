# Immagine Python leggera
FROM python:3.10-slim

# Imposta la direcotry di lavoro
WORKDIR /app

# Copia i file necessari nella direcoty corrente del container (app), ovvero
# applicazione e dipendenze (requirements.txt)
COPY server.py .
COPY financial_service_pb2.py .
COPY financial_service_pb2_grpc.py .
COPY wait-for-it.sh ../usr/local/bin
COPY requirements.txt .

# Installa le dipendenze
RUN pip install --no-cache-dir -r requirements.txt

# Esegui lo script per aspettare il db
RUN chmod +x ../usr/local/bin/wait-for-it.sh

#EXPOSE 50051 #superfluo

# Comando per eseguire il server
CMD ["wait-for-it.sh", "mysql:3306", "--","python", "server.py"]
