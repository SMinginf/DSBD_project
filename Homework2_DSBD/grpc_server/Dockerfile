FROM python:3.10-slim 

WORKDIR /app

COPY requirements.txt .

COPY server.py .
COPY CQRS.py .
COPY financial_service_pb2.py .
COPY financial_service_pb2_grpc.py .

COPY wait-for-it.sh /usr/local/bin

# Crea una directory specifica per salvare le dipendenze
RUN pip install --no-cache-dir -r requirements.txt

# Aggiungi i permessi di esecuzione al file wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

CMD ["wait-for-it.sh", "mysql:3306", "--", "python", "server.py"]

