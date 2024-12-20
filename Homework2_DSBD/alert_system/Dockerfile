# Immagine Python leggera
FROM python:3.10-slim

# Imposta la direcotry di lavoro
WORKDIR /app

# Copia i file necessari nella direcoty corrente del container (app), ovvero
# applicazione e dipendenze (requirements.txt)
COPY AlertSystem.py .
COPY CQRS.py .
COPY wait-for-it.sh ../usr/local/bin
COPY requirements.txt .

# Installa le dipendenze
RUN pip install --no-cache-dir -r requirements.txt


RUN chmod +x ../usr/local/bin/wait-for-it.sh


# Comando per eseguire il server
CMD ["wait-for-it.sh", "kafka:9092", "--timeout=30", "--","python", "AlertSystem.py"]
