# 1. Usa un'immagine Python 3.12 snella (slim)
FROM python:3.12-slim

# 2. Imposta la directory di lavoro all'interno del container
WORKDIR /app

# 3. Imposta variabili d'ambiente per Python
# Evita la creazione di file .pyc e forza l'invio immediato dei log al terminale
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 4. Installa le dipendenze di sistema minime (se necessarie per pika o mysql)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 5. Copia il file dei requisiti generato con pip-compile
COPY requirements.txt .

# 6. Installa le dipendenze Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Ora pip troverà il comando git per installare MyAppUtilities
RUN pip install --no-cache-dir --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# 7. Copia tutto il contenuto della cartella locale nel container
# Includerà gateway.py, le tue utility e il config.ini
COPY . .

# 8. Crea la cartella per i log (se non esiste)
RUN mkdir -p /app/logs

# 9. Espone la porta del Gateway (40000)
EXPOSE 40000

# 10. Comando per avviare l'applicazione
CMD ["python", "gateway.py"]