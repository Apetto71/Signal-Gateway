📬 AI-Distributor Service
Servizio di notifica automatizzata che consuma le previsioni generate dall'AI e le distribuisce via email a una mailing list configurata.

🎯 Obiettivi del Servizio
Consumo asincrono: Ascolta i risultati dell'AI tramite RabbitMQ.

Idempotenza: Grazie a Redis, garantisce che la stessa previsione non venga inviata più volte nello stesso giorno (evita lo spam in caso di riavvii).

Formattazione: Trasforma i dati JSON grezzi in email HTML leggibili.

🛠️ Stack Tecnologico
Python 3.10

Redis: Storage chiave-valore per lo stato degli invii.

RabbitMQ (Pika): Client per la gestione delle code.

SMTPLIB: Invio mail tramite protocollo TLS.

SimpleConfigManager: Gestione centralizzata delle configurazioni.

📂 Struttura del Progetto
Plaintext
Ai-Distributor/
├── distributor.py       # Main script: Logica del Consumer e invio mail
├── config.ini           # Configurazioni locali (SMTP, Redis, RabbitMQ)
├── requirements.txt     # Dipendenze (pika, redis, myapputilities)
└── Dockerfile           # Istruzioni per la containerizzazione