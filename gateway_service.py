import os
import logging
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, status

# Importazione delle tue utility aziendali
from myapputilities.config_manager.simple_config_manager import SimpleConfigManager
from myapputilities.logging_utils import LogRotator

from connection_manager import ClientConnectionManager
from redis_listener import RedisPubSubListener

# 1. Inizializzazione Configurazione (Supporta ENV di Docker)
base_path = Path(__file__).parent
config = SimpleConfigManager(str(base_path / "config.ini"))

# 2. Inizializzazione Logger tramite il tuo LogRotator
log_rotator = LogRotator(
    logger_name="SignalGateway",
    log_path=base_path / "logs",
    log_file="gateway.log",
    log_level=config.get_string("LOGGING", "LEVEL", fallback="INFO")
)
logger = log_rotator.get_logger()

# Disattiviamo i log interni di uvicorn per non sporcare i log di Docker
logging.getLogger("uvicorn").setLevel(logging.WARNING)

logger.info("🚀 Avvio in corso del Signal Gateway (Modalità Pure Stateless)...")

# Estrazione parametri essenziali di connettività
redis_host = config.get_string("REDIS", "HOST", fallback="localhost")
redis_port = config.get_int("REDIS", "PORT", fallback=6379)
redis_prefix = config.get_string("REDIS", "PREFIX", fallback="filter").rstrip(':')
pubsub_channel = f"{redis_prefix}:pubsub:updates"

gateway_host = config.get_string("GATEWAY", "HOST", fallback="0.0.0.0")
gateway_port = config.get_int("GATEWAY", "PORT", fallback=40000)
auth_token = config.get_string("GATEWAY", "API_AUTH_TOKEN")
keepalive_interval = config.get_float("GATEWAY", "KEEPALIVE_INTERVAL", fallback=60.0)

# 3. Istanziazione Componenti Architetturali OO
connection_manager = ClientConnectionManager(logger=logger, keepalive_interval=keepalive_interval)
redis_listener = RedisPubSubListener(
    redis_host=redis_host,
    redis_port=redis_port,
    channel=pubsub_channel,
    connection_manager=connection_manager,
    logger=logger
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- ASYNC BOOTSTRAP ---
    connection_manager.start()
    redis_listener.start()
    logger.info(f"✅ Signal Gateway in ascolto su http://{gateway_host}:{gateway_port}")
    yield
    # --- ASYNC SHUTDOWN CONTROLLATO (SIGTERM / Docker stop) ---
    logger.info("🛑 Segnale di arresto ricevuto. Chiusura delle connessioni in corso...")
    redis_listener.stop()
    connection_manager.stop()
    logger.info("👋 Signal Gateway disattivato con successo.")

app = FastAPI(lifespan=lifespan)

# 4. Endpoint WebSocket Real-Time con estrazione manuale del token
@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    # 🎯 RECUPERIAMO IL TOKEN DALL'HEADER HTTP "Authorization" INVECE CHE DALLA URL
    auth_header = websocket.headers.get("authorization", "")
    token = None

    # Verifichiamo che l'header segua lo schema standard "Bearer <token>"
    if auth_header.startswith("Bearer "):
        try:
            token = auth_header.split(" ")[1].strip()
        except IndexError:
            token = None

    # 1. Controllo sicurezza sul Token PRIMA di accettare
    if auth_token and token != auth_token:
        logger.warning(f"🔒 Connessione WebSocket respinta: Token errato o assente.")
        # Accettiamo e chiudiamo subito con codice 1008 per segnalare la violazione di policy
        await websocket.accept()
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    # 2. SE IL TOKEN È CORRETTO, LA CONNESIONE PROSEGUE:
    await connection_manager.connect(websocket)
    # 3. COMPILAZIONE E INVIO SNAPSHOT INIZIALE DA REDIS
    try:
        logger.info("📊 Richiesta di snapshot globale al modulo Redis...")
        snapshot_data = await redis_listener.get_all_data_snapshot(redis_prefix)
        await websocket.send_json(snapshot_data)
        logger.info("✅ Snapshot iniziale inviato con successo al client.")
    except Exception as e:
        logger.error(f"⚠️ Impossibile inviare lo snapshot iniziale: {e}")

    # 4. CICLO CONTINUO PER GLI AGGIORNAMENTI FUTURI (DELTA)
    try:
        while True:
            data = await websocket.receive_text()
            logger.debug(f"Inbound dal client: {data}")
    except WebSocketDisconnect:
        connection_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"Errore sul canale del client: {e}")
        connection_manager.disconnect(websocket)

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "active_clients": len(connection_manager.active_connections)
    }

if __name__ == "__main__":
    import uvicorn
    # 🎯 UTILIZZIAMO LA SINTASSI A STRINGA "gateway_service:app" con reload=True
    # Questo forza Uvicorn a rinfrescare il modulo, liberare la porta 40000 e applicare il codice reale!
    uvicorn.run("gateway_service:app", host=gateway_host, port=gateway_port, reload=True)