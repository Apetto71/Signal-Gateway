import json
import threading
import asyncio
import redis
import uvicorn
import time
import pika
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# Utility originali
from myapputilities.config_manager import SimpleConfigManager
from myapputilities.RabbitMqUtils.rabbitQueueDlq import RabbitMQSimpleDLQ
from myapputilities.logging_utils.logger import LogRotator

# ==========================================
# CONFIGURAZIONE E LIFESPAN
# ==========================================
path_config = str(Path(__file__).parent / "config.ini")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestisce l'avvio e lo spegnimento in modo moderno"""
    # Catturiamo il loop principale qui, dove FastAPI sta girando
    loop = asyncio.get_running_loop()

    gateway.logger.info("🚀 FastAPI pronto, avvio thread RabbitMQ...")

    # Passiamo il loop come argomento al thread
    rabbit_thread = threading.Thread(
        target=gateway.run_rabbit_forever,
        args=(loop,),
        daemon=True
    )
    rabbit_thread.start()

    yield
    gateway.logger.info("🛑 Spegnimento Gateway...")

# ==========================================
# CLASSE CORE MIGLIORATA
# ==========================================
class SignalGateway:
    def __init__(self, config_path: str):
        self.config = SimpleConfigManager(config_path)

        # Setup Logging
        log_path = Path(self.config.get_string('LOGGING', 'log_path', fallback='./logs'))
        log_file = self.config.get_string('LOGGING', 'log_file', fallback='gateway.log')
        self.log_setup = LogRotator(
            logger_name='SignalGateway',
            log_path=log_path,
            log_file=log_file,
            log_level=self.config.get_string('LOGGING', 'level', fallback='INFO')
        )
        self.logger = self.log_setup.get_logger()

        # FastAPI con Lifespan
        self.app = FastAPI(lifespan=lifespan)

        # Stato
        self.api_token = self.config.get_string('GATEWAY', 'api_auth_token')
        signals_str = self.config.get_string('GATEWAY', 'managed_signals', fallback='daily_forecast')
        self.managed_signals = [s.strip() for s in signals_str.split(',')]
        self.active_connections = set()  # Set è più efficiente per rimuovere connessioni

        # Loop di riferimento per il broadcast
        self.loop = None

        # Redis
        self.r = redis.Redis(
            host=self.config.get_string('REDIS', 'host'),
            port=self.config.get_int('REDIS', 'port'),
            decode_responses=True
        )

    def rabbit_callback(self, ch, method, properties, body):
        """Callback per processare i messaggi in arrivo"""
        try:
            message_str = body.decode('utf-8')
            data = json.loads(message_str)
            msg_type = data.get("type")

            if msg_type in self.managed_signals:
                current_redis_key = f"latest_{msg_type}"
                self.r.set(current_redis_key, message_str)
                self.logger.info(f"💾 Segnale '{msg_type}' salvato su Redis")

                # Invia ai websocket se il loop è attivo
                if self.loop and self.loop.is_running():
                    asyncio.run_coroutine_threadsafe(self.broadcast(message_str), self.loop)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            self.logger.error(f"❌ Errore processamento messaggio: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    async def broadcast(self, message: str):
        """Invia il messaggio a tutti i client connessi"""
        if not self.active_connections:
            return

        # Creiamo una copia per evitare errori di dimensione mutata durante l'invio
        for connection in list(self.active_connections):
            try:
                await connection.send_text(message)
            except Exception:
                self.active_connections.discard(connection)

    def run_rabbit_forever(self, loop):
        self.loop = loop
        retry_delay = 5
        while True:
            try:
                self.logger.info("🐰 Connessione a RabbitMQ per DOPPIO ascolto...")
                rabbit = RabbitMQSimpleDLQ(
                    host=self.config.get_string('RABBITMQ', 'host'),
                    porta=self.config.get_int('RABBITMQ', 'port'),
                    user=self.config.get_string('RABBITMQ', 'user'),
                    passw=self.config.get_string('RABBITMQ', 'password')
                )

                if rabbit.connect():
                    # 1. Ascolta la coda TOP10
                    queue_top = self.config.get_string('RABBITMQ', 'queue_top_list')
                    rabbit.channel.basic_consume(
                        queue=queue_top,
                        on_message_callback=self.rabbit_callback
                    )

                    # 2. Ascolta la coda PREZZI FILTRATI
                    queue_prices = self.config.get_string('RABBITMQ', 'queue_prices')
                    rabbit.channel.basic_consume(
                        queue=queue_prices,
                        on_message_callback=self.rabbit_callback
                    )

                    self.logger.info(f"✅ In ascolto su: {queue_top} E {queue_prices}")
                    rabbit.channel.start_consuming()

            except Exception as e:
                self.logger.error(f"⚠️ Errore RabbitMQ: {e}")
                time.sleep(retry_delay)

# Inizializzazione Gateway
gateway = SignalGateway(path_config)


# ==========================================
# ENDPOINT WEBSOCKET
# ==========================================
@gateway.app.websocket("/ws/{token}")
async def websocket_endpoint(websocket: WebSocket, token: str):
    if token != gateway.api_token:
        await websocket.close(code=1008)
        return

    await websocket.accept()
    gateway.active_connections.add(websocket)
    gateway.logger.info(f"🔌 Client connesso. Totale: {len(gateway.active_connections)}")

    try:
        # Invio snapshot iniziale
        for sig_type in gateway.managed_signals:
            snap_key = f"latest_{sig_type}"
            last_data = gateway.r.get(snap_key)
            if last_data:
                await websocket.send_text(last_data)

        while True:
            # Mantieni viva la connessione ascoltando eventuali ping/messaggi
            await websocket.receive_text()

    except WebSocketDisconnect:
        gateway.active_connections.discard(websocket)
        gateway.logger.info("🔌 Client disconnesso")
    except Exception as e:
        gateway.logger.error(f"❌ Errore WebSocket: {e}")
        gateway.active_connections.discard(websocket)


# ==========================================
# AVVIO
# ==========================================
if __name__ == "__main__":
    host_ip = gateway.config.get_string('GATEWAY', 'host', fallback='0.0.0.0')
    port_num = gateway.config.get_int('GATEWAY', 'port', fallback=40000)

    uvicorn.run(
        gateway.app,
        host=host_ip,
        port=port_num,
        log_config=None  # Usiamo il nostro logger
    )