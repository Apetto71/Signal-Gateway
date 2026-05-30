import asyncio
import logging
from typing import Set
from fastapi import WebSocket


class ClientConnectionManager:
    def __init__(self, logger: logging.Logger, keepalive_interval: float = 60.0):
        self.active_connections: Set[WebSocket] = set()
        self.logger = logger
        self.keepalive_interval = keepalive_interval
        self._keepalive_task = None

    def start(self):
        self._keepalive_task = asyncio.create_task(self._ping_loop())
        self.logger.info(f"⏱️ Keep-Alive Manager avviato (Ogni {self.keepalive_interval} secondi)")

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        self.logger.info(f"🔌 Client connesso. Connessioni attive: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            self.logger.info(f"❌ Client disconnesso. Connessioni attive: {len(self.active_connections)}")

    async def broadcast(self, message: str):
        if not self.active_connections:
            return
        tasks = [self._send_safe(client, message) for client in self.active_connections]
        await asyncio.gather(*tasks)

    async def _send_safe(self, client: WebSocket, message: str):
        try:
            await client.send_text(message)
        except Exception:
            self.disconnect(client)

    async def _ping_loop(self):
        try:
            while True:
                await asyncio.sleep(self.keepalive_interval)
                if self.active_connections:
                    self.logger.debug(f"💓 Invio Keep-Alive a {len(self.active_connections)} client.")
                    dead_clients = []
                    for client in self.active_connections:
                        try:
                            await client.send_json({"type": "ping"})
                        except Exception:
                            dead_clients.append(client)

                    for client in dead_clients:
                        self.disconnect(client)
        except asyncio.CancelledError:
            self.logger.info("🛑 Loop di Keep-Alive interrotto.")

    def stop(self):
        if self._keepalive_task:
            self._keepalive_task.cancel()