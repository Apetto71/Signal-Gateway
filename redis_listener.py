import asyncio
import logging
import redis.asyncio as aioredis
import json

from connection_manager import ClientConnectionManager


class RedisPubSubListener:
    def __init__(self, redis_host: str, redis_port: int, channel: str, connection_manager: ClientConnectionManager,
                 logger: logging.Logger):
        self.redis_url = f"redis://{redis_host}:{redis_port}"
        self.channel = channel
        self.manager = connection_manager
        self.logger = logger
        self._pubsub_task = None

        # 🎯 CREIAMO IL CLIENT CENTRALIZZATO SUBITO NELL'INIT
        # decode_responses=True converte automaticamente i byte di Redis in stringhe Python standard
        self._redis_client = aioredis.from_url(self.redis_url, decode_responses=True)

    def start(self):
        self._pubsub_task = asyncio.create_task(self._listen_loop())
        self.logger.info(f"📡 Redis Listener agganciato sull'URL {self.redis_url} per il canale '{self.channel}'")

    async def _listen_loop(self):
        while True:
            try:
                # Usiamo il client centralizzato dell'istanza
                pubsub = self._redis_client.pubsub()
                await pubsub.subscribe(self.channel)

                self.logger.info(f"✅ Sottoscritto al Pub/Sub di Redis sul canale: {self.channel}")

                async for message in pubsub.listen():
                    if message["type"] == "message":
                        raw_data = message["data"]
                        self.logger.debug(f"📥 Ricevuto pacchetto da Redis: {raw_data}")
                        await self.manager.broadcast(raw_data)

            except (aioredis.RedisError, Exception) as e:
                self.logger.error(f"💥 Errore Redis Pub/Sub: {e}. Riconnessione tra 5 secondi...")
                await asyncio.sleep(5)

    # 🎯 METODO COMPATIBILE CON DECODE_RESPONSES=TRUE (SENZA CRASH DA DECODE)
    async def get_all_data_snapshot(self, redis_prefix: str) -> dict:
        """
        Scansiona ed estrae in modo strutturato tutte le chiavi
        presenti su Redis divise per tipologia (String, Hash, Sorted Set).
        """
        snapshot = {
            "type": "full_snapshot",
            "strings": {},
            "hashes": {},
            "sorted_sets": {}
        }

        pattern = f"{redis_prefix}:*" if redis_prefix else "*"

        try:
            cursor = 0
            all_keys = []
            while True:
                # scan restituisce già stringhe grazie a decode_responses=True
                cursor, keys = await self._redis_client.scan(cursor=cursor, match=pattern, count=100)
                all_keys.extend(keys)
                if cursor == 0:
                    break

            for key in all_keys:
                # key è già una stringa, non serve .decode()
                key_type = await self._redis_client.type(key)

                # 1. Gestione STRING
                if key_type == "string":
                    val = await self._redis_client.get(key)
                    try:
                        snapshot["strings"][key] = json.loads(val)
                    except Exception:
                        snapshot["strings"][key] = val

                # 2. Gestione HASH
                elif key_type == "hash":
                    raw_hash = await self._redis_client.hgetall(key)
                    snapshot["hashes"][key] = {}
                    for h_key, h_val in raw_hash.items():
                        try:
                            snapshot["hashes"][key][h_key] = json.loads(h_val)
                        except Exception:
                            snapshot["hashes"][key][h_key] = h_val

                # 3. Gestione SORTED SET (ZSET)
                elif key_type == "zset":
                    raw_zset = await self._redis_client.zrange(key, 0, -1, withscores=True)
                    snapshot["sorted_sets"][key] = []
                    for member, score in raw_zset:
                        try:
                            member_val = json.loads(member)
                        except Exception:
                            member_val = member
                        snapshot["sorted_sets"][key].append({
                            "value": member_val,
                            "score": score
                        })

            return snapshot

        except Exception as e:
            self.logger.error(f"❌ Errore durante lo scan delle chiavi Redis: {e}", exc_info=True)
            raise e

    def stop(self):
        if self._pubsub_task:
            self._pubsub_task.cancel()
        # Chiudiamo correttamente il client quando l'applicazione si spegne
        if self._redis_client:
            asyncio.create_task(self._redis_client.close())