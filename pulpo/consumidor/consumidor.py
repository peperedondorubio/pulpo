# mi_paquete/consumer.py
from aiokafka import AIOKafkaConsumer
import asyncio
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "alcazar:29092")

class KafkaEventConsumer:
    def __init__(self, topic: str, callback: callable, id_grupo: str = "global"):
        """
        Constructor para el consumidor.
        :param topic: Tópico de Kafka que se desea consumir.
        :param callback: Función que se llamará cuando un mensaje sea recibido.
        """
        self.consumer = None
        self.topic = topic
        self.callback = callback  # Guardamos el callback
        self.consumer_task = None 
        self.id_grupo = id_grupo

    async def start(self, broker = KAFKA_BROKER):
        """Inicia el consumidor de Kafka."""
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=broker,
            group_id=self.id_grupo,
            enable_auto_commit=False,
            session_timeout_ms=60000,  # 🔹 Aumenta el tiempo de espera (60 segundos)
            heartbeat_interval_ms=15000,  # 🔹 Envía heartbeats cada 15 segundos
            isolation_level="read_committed"  # Ignora mensajes no confirmados
        )
        await self.consumer.start()

        # Crea la tarea del consumidor en paralelo
        if self.consumer_task is None:
            self.consumer_task = asyncio.create_task(self.consume())

    async def stop(self):
        """Detiene el consumidor de Kafka."""

        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task  # espera que termine
            except asyncio.CancelledError:
                print("La tarea de consumo fue cancelada correctamente.")
            self.consumer_task = None

        if self.consumer:
            await self.consumer.stop()

    async def consume(self):
        """Consume los mensajes de Kafka y ejecuta el callback."""
        async for message in self.consumer:
            print(f"Mensaje recibido en el tópico {self.topic}: {message.value.decode('utf-8')}")
            # Consumimos el mensaje
            await self.consumer.commit()
            # Llamamos al callback con el mensaje recibido
            await self.callback(message)
