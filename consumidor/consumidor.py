# mi_paquete/consumer.py
from aiokafka import AIOKafkaConsumer
import asyncio

KAFKA_BROKER = 'escorial:29092'

class KafkaEventConsumer:
    def __init__(self, topic: str, callback: callable):
        """
        Constructor para el consumidor.
        :param topic: Tópico de Kafka que se desea consumir.
        :param callback: Función que se llamará cuando un mensaje sea recibido.
        """
        self.consumer = None
        self.topic = topic
        self.callback = callback  # Guardamos el callback
        self.consumer_task = None

    async def start(self):
        """Inicia el consumidor de Kafka."""
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=KAFKA_BROKER,
            group_id="grupo_consumidor",
            enable_auto_commit=False,
            session_timeout_ms=60000,  # 🔹 Aumenta el tiempo de espera (60 segundos)
            heartbeat_interval_ms=15000  # 🔹 Envía heartbeats cada 15 segundos
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
                await self.consumer_task
            except asyncio.CancelledError:
                print("La tarea de consumo fue cancelada correctamente.")
            self.consumer_task = None

        if self.consumer:
            await self.consumer.stop()

    async def consume(self):
        """Consume los mensajes de Kafka y ejecuta el callback."""
        async for message in self.consumer:
            print(f"Mensaje recibido en el tópico {self.topic}: {message.value.decode('utf-8')}")
            await self.consumer.commit()
            # Llamamos al callback con el mensaje recibido
            await self.callback(message)
