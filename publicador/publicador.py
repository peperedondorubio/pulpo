from aiokafka import AIOKafkaProducer
from confluent_kafka.admin import AdminClient, NewTopic
import json
import asyncio

KAFKA_BROKER = 'escorial:29092'

async def crear_topico(kafka_broker: str, topic_name: str, num_particiones: int = 1, replication_factor: int = 1) -> bool:
    """
    Verifica si un tópico existe en Kafka. Si no existe, intenta crearlo.
    
    :param kafka_broker: Dirección del broker de Kafka (ej. 'localhost:9092').
    :param topic_name: Nombre del tópico a verificar o crear.
    :param num_particiones: Número de particiones para el tópico (por defecto 1).
    :param replication_factor: Factor de replicación para el tópico (por defecto 1).
    :return: True si el tópico ya existía o se creó correctamente, False en caso de error.
    """
    try:
        # Crear cliente de administración
        admin_client = AdminClient({"bootstrap.servers": kafka_broker})

        # Verificar si el tópico ya existe
        cluster_metadata = admin_client.list_topics(timeout=5)
        if topic_name in cluster_metadata.topics:
            print(f"El tópico '{topic_name}' ya existe.")
            return False  # Ya existía

        # Crear el tópico
        new_topic = NewTopic(topic_name, num_particiones, replication_factor)
        fs = admin_client.create_topics([new_topic])

        # Esperar la respuesta de Kafka
        result = await asyncio.to_thread(fs[topic_name].result)

        print(f"Tópico '{topic_name}' creado correctamente.")
        return True  # Se creó correctamente
    except Exception as e:
        print(f"Error al crear el tópico '{topic_name}': {e}")
        return False  # Hubo un error

class KafkaEventPublisher:
    def __init__(self):
        self.producer = None

    async def start(self):
        """Inicia el productor de Kafka."""
        self.producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
        await self.producer.start()

    async def stop(self):
        """Detiene el productor de Kafka."""
        if self.producer:
            await self.producer.stop()

    async def publish(self, topic: str, message: dict):
        """Publica un mensaje en un tópico específico."""
        if not self.producer:
            raise Exception("Producer not started.")
        
         # Verificar y crear el tópico si es necesario
        await crear_topico(KAFKA_BROKER, topic)

        message_str = json.dumps(message)
        await self.producer.send_and_wait(topic, message_str.encode('utf-8'))
        print(f"Mensaje publicado en el tópico '{topic}': {message}")

