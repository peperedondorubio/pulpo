# mi_paquete/main.py
import asyncio
import json
from publicador import publicador
from consumidor import consumidor

async def run():
    # Crear publicador y consumidor
    publisher = publicador.KafkaEventPublisher()

    consumer = consumidor.KafkaEventConsumer(
        topic='procesos_pendientes',
        callback=procesar_mensaje  # Pasa el callback importado
    )

    # Iniciar publicador y consumidor
    await publisher.start()
    await consumer.start()

    # Publicar mensaje en el tópico
    await publisher.publish("procesos_pendientes", {"parametro": "valor1"})

    # Esto es el programa
    await asyncio.sleep(10)  # Esperamos un poco para ver los resultados

    # y cuando termino el programa
    await publisher.stop()
    await consumer.stop()
 
# Callback
async def procesar_mensaje(message):
    """
    Esta es la función que se ejecutará cuando el consumidor reciba un mensaje.
    :param message: El mensaje recibido de Kafka.
    """
    # Deserializar el mensaje
    data = json.loads(message.value.decode('utf-8'))
    print(f"Procesando mensaje: {data}")
    # Aquí podrías hacer cualquier acción, como procesar los datos y realizar alguna lógic


if __name__ == "__main__":
    asyncio.run(run())


