import asyncio
import json
import os
import sys
from pathlib import Path

# Añadir el directorio raíz del proyecto al path de Python
project_root = Path(__file__).parent.parent  # Sube dos niveles desde taskmanager.py
sys.path.append(str(project_root))

from consumidor.consumidor import KafkaEventConsumer
from publicador.publicador import KafkaEventPublisher

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "alcazar:29092")
SUBJECT_TAREA = os.getenv("SUBJECT_TAREA", "compai_tarea")
SUBJECT_FIN_TAREA = os.getenv("SUBJECT_FIN_TAREA", "compai_fin_tarea")


class TaskWorker:
    def __init__(self, group_id="task_worker_group"):
        self.consumer = KafkaEventConsumer(
            topic=SUBJECT_TAREA,
            bootstrap_servers=KAFKA_BROKER,
            group_id=group_id,
        )
        self.producer = KafkaEventPublisher(bootstrap_servers=KAFKA_BROKER)
        self.running = False
        self.consumer_task = None

    async def start(self):
        await self.producer.start()
        await self.consumer.start()
        self.running = True
        self.consumer_task = asyncio.create_task(self._consume_loop())
        print("TaskWorker iniciado, escuchando tareas...")

    async def stop(self):
        self.running = False
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass
        await self.consumer.stop()
        await self.producer.stop()
        print("TaskWorker detenido.")

    async def _consume_loop(self):
        async for message in self.consumer:
            try:
                data = json.loads(message.value.decode("utf-8"))
                job_id = data.get("job_id")
                task_id = data.get("task_id")
                action = data.get("action")

                if action == "start_task" and job_id and task_id:
                    print(f"[+] Recibida tarea '{task_id}' del job '{job_id}'. Ejecutando...")
                    await self.do_task(job_id, task_id)
                else:
                    print(f"[!] Mensaje inválido o sin acción 'start_task': {data}")

            except Exception as e:
                print(f"Error procesando mensaje: {e}")

    async def do_task(self, job_id: str, task_id: str):
        # Simula hacer el trabajo con una espera asíncrona
        await asyncio.sleep(2)  # Aquí pones la lógica real de la tarea
        print(f"[✔] Tarea '{task_id}' del job '{job_id}' completada.")

        # Publicar mensaje de tarea terminada
        msg = {
            "job_id": job_id,
            "task_id": task_id,
            "status": "completed"
        }
        await self.producer.send_and_wait(SUBJECT_FIN_TAREA, json.dumps(msg).encode("utf-8"))
        print(f"Mensaje de fin de tarea publicado para '{task_id}' del job '{job_id}'.")


# Ejemplo de uso básico para ejecutar el worker
async def main():
    worker = TaskWorker()
    await worker.start()

    try:
        while True:
            await asyncio.sleep(3600)  # Mantener corriendo
    except KeyboardInterrupt:
        print("Deteniendo worker...")
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
