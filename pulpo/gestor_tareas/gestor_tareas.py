import asyncio
import json
import uuid
from arango import ArangoClient
import os
import sys
from pathlib import Path


# Añadir el directorio raíz del proyecto al path de Python
project_root = Path(__file__).parent.parent  # Sube dos niveles desde taskmanager.py
sys.path.append(str(project_root))

from consumidor.consumidor import KafkaEventConsumer
from publicador.publicador import KafkaEventPublisher

ARANGO_HOST = os.getenv("ARANGO_HOST", "http://alcazar:8529")
ARANGO_DB = os.getenv("ARANGO_DB", "compai_db")
ARANGO_USER = os.getenv("ARANGO_USER", "root")
ARANGO_PASSWORD = os.getenv("ARANGO_PASSWORD", "sabbath")
ARANGO_COLLECTION = os.getenv("ARANGO_COLLECTION", "tareas")

# Tópicos
TOPIC_JOB = os.getenv("TOPIC_JOB", "job.start")
TOPIC_TASK = os.getenv("TOPIC_TASK", "job.task.start")
TOPIC_END_TASK = os.getenv("TOPIC_END_TASK", "job.task.end")
TOPIC_END_JOB = os.getenv("TOPIC_END_JOB", "job.end")


class GestorTareas:
    def __init__(
        self,
        topic_finalizacion_tareas: str = TOPIC_END_TASK,
        topic_finalizacion_global: str = TOPIC_END_JOB,
        on_complete_callback=None,
        on_all_complete_callback=None,
    ):
        self.topic_finalizacion_tareas = topic_finalizacion_tareas
        self.topic_finalizacion_global = topic_finalizacion_global

        self.client = ArangoClient(hosts=ARANGO_HOST)
        self.db = self.client.db(ARANGO_DB, username=ARANGO_USER, password=ARANGO_PASSWORD)
        self.collection = self.db.collection(ARANGO_COLLECTION)

        self.producer = KafkaEventPublisher()

        self.consumer = KafkaEventConsumer(
            topic=self.topic_finalizacion_tareas,
            callback=self._on_kafka_message,
            id_grupo="job_monitor_group"
        )

        self.on_complete_callback = on_complete_callback
        self.on_all_complete_callback = on_all_complete_callback

    async def start(self):
        await self.producer.start()
        await self.consumer.start()

    async def stop(self):
        await self.consumer.stop()
        await self.producer.stop()

    async def add_job(self, job_id: str = None, task_ids: list[str] = []):
        if job_id is None:
            job_id = str(uuid.uuid4())

        doc = {
            "_key": job_id,
            "tasks": {task_id: False for task_id in task_ids}
        }

        if not self.collection.has(job_id):
            self.collection.insert(doc)
        else:
            self.collection.update_match({"_key": job_id}, doc)

        print(f"Job '{job_id}' creado con tareas: {task_ids}")

        for task_id in task_ids:
            msg = {
                "job_id": job_id,
                "task_id": task_id,
                "action": "start_task"
            }
            asyncio.create_task(self._publicar_tarea(msg))

        return job_id

    async def _publicar_tarea(self, msg: dict):
        await self.producer.publish(TOPIC_TASK, msg)

    async def _on_kafka_message(self, message):
        try:
            data = json.loads(message.value.decode("utf-8"))
            job_id = data.get("job_id")
            task_id = data.get("task_id")
            if job_id and task_id:
                await self.task_completed(job_id, task_id)
        except Exception as e:
            print(f"Error procesando mensaje Kafka: {e}")

    async def task_completed(self, job_id: str, task_id: str):
        job = self.collection.get(job_id)
        if not job:
            print(f"[!] Job '{job_id}' no encontrado")
            return

        if task_id not in job["tasks"]:
            print(f"[!] Tarea '{task_id}' no pertenece al job '{job_id}'")
            return

        if job["tasks"][task_id]:
            print(f"[i] Tarea '{task_id}' ya estaba marcada como completada")
            return

        job["tasks"][task_id] = True
        self.collection.update(job)
        print(f"[✔] Tarea '{task_id}' completada en job '{job_id}'")

        if all(job["tasks"].values()):
            print(f"[🎉] Job '{job_id}' completado")
            if self.on_complete_callback:
                await self.on_complete_callback(job_id)

            msg = {
                "job_id": job_id,
                "status": "completed",
                "uuid": str(uuid.uuid4())
            }
            await self.producer.publish(self.topic_finalizacion_global, msg)

            if self._all_jobs_completed():
                print("[🎉] Todos los jobs completados")
                if self.on_all_complete_callback:
                    await self.on_all_complete_callback()

    def _all_jobs_completed(self) -> bool:
        cursor = self.collection.find({})
        for job in cursor:
            if not all(job["tasks"].values()):
                return False
        return True


##############################################################
# Ejemplo callbacks para pruebas

async def on_job_complete(job_id):
    print(f"Callback: Job {job_id} completado.")
    publisher = KafkaEventPublisher()
    await publisher.start()
    await publisher.publish("jobs.complete", {"job_id": job_id, "status": "completed"})
    await publisher.stop()

async def on_all_jobs_complete():
    print("Callback: Todos los jobs completados.")
    publisher = KafkaEventPublisher()
    await publisher.start()
    await publisher.publish("jobs.global", {"status": "all_jobs_completed"})
    await publisher.stop()


# Ejemplo de uso
async def main():
    monitor = GestorTareas(
        on_complete_callback=on_job_complete,
        on_all_complete_callback=on_all_jobs_complete,
    )

    await monitor.start()

    # ...existing code...
    # Crear el job y obtener el job_id generado
    task_ids = [f"task-{i}" for i in range(3)]
    job_id = await monitor.add_job(task_ids=task_ids)
    # ...existing code...

    # Simular finalización de tareas
    for task_id in task_ids:
        await monitor.task_completed(job_id, task_id)

    # Mantener corriendo para escuchar mensajes reales
    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        await monitor.stop()


if __name__ == "__main__":
    asyncio.run(main())
