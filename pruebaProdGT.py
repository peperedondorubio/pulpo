import asyncio
from pulpo.gestor_tareas.gestor_tareas import GestorTareas

async def main():
    # Instancia el gestor SIN callbacks (solo publica)
    monitor = GestorTareas(id_grupo="grupo_kafka_productor_tareas")

    await monitor.start()
    print("Productor de tareas iniciado.")

    # Crea un job con varias tareas de ejemplo
    tasks = [
        {"task_id": "tarea_1", "descripcion": "Primera tarea"},
        {"task_id": "tarea_2", "descripcion": "Segunda tarea"},
        {"task_id": "tarea_3", "descripcion": "Tercera tarea"},
    ]
    job_id = await monitor.add_job(tasks)
    print(f"Job publicado con id: {job_id}")

    await monitor.stop()
    print("Productor de tareas finalizado.")

if __name__ == "__main__":
    asyncio.run(main())