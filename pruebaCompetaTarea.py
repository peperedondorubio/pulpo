import asyncio
from pulpo.gestor_tareas.gestor_tareas import GestorTareas

async def main():
    # Pide el job_id por consola
    job_id = input("Introduce el job_id a completar: ").strip()

    # Instancia el gestor SIN callbacks (solo para completar)
    monitor = GestorTareas(id_grupo="grupo_kafka_completador")
    await monitor.start()

    # Recupera el job y sus tareas
    job = monitor.collection.get(job_id)
    if not job:
        print(f"No se encontró el job con id: {job_id}")
        await monitor.stop()
        return

    tasks = list(job["tasks"].keys())
    print(f"Tareas a completar: {tasks}")

    # Marca cada tarea como completada
    for task_id in tasks:
        await monitor.task_completed(job_id, task_id)
        print(f"Tarea completada: {task_id}")

    await monitor.stop()
    print("Todas las tareas completadas.")

if __name__ == "__main__":
    asyncio.run(main())