import asyncio
from pulpo.gestor_tareas.gestor_tareas import GestorTareas

# Callbacks solo para mostrar que se reciben eventos
async def on_task_complete(job_id, task_id):
    print(f"[CONSUMIDOR] Callback: Tarea {task_id} del job {job_id} completada.")

async def on_job_complete(job_id):
    print(f"[CONSUMIDOR] Callback: Job {job_id} completado.")

async def on_all_jobs_complete():
    print("[CONSUMIDOR] Callback: Todos los jobs completados.")

async def main():
    # Solo consume: no crea ni publica jobs/tareas
    monitor = GestorTareas(
        on_complete_callback=on_job_complete,
        on_all_complete_callback=on_all_jobs_complete,
        on_task_complete_callback=on_task_complete,
        id_grupo="grupo_kafka_test_consume"  # Usa un group_id específico para pruebas
    )

    await monitor.start()
    print("Consumidor de tareas iniciado. Esperando eventos... (Ctrl+C para salir)")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Cerrando consumidor...")
        await monitor.stop()

if __name__ == "__main__":
    asyncio.run(main())