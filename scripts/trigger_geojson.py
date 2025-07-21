from app.geojson_processor.tasks import run_geojson_processor_task

def main():
    run_geojson_processor_task.delay()

if __name__ == "__main__":
    main()
